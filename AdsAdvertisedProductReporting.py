import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from AdsAuth import AD_PROFILES, ADS_REGION_ENDPOINTS, cors_headers, json_response
from AdsReporting import (
    ADMIN_KEY,
    POCKETBASE_BATCH_SIZE,
    POCKETBASE_URL,
    check_report_status,
    download_report_rows,
    last_recorded_date,
    pb_authenticate,
    pb_batch,
    pb_list_connected,
    refresh_access_token,
    request_campaign_report,
)

SYSTEM_TZ = ZoneInfo("Asia/Jerusalem")  # matches this machine's local cron timezone, not Amazon's
POCKETBASE_ADS_ADVERTISED_PRODUCT_COLLECTION = os.environ.get(
    "POCKETBASE_ADS_ADVERTISED_PRODUCT_COLLECTION", "ads_advertised_product_stats"
)

REPORT_POLL_ROUNDS = 165
REPORT_POLL_DELAY_SECONDS = 10

# Confirmed live against the real account (probed with a deliberately bad
# column name, same technique used elsewhere in this project): SP has
# "spAdvertisedProduct" (groupBy advertiser); SD has "sdAdvertisedProduct"
# too, with different column names (promotedAsin/promotedSku, sales/purchases
# with no "7d" suffix, matching SD's existing pattern elsewhere). SB has NO
# advertised-product report type at all - confirmed by testing (400 "unknown
# or invalid reportTypeId") - Sponsored Brands promotes a brand/collection,
# not individual ASINs the same way SP/SD do, so it's excluded here, not an
# oversight.
AD_ADVERTISED_PRODUCT_PRODUCTS = [
    {
        "key": "SP",
        "ad_product": "SPONSORED_PRODUCTS",
        "report_type_id": "spAdvertisedProduct",
        "columns": [
            "date", "campaignId", "campaignName", "adGroupId", "adGroupName",
            "advertisedAsin", "advertisedSku",
            "impressions", "clicks", "cost", "purchases7d", "sales7d",
        ],
        "asin_field": "advertisedAsin",
        "sku_field": "advertisedSku",
        "sales_field": "sales7d",
        "purchases_field": "purchases7d",
    },
    {
        "key": "SD",
        "ad_product": "SPONSORED_DISPLAY",
        "report_type_id": "sdAdvertisedProduct",
        "columns": [
            "date", "campaignId", "campaignName",
            "promotedAsin", "promotedSku",
            "impressions", "clicks", "cost", "purchases", "sales",
        ],
        "asin_field": "promotedAsin",
        "sku_field": "promotedSku",
        "sales_field": "sales",
        "purchases_field": "purchases",
    },
]


def advertised_product_row_to_body(ads_profile, row, product):
    date_str = row.get("date", "")
    year, month = 0, 0
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        year, month = parsed.year, parsed.month
    except ValueError:
        pass

    return {
        "profile_id": str(ads_profile.get("profileId")),
        "campaign_id": str(row.get("campaignId")),
        "campaign_name": row.get("campaignName", ""),
        "ad_group_id": str(row.get("adGroupId", "")),
        "ad_group_name": row.get("adGroupName", ""),
        "asin": row.get(product["asin_field"], "") or "",
        "sku": row.get(product["sku_field"], "") or "",
        "country_code": ads_profile.get("countryCode", ""),
        "currency_code": ads_profile.get("currencyCode", ""),
        "ad_product": product["ad_product"],
        "date": date_str,
        "month": month,
        "year": year,
        "impressions": row.get("impressions", 0),
        "clicks": row.get("clicks", 0),
        "spend": row.get("cost", 0),
        "sales": row.get(product["sales_field"], 0),
        "orders": row.get(product["purchases_field"], 0),
    }


def submit_advertised_product_report_jobs(connections, start_date, end_date, errors):
    jobs = []
    for connection in connections:
        profile_key = connection.get("region")
        refresh_token = connection.get("refresh_token")
        if profile_key not in AD_PROFILES or not refresh_token:
            continue

        try:
            access_token = refresh_access_token(profile_key, refresh_token)
        except Exception as exc:
            errors.append(f"{profile_key}: token refresh failed: {exc}")
            continue

        client_id = AD_PROFILES[profile_key]["client_id"]
        for ads_profile in connection.get("profiles", []) or []:
            if ads_profile.get("accountType") == "agency":
                continue

            region = ads_profile.get("region")
            base_url = ADS_REGION_ENDPOINTS.get(region)
            ads_profile_id = ads_profile.get("profileId")
            if not base_url or not ads_profile_id:
                continue

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Amazon-Advertising-API-ClientId": client_id,
                "Amazon-Advertising-API-Scope": str(ads_profile_id),
            }
            for product in AD_ADVERTISED_PRODUCT_PRODUCTS:
                try:
                    report_id = request_campaign_report(
                        base_url, access_token, client_id, ads_profile_id, start_date, end_date, product,
                        group_by=("advertiser",),
                    )
                    jobs.append({
                        "profile_key": profile_key,
                        "ads_profile": ads_profile,
                        "base_url": base_url,
                        "headers": headers,
                        "report_id": report_id,
                        "product": product,
                    })
                except Exception as exc:
                    errors.append(f"{profile_key}/{ads_profile_id} ({ads_profile.get('countryCode')}) {product['key']}: {exc}")
                time.sleep(2)

    return jobs


def poll_and_store_advertised_product_jobs(pb_token, jobs, errors):
    pending = list(jobs)
    total_written = 0

    for _ in range(REPORT_POLL_ROUNDS):
        if not pending:
            break
        still_pending = []
        for job in pending:
            try:
                state, download_url = check_report_status(job)
            except Exception as exc:
                errors.append(f"{job['profile_key']}/{job['ads_profile'].get('profileId')} {job['product']['key']}: {exc}")
                continue
            if state == "pending":
                still_pending.append(job)
                continue
            try:
                rows = download_report_rows(download_url)
                bodies = [advertised_product_row_to_body(job["ads_profile"], row, job["product"]) for row in rows]
                ops = [
                    {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_ADVERTISED_PRODUCT_COLLECTION}/records", "body": b}
                    for b in bodies
                ]
                for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
                    pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])
                total_written += len(bodies)
            except Exception as exc:
                errors.append(f"{job['profile_key']}/{job['ads_profile'].get('profileId')} {job['product']['key']}: download/write failed: {exc}")
        pending = still_pending
        if pending:
            time.sleep(REPORT_POLL_DELAY_SECONDS)

    for job in pending:
        errors.append(
            f"{job['profile_key']}/{job['ads_profile'].get('profileId')} "
            f"({job['ads_profile'].get('countryCode')}) {job['product']['key']}: "
            f"report did not complete after {REPORT_POLL_ROUNDS} polls"
        )

    return total_written


def pb_list_advertised_product_stats_ids(token, profile_id, start_date, end_date):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_ADVERTISED_PRODUCT_COLLECTION}/records",
            headers={"Authorization": token},
            params={
                "filter": f'(profile_id = "{profile_id}" && date >= "{start_date}" && date <= "{end_date}")',
                "fields": "id",
                "perPage": 200,
                "page": page,
            },
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        ids.extend(item["id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


POCKETBASE_ADS_PRODUCT_ADS_COLLECTION = os.environ.get("POCKETBASE_ADS_PRODUCT_ADS_COLLECTION", "ads_product_ads")


def fetch_sp_product_ads(base_url, access_token, client_id, ads_profile_id):
    """Lists every SP product ad for one profile (no filter - the full
    catalog, refreshed alongside the daily advertised-product stats pull) -
    added 2026-09-01 after a real per-ad status mismatch was found: a
    campaign can stay ENABLED overall while one specific product ad inside
    it is individually paused, which campaign-level status alone can't show
    (confirmed live: Amazon's own console showed PAREO_ACA_1A as "Paused"
    while its campaign, PAREO SP BROAD, was still enabled). Pagination
    field names (nextToken/maxResults) follow the same v3 convention as
    every other SP list endpoint in this project - not directly confirmed
    for this specific endpoint from Amazon's official Postman collection,
    whose saved example has only one result and doesn't exercise paging."""
    ads = []
    next_token = None
    while True:
        body = {"maxResults": 100}
        if next_token:
            body["nextToken"] = next_token
        response = requests.post(
            f"{base_url}/sp/productAds/list",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Amazon-Advertising-API-ClientId": client_id,
                "Amazon-Advertising-API-Scope": str(ads_profile_id),
                "Content-Type": "application/vnd.spProductAd.v3+json",
                "Accept": "application/vnd.spProductAd.v3+json",
            },
            json=body,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        ads.extend(payload.get("productAds", []))
        next_token = payload.get("nextToken")
        if not next_token:
            break
    return ads


def sync_product_ads_catalog(pb_token, connections, errors):
    """Delete-and-recreate snapshot of every SP product ad's real state,
    same pattern as ads_campaigns - refreshed every time
    UpdateAdsAdvertisedProductStats runs (now daily) so the Advertised
    Products page's status column and pause-checkbox lockout reflect the
    real per-ad state, not just the parent campaign's."""
    for connection in connections:
        profile_key = connection.get("region")
        refresh_token = connection.get("refresh_token")
        if profile_key not in AD_PROFILES or not refresh_token:
            continue
        try:
            access_token = refresh_access_token(profile_key, refresh_token)
        except Exception as exc:
            errors.append(f"{profile_key}: token refresh failed (product ads catalog): {exc}")
            continue
        client_id = AD_PROFILES[profile_key]["client_id"]

        for ads_profile in connection.get("profiles", []) or []:
            if ads_profile.get("accountType") == "agency":
                continue
            region = ads_profile.get("region")
            base_url = ADS_REGION_ENDPOINTS.get(region)
            ads_profile_id = ads_profile.get("profileId")
            if not base_url or not ads_profile_id:
                continue

            try:
                ads = fetch_sp_product_ads(base_url, access_token, client_id, str(ads_profile_id))
            except Exception as exc:
                errors.append(f"{profile_key}/{ads_profile_id} ({ads_profile.get('countryCode')}) product ads list: {exc}")
                continue

            rows = [
                {
                    "profile_id": str(ads_profile_id),
                    "campaign_id": str(a.get("campaignId", "")),
                    "ad_group_id": str(a.get("adGroupId", "")),
                    "ad_id": str(a.get("adId", "")),
                    "asin": a.get("asin", ""),
                    "sku": a.get("sku", ""),
                    "state": a.get("state", ""),
                    "country_code": ads_profile.get("countryCode", ""),
                }
                for a in ads
            ]

            try:
                existing_ids = []
                page = 1
                while True:
                    resp = requests.get(
                        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_PRODUCT_ADS_COLLECTION}/records",
                        headers={"Authorization": pb_token},
                        params={"perPage": 500, "page": page, "filter": f'profile_id = "{ads_profile_id}"', "fields": "id"},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    existing_ids.extend(it["id"] for it in data.get("items", []))
                    if page >= data.get("totalPages", 1):
                        break
                    page += 1

                ops = [
                    {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ADS_PRODUCT_ADS_COLLECTION}/records/{rid}"}
                    for rid in existing_ids
                ]
                ops.extend(
                    {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_PRODUCT_ADS_COLLECTION}/records", "body": row}
                    for row in rows
                )
                for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
                    pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])
            except Exception as exc:
                errors.append(f"{profile_key}/{ads_profile_id} ({ads_profile.get('countryCode')}): product ads catalog write failed: {exc}")
            time.sleep(1)


def pull_and_store_advertised_product_stats(start_date, end_date):
    pb_token = pb_authenticate()
    connections = pb_list_connected(pb_token)
    errors = []

    profile_ids = set()
    for connection in connections:
        for ads_profile in connection.get("profiles", []) or []:
            if ads_profile.get("accountType") != "agency" and ads_profile.get("profileId"):
                profile_ids.add(str(ads_profile.get("profileId")))

    for profile_id in profile_ids:
        existing_ids = pb_list_advertised_product_stats_ids(pb_token, profile_id, start_date, end_date)
        ops = [
            {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ADS_ADVERTISED_PRODUCT_COLLECTION}/records/{rid}"}
            for rid in existing_ids
        ]
        for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
            pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])

    jobs = submit_advertised_product_report_jobs(connections, start_date, end_date, errors)
    rows_written = poll_and_store_advertised_product_jobs(pb_token, jobs, errors)

    sync_product_ads_catalog(pb_token, connections, errors)

    return {"rowsWritten": rows_written, "profilesPulled": len(profile_ids), "errors": errors}


def default_previous_month_range():
    now_local = datetime.now(SYSTEM_TZ)
    first_of_this_month = now_local.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start.strftime("%Y-%m-%d"), last_month_end.strftime("%Y-%m-%d")


def UpdateAdsAdvertisedProductStats(request):
    """Monthly job: pulls the previous calendar month's Advertised Product
    report (per-ASIN ad performance, SP + SD) - runs the 1st of each month,
    per the user's request 2026-08-29. Same catch-up-safe default as the
    other monthly/weekly Ads pullers: defaults to since-the-last-recorded-day
    (capped at Amazon's 31-day max range) rather than a fixed "previous
    month" window, so a missed run gets backfilled by the next one instead
    of silently skipping the gap."""
    if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
        return json_response({"error": "Unauthorized"}, 401)

    now_local = datetime.now(SYSTEM_TZ)
    yesterday = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    if request.args.get("start_date"):
        start_date = request.args["start_date"]
        end_date = request.args.get("end_date", yesterday)
    else:
        start_date, end_date = default_previous_month_range()
        try:
            token = pb_authenticate()
            last_date = last_recorded_date(token, "ads_advertised_product_stats")
            if last_date:
                gap_start = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                floor_date = (now_local - timedelta(days=31)).strftime("%Y-%m-%d")
                start_date = max(gap_start, floor_date)
                end_date = yesterday
        except Exception:
            pass

    if start_date > end_date:
        return json_response({"startDate": start_date, "endDate": end_date, "skipped": "already up to date"})

    try:
        result = pull_and_store_advertised_product_stats(start_date, end_date)
        return json_response({"startDate": start_date, "endDate": end_date, **result})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetAdsAdvertisedProductStats(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    start_date = request.args.get("start_date") if hasattr(request, "args") else None
    end_date = request.args.get("end_date") if hasattr(request, "args") else None
    country_code = request.args.get("country_code") if hasattr(request, "args") else None
    campaign_id = request.args.get("campaign_id") if hasattr(request, "args") else None

    if not start_date or not end_date:
        start_date, end_date = default_previous_month_range()

    try:
        token = pb_authenticate()
        filter_str = f'(date >= "{start_date}" && date <= "{end_date}")'
        if country_code:
            filter_str += f' && country_code = "{country_code}"'
        if campaign_id:
            filter_str += f' && campaign_id = "{campaign_id}"'

        products = {}
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_ADVERTISED_PRODUCT_COLLECTION}/records",
                headers={"Authorization": token},
                params={
                    "filter": filter_str,
                    "perPage": 500,
                    "page": page,
                    "fields": "profile_id,campaign_id,campaign_name,ad_group_id,ad_group_name,asin,sku,"
                              "ad_product,country_code,currency_code,impressions,clicks,spend,sales,orders,date",
                },
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                key = (item.get("profile_id"), item.get("campaign_id"), item.get("asin"))
                bucket = products.setdefault(key, {
                    "profileId": item.get("profile_id"),
                    "campaignId": item.get("campaign_id"),
                    "campaignName": item.get("campaign_name", ""),
                    "adGroupId": item.get("ad_group_id", ""),
                    "adGroupName": item.get("ad_group_name", ""),
                    "asin": item.get("asin", ""),
                    "sku": item.get("sku", ""),
                    "adProduct": item.get("ad_product", ""),
                    "countryCode": item.get("country_code", ""),
                    "currencyCode": item.get("currency_code", ""),
                    "impressions": 0, "clicks": 0, "spend": 0, "sales": 0, "orders": 0,
                    "_nameDate": "",
                })
                bucket["impressions"] += item.get("impressions", 0)
                bucket["clicks"] += item.get("clicks", 0)
                bucket["spend"] += item.get("spend", 0)
                bucket["sales"] += item.get("sales", 0)
                bucket["orders"] += item.get("orders", 0)
                # campaignName can change mid-window if the campaign gets
                # renamed - show the name from the most recent day in range.
                if item.get("campaign_name") and item.get("date", "") >= bucket["_nameDate"]:
                    bucket["campaignName"] = item.get("campaign_name")
                    bucket["_nameDate"] = item.get("date", "")
            if page >= data.get("totalPages", 1):
                break
            page += 1

        # Real per-ad status (not just campaign status) - added 2026-09-01
        # after finding a live mismatch: a campaign can stay ENABLED while
        # one specific product ad inside it is individually paused
        # (confirmed against Amazon's own console). ads_product_ads is the
        # real per-ad snapshot synced daily by UpdateAdsAdvertisedProductStats;
        # campaign_status is kept as a fallback for a product ad this
        # snapshot hasn't captured yet (e.g. right after a new campaign
        # launches, before the next daily sync).
        campaign_status = {}
        cs_page = 1
        while True:
            cs_resp = requests.get(
                f"{POCKETBASE_URL}/api/collections/ads_campaigns/records",
                headers={"Authorization": token},
                params={"perPage": 500, "page": cs_page, "fields": "campaign_id,campaign_status"},
                timeout=30,
            )
            cs_resp.raise_for_status()
            cs_data = cs_resp.json()
            for row in cs_data.get("items", []):
                if row.get("campaign_id"):
                    campaign_status[row["campaign_id"]] = row.get("campaign_status", "")
            if cs_page >= cs_data.get("totalPages", 1):
                break
            cs_page += 1

        ad_status = {}
        pa_page = 1
        while True:
            pa_resp = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_PRODUCT_ADS_COLLECTION}/records",
                headers={"Authorization": token},
                params={"perPage": 500, "page": pa_page, "fields": "campaign_id,ad_group_id,asin,state"},
                timeout=30,
            )
            pa_resp.raise_for_status()
            pa_data = pa_resp.json()
            for row in pa_data.get("items", []):
                key = (row.get("campaign_id"), row.get("ad_group_id"), row.get("asin"))
                ad_status[key] = row.get("state", "")
            if pa_page >= pa_data.get("totalPages", 1):
                break
            pa_page += 1

        rows = sorted(products.values(), key=lambda p: -p["spend"])
        for row in rows:
            row["acos"] = (row["spend"] / row["sales"] * 100) if row["sales"] else 0
            row.pop("_nameDate", None)
            ad_key = (row["campaignId"], row["adGroupId"], row["asin"])
            row["adStatus"] = ad_status.get(ad_key) or campaign_status.get(row["campaignId"], "")

        return json_response({"startDate": start_date, "endDate": end_date, "products": rows})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
