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

LA_TZ = ZoneInfo("America/Los_Angeles")
POCKETBASE_ADS_KEYWORD_COLLECTION = os.environ.get("POCKETBASE_ADS_KEYWORD_COLLECTION", "ads_keyword_stats")

REPORT_POLL_ROUNDS = 165
REPORT_POLL_DELAY_SECONDS = 10

# Column names confirmed live against the real account: SP's spTargeting
# schema differs from SB/SD's (SP uses "keyword"/"targeting", SB/SD reject
# those and require "keywordText"/"targetingExpression"/"targetingText" -
# discovered from the HTTP 400 "Allowed values" list Amazon returns for a
# bad column, same as the campaign-report schemas). SD has no keyword
# columns at all - it only targets products/audiences, not keywords.
# "keywordBid" (added 2026-08-26, confirmed via the same allowed-values
# probe) is valid for SP/SB but not present at all in sdTargeting's allowed
# column list - SD uses algorithmic/different bidding with no per-target
# bid value to report, so SD rows simply have no bid.
AD_KEYWORD_PRODUCTS = [
    {
        "key": "SP",
        "ad_product": "SPONSORED_PRODUCTS",
        "report_type_id": "spTargeting",
        "columns": [
            "date", "campaignId", "campaignName", "campaignStatus", "adGroupId", "adGroupName",
            "keywordId", "keyword", "keywordType", "matchType", "targeting", "keywordBid",
            "impressions", "clicks", "cost", "purchases7d", "sales7d",
        ],
        "sales_field": "sales7d",
        "purchases_field": "purchases7d",
    },
    {
        "key": "SB",
        "ad_product": "SPONSORED_BRANDS",
        "report_type_id": "sbTargeting",
        "columns": [
            "date", "campaignId", "campaignName", "campaignStatus", "adGroupId", "adGroupName",
            "keywordId", "keywordText", "keywordType", "matchType", "keywordBid",
            "targetingId", "targetingExpression", "targetingText", "targetingType",
            "impressions", "clicks", "cost", "purchases", "sales",
        ],
        "sales_field": "sales",
        "purchases_field": "purchases",
    },
    {
        "key": "SD",
        "ad_product": "SPONSORED_DISPLAY",
        "report_type_id": "sdTargeting",
        "columns": [
            "date", "campaignId", "campaignName", "adGroupId", "adGroupName",
            "targetingId", "targetingExpression", "targetingText",
            "impressions", "clicks", "cost", "purchases", "sales",
        ],
        "sales_field": "sales",
        "purchases_field": "purchases",
    },
]


def keyword_row_to_body(ads_profile, row, product):
    date_str = row.get("date", "")
    year, month = 0, 0
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        year, month = parsed.year, parsed.month
    except ValueError:
        pass

    # SP rows carry keyword*/targeting fields under its own names; SB rows
    # can be either a keyword-targeted or product-targeted ad group (only
    # one set of fields populated per row); SD only ever has targeting*.
    # Unified into one target_id/target_text/target_type regardless of which
    # product or targeting style produced the row.
    target_id = row.get("keywordId") or row.get("targetingId")
    target_text = row.get("keyword") or row.get("keywordText") or row.get("targetingText") or row.get("targetingExpression")
    target_type = row.get("keywordType") or row.get("targetingType")

    return {
        "profile_id": str(ads_profile.get("profileId")),
        "campaign_id": str(row.get("campaignId")),
        "campaign_name": row.get("campaignName", ""),
        "campaign_status": row.get("campaignStatus", ""),
        "ad_group_id": str(row.get("adGroupId", "")),
        "ad_group_name": row.get("adGroupName", ""),
        "target_id": str(target_id) if target_id is not None else "",
        "target_text": target_text or "",
        "target_type": target_type or "",
        "match_type": row.get("matchType", ""),
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
        "bid": row.get("keywordBid"),
    }


def submit_keyword_report_jobs(connections, start_date, end_date, errors):
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
            for product in AD_KEYWORD_PRODUCTS:
                try:
                    report_id = request_campaign_report(
                        base_url, access_token, client_id, ads_profile_id, start_date, end_date, product,
                        group_by=("targeting",),
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


def poll_and_store_keyword_jobs(pb_token, jobs, errors):
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
                bodies = [keyword_row_to_body(job["ads_profile"], row, job["product"]) for row in rows]
                ops = [
                    {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_KEYWORD_COLLECTION}/records", "body": b}
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


def pb_list_keyword_stats_ids(token, profile_id, start_date, end_date):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_KEYWORD_COLLECTION}/records",
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


def pull_and_store_keyword_stats(start_date, end_date):
    pb_token = pb_authenticate()
    connections = pb_list_connected(pb_token)
    errors = []

    profile_ids = set()
    for connection in connections:
        for ads_profile in connection.get("profiles", []) or []:
            if ads_profile.get("accountType") != "agency" and ads_profile.get("profileId"):
                profile_ids.add(str(ads_profile.get("profileId")))

    # Clear this date range's existing rows up front, per profile, before any
    # report completes - matches the campaign-stats pipeline's incremental
    # write pattern (see AdsReporting.py) so a platform-level timeout can't
    # lose an entire run's downloaded rows.
    for profile_id in profile_ids:
        existing_ids = pb_list_keyword_stats_ids(pb_token, profile_id, start_date, end_date)
        ops = [
            {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ADS_KEYWORD_COLLECTION}/records/{rid}"}
            for rid in existing_ids
        ]
        for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
            pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])

    jobs = submit_keyword_report_jobs(connections, start_date, end_date, errors)
    rows_written = poll_and_store_keyword_jobs(pb_token, jobs, errors)

    return {"rowsWritten": rows_written, "profilesPulled": len(profile_ids), "errors": errors}


def UpdateAdsKeywordStats(request):
    if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
        return json_response({"error": "Unauthorized"}, 401)

    now_la = datetime.now(LA_TZ)
    yesterday = (now_la - timedelta(days=1)).strftime("%Y-%m-%d")

    if request.args.get("start_date"):
        start_date = request.args["start_date"]
        end_date = request.args.get("end_date", yesterday)
    else:
        # Default to since-the-last-recorded-day (capped at Amazon's 31-day
        # max range) rather than a fixed trailing 7 days, so a missed
        # weekly run gets backfilled automatically by the next one instead
        # of silently losing whatever fell outside the fixed window.
        start_date = (now_la - timedelta(days=7)).strftime("%Y-%m-%d")
        try:
            token = pb_authenticate()
            last_date = last_recorded_date(token, "ads_keyword_stats")
            if last_date:
                gap_start = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                floor_date = (now_la - timedelta(days=31)).strftime("%Y-%m-%d")
                start_date = max(gap_start, floor_date)
        except Exception:
            pass
        end_date = yesterday

    if start_date > end_date:
        return json_response({"startDate": start_date, "endDate": end_date, "skipped": "already up to date"})

    try:
        result = pull_and_store_keyword_stats(start_date, end_date)
        return json_response({"startDate": start_date, "endDate": end_date, **result})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetAdsKeywordStats(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    start_date = request.args.get("start_date") if hasattr(request, "args") else None
    end_date = request.args.get("end_date") if hasattr(request, "args") else None
    country_code = request.args.get("country_code") if hasattr(request, "args") else None
    campaign_id = request.args.get("campaign_id") if hasattr(request, "args") else None

    if not start_date or not end_date:
        # Back-compat: no explicit range means "this month".
        month = request.args.get("month", type=int) if hasattr(request, "args") else None
        year = request.args.get("year", type=int) if hasattr(request, "args") else None
        now_la = datetime.now(LA_TZ)
        month, year = month or now_la.month, year or now_la.year
        start_date = f"{year:04d}-{month:02d}-01"
        next_month_first = datetime(year + (month == 12), (month % 12) + 1, 1)
        end_date = (next_month_first - timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        token = pb_authenticate()
        filter_str = f'(date >= "{start_date}" && date <= "{end_date}")'
        if country_code:
            filter_str += f' && country_code = "{country_code}"'
        if campaign_id:
            filter_str += f' && campaign_id = "{campaign_id}"'

        keywords = {}
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_KEYWORD_COLLECTION}/records",
                headers={"Authorization": token},
                params={
                    "filter": filter_str,
                    "perPage": 500,
                    "page": page,
                    "fields": "profile_id,campaign_id,campaign_name,ad_group_id,ad_group_name,target_id,"
                              "target_text,target_type,match_type,ad_product,country_code,currency_code,"
                              "impressions,clicks,spend,sales,orders,bid,date",
                },
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                key = (item.get("profile_id"), item.get("campaign_id"), item.get("ad_group_id"), item.get("target_id"))
                bucket = keywords.setdefault(key, {
                    "campaignId": item.get("campaign_id"),
                    "campaignName": item.get("campaign_name", ""),
                    "adGroupId": item.get("ad_group_id", ""),
                    "adGroupName": item.get("ad_group_name", ""),
                    "targetId": item.get("target_id"),
                    "targetText": item.get("target_text", ""),
                    "targetType": item.get("target_type", ""),
                    "matchType": item.get("match_type", ""),
                    "adProduct": item.get("ad_product", ""),
                    "countryCode": item.get("country_code", ""),
                    "currencyCode": item.get("currency_code", ""),
                    "impressions": 0, "clicks": 0, "spend": 0, "sales": 0, "orders": 0,
                    "bid": None, "_bidDate": "", "_nameDate": "",
                })
                bucket["impressions"] += item.get("impressions", 0)
                bucket["clicks"] += item.get("clicks", 0)
                bucket["spend"] += item.get("spend", 0)
                bucket["sales"] += item.get("sales", 0)
                bucket["orders"] += item.get("orders", 0)
                # bid is a current setting, not a metric to sum - keep the
                # value from whichever row in range is most recent.
                if item.get("bid") is not None and item.get("date", "") >= bucket["_bidDate"]:
                    bucket["bid"] = item.get("bid")
                    bucket["_bidDate"] = item.get("date", "")
                # campaignName can change mid-window if the campaign gets
                # renamed - show the name from the most recent day in range,
                # not whichever row happened to arrive first (campaign_id,
                # not name, is always the real grouping key here).
                if item.get("campaign_name") and item.get("date", "") >= bucket["_nameDate"]:
                    bucket["campaignName"] = item.get("campaign_name")
                    bucket["_nameDate"] = item.get("date", "")
            if page >= data.get("totalPages", 1):
                break
            page += 1

        rows = sorted(keywords.values(), key=lambda k: -k["spend"])
        for row in rows:
            row["acos"] = (row["spend"] / row["sales"] * 100) if row["sales"] else 0
            row.pop("_bidDate", None)
            row.pop("_nameDate", None)

        return json_response({"startDate": start_date, "endDate": end_date, "keywords": rows})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
