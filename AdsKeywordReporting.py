import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from AdsAuth import AD_PROFILES, ADS_REGION_ENDPOINTS, cors_headers, json_response
from AdsReporting import (
    ADMIN_KEY,
    LEGACY_CREATIVE_TYPES,
    POCKETBASE_ADS_CAMPAIGNS_COLLECTION,
    POCKETBASE_BATCH_SIZE,
    POCKETBASE_URL,
    check_report_status,
    download_report_rows,
    fetch_campaign_to_portfolio_name,
    fetch_campaign_to_profile_id,
    last_recorded_date,
    pb_authenticate,
    pb_batch,
    pb_list_connected,
    poll_legacy_report,
    refresh_access_token,
    request_campaign_report,
)

SYSTEM_TZ = ZoneInfo("Asia/Jerusalem")  # matches this machine's local cron timezone, not Amazon's
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


def request_legacy_keyword_report(base_url, access_token, client_id, ads_profile_id, date_str, creative_type):
    """Same v2 legacy endpoint family as AdsReporting.request_legacy_report,
    but /v2/hsa/keywords/report - confirmed live 2026-09-06 to exist and
    return real keyword-level data for legacy Sponsored Brands campaigns
    (video and non-video) that the modern v3 sbTargeting report never
    covers at all, the same exclusion already known and worked around at
    the campaign level (see AdsReporting.py)."""
    body = {
        "reportDate": date_str.replace("-", ""),
        "metrics": "campaignId,adGroupId,keywordId,keywordText,matchType,impressions,clicks,cost,attributedSales14d,attributedConversions14d",
    }
    if creative_type:
        body["creativeType"] = creative_type
    resp = requests.post(
        f"{base_url}/v2/hsa/keywords/report",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/json",
        },
        json=body,
        timeout=30,
    )
    if resp.status_code != 202:
        raise RuntimeError(f"Legacy keyword report request failed (creativeType={creative_type}): HTTP {resp.status_code} - {resp.text}")
    return resp.json()["reportId"]


def legacy_keyword_row_to_body(ads_profile, row, date_str, campaign_names):
    """This v2 legacy report has no campaignName/adGroupName/bid fields at
    all (unlike the v3 report) - campaign_name is backfilled from the live
    ads_campaigns snapshot; ad_group_name and bid are left blank/None, same
    tolerance GetAdsKeywordStats already has for a row missing them."""
    year, month = 0, 0
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        year, month = parsed.year, parsed.month
    except ValueError:
        pass
    campaign_id = str(row.get("campaignId"))
    return {
        "profile_id": str(ads_profile.get("profileId")),
        "campaign_id": campaign_id,
        "campaign_name": campaign_names.get(campaign_id, ""),
        "campaign_status": "",
        "ad_group_id": str(row.get("adGroupId", "")),
        "ad_group_name": "",
        "target_id": str(row.get("keywordId", "")),
        "target_text": row.get("keywordText", ""),
        "target_type": "",
        "match_type": row.get("matchType", ""),
        "country_code": ads_profile.get("countryCode", ""),
        "currency_code": ads_profile.get("currencyCode", ""),
        "ad_product": "SPONSORED_BRANDS",
        "date": date_str,
        "month": month,
        "year": year,
        "impressions": row.get("impressions", 0),
        "clicks": row.get("clicks", 0),
        "spend": row.get("cost", 0),
        "sales": row.get("attributedSales14d", 0),
        "orders": row.get("attributedConversions14d", 0),
        "bid": None,
    }


def pb_known_target_ids(token, profile_id, ad_product, date_str):
    ids = set()
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_KEYWORD_COLLECTION}/records",
            headers={"Authorization": token},
            params={
                "filter": f'(profile_id = "{profile_id}" && ad_product = "{ad_product}" && date = "{date_str}")',
                "perPage": 500,
                "fields": "target_id",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        ids.update(item["target_id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


def fetch_campaign_names(token):
    names = {}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_CAMPAIGNS_COLLECTION}/records",
            headers={"Authorization": token},
            params={"perPage": 500, "page": page, "fields": "campaign_id,campaign_name"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            names[item.get("campaign_id")] = item.get("campaign_name", "")
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return names


def pull_and_store_legacy_sb_keyword_stats(pb_token, connections, start_date, end_date, errors):
    """Supplements the modern keyword pull with legacy-only Sponsored Brands
    campaigns (both video and non-video, see LEGACY_CREATIVE_TYPES) for
    every day in the range, per profile - same exclusion and same v2
    fallback pattern as AdsReporting.pull_and_store_legacy_sb_stats, but at
    keyword level (confirmed live 2026-09-06: legacy SB campaigns like "FR
    PAREO VIDEO OLD" never get keyword-level rows from the modern
    sbTargeting v3 report at all, unlike at the campaign level where at
    least aggregate totals came through). Skips any target_id already
    written for that (profile, date) by the v3 pull, since the v2 legacy
    report also includes modern SBv4 campaigns' keywords already captured
    correctly there - summing both would double-count them."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_strs = []
    d = start
    while d <= end:
        date_strs.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    campaign_names = fetch_campaign_names(pb_token)
    rows_written = 0
    for connection in connections:
        profile_key = connection.get("region")
        refresh_token = connection.get("refresh_token")
        if profile_key not in AD_PROFILES or not refresh_token:
            continue
        try:
            access_token = refresh_access_token(profile_key, refresh_token)
        except Exception as exc:
            errors.append(f"legacy-kw {profile_key}: token refresh failed: {exc}")
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

            for date_str in date_strs:
                for creative_type in LEGACY_CREATIVE_TYPES:
                    try:
                        known_ids = pb_known_target_ids(pb_token, str(ads_profile_id), "SPONSORED_BRANDS", date_str)
                        report_id = request_legacy_keyword_report(
                            base_url, access_token, client_id, ads_profile_id, date_str, creative_type
                        )
                        rows = poll_legacy_report(base_url, headers, report_id)
                        bodies = [
                            legacy_keyword_row_to_body(ads_profile, row, date_str, campaign_names)
                            for row in rows
                            if str(row.get("keywordId")) not in known_ids and (row.get("cost") or row.get("attributedSales14d"))
                        ]
                        ops = [
                            {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_KEYWORD_COLLECTION}/records", "body": b}
                            for b in bodies
                        ]
                        for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
                            pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])
                        rows_written += len(bodies)
                    except Exception as exc:
                        errors.append(f"legacy-kw({creative_type}) {profile_key}/{ads_profile_id} {date_str}: {exc}")

    return rows_written


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

    # Legacy Sponsored Brands campaigns (no brandEntityId) are invisible to
    # the v3 pull above at keyword level too, not just campaign level - see
    # pull_and_store_legacy_sb_keyword_stats.
    legacy_rows_written = pull_and_store_legacy_sb_keyword_stats(pb_token, connections, start_date, end_date, errors)

    return {
        "rowsWritten": rows_written + legacy_rows_written,
        "legacyRowsWritten": legacy_rows_written,
        "profilesPulled": len(profile_ids),
        "errors": errors,
    }


def UpdateAdsKeywordStats(request):
    if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
        return json_response({"error": "Unauthorized"}, 401)

    now_local = datetime.now(SYSTEM_TZ)
    yesterday = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    if request.args.get("start_date"):
        start_date = request.args["start_date"]
        end_date = request.args.get("end_date", yesterday)
    else:
        # Default to since-the-last-recorded-day (capped at Amazon's 31-day
        # max range) rather than a fixed trailing 7 days, so a missed
        # weekly run gets backfilled automatically by the next one instead
        # of silently losing whatever fell outside the fixed window.
        start_date = (now_local - timedelta(days=7)).strftime("%Y-%m-%d")
        try:
            token = pb_authenticate()
            last_date = last_recorded_date(token, "ads_keyword_stats")
            if last_date:
                gap_start = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                floor_date = (now_local - timedelta(days=31)).strftime("%Y-%m-%d")
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
    portfolio = request.args.get("portfolio") if hasattr(request, "args") else None

    if not start_date or not end_date:
        # Back-compat: no explicit range means "this month".
        month = request.args.get("month", type=int) if hasattr(request, "args") else None
        year = request.args.get("year", type=int) if hasattr(request, "args") else None
        now_local = datetime.now(SYSTEM_TZ)
        month, year = month or now_local.month, year or now_local.year
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
                # Keyed WITH profile_id (per-profile totals kept separate
                # here) - the actual cross-profile de-dup happens in a
                # second pass below, per the user's correction (2026-09-06):
                # the same real campaign/ad group/target can be reported
                # under more than one Amazon Ads profile for this account,
                # and it's the SAME underlying campaign in both, not two
                # independent ones - so their numbers must not be summed
                # together (that would double the real total). Instead,
                # exactly one profile's own numbers are kept, discarding the
                # other's entirely.
                key = (item.get("profile_id"), item.get("campaign_id"), item.get("ad_group_id"), item.get("target_id"))
                bucket = keywords.setdefault(key, {
                    "profileId": item.get("profile_id"),
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
                    "bid": None, "bidUnverified": False, "_bidDate": "", "_nameDate": "", "_lastDate": "",
                })
                bucket["impressions"] += item.get("impressions", 0)
                bucket["clicks"] += item.get("clicks", 0)
                bucket["spend"] += item.get("spend", 0)
                bucket["sales"] += item.get("sales", 0)
                bucket["orders"] += item.get("orders", 0)
                # bid is a current setting, not a metric to sum - keep the
                # value from whichever row in range is most recent.
                # A stored 0 means "unknown" (a PocketBase number field
                # stores a written null as 0, and no real Amazon bid is
                # ever $0), never let it overwrite an actually-known bid.
                if item.get("bid") and item.get("date", "") >= bucket["_bidDate"]:
                    bucket["bid"] = item.get("bid")
                    bucket["_bidDate"] = item.get("date", "")
                # campaignName can change mid-window if the campaign gets
                # renamed - show the name from the most recent day in range,
                # not whichever row happened to arrive first (campaign_id,
                # not name, is always the real grouping key here).
                if item.get("campaign_name") and item.get("date", "") >= bucket["_nameDate"]:
                    bucket["campaignName"] = item.get("campaign_name")
                    bucket["_nameDate"] = item.get("date", "")
                bucket["_lastDate"] = max(bucket["_lastDate"], item.get("date", ""))
            if page >= data.get("totalPages", 1):
                break
            page += 1

        # Second pass: when the same real campaign/ad group/target came
        # back under more than one profile_id, keep only whichever profile
        # has the most recent activity - not a sum of both.
        winners = {}
        candidates_by_outer = {}
        for bucket in keywords.values():
            outer_key = (bucket["campaignId"], bucket["adGroupId"], bucket["targetId"])
            candidates_by_outer.setdefault(outer_key, []).append(bucket)
            current = winners.get(outer_key)
            if current is None or bucket["_lastDate"] > current["_lastDate"]:
                winners[outer_key] = bucket
        # The winning profile can be the legacy-SB-sourced one (see
        # pull_and_store_legacy_sb_keyword_stats), which has neither a real
        # bid nor an ad_group_name at all - found live 2026-09-06 as a real
        # bid showing as "$0.00" once a legacy row became the most recent.
        # Backfill just those two fields from a losing candidate that
        # actually has them, without touching the winner's own real (and
        # more current) metrics. bidUnverified marks that the bid did NOT
        # come from the winning (more current) source - it's whatever a
        # possibly-old historical/manually-imported row last reported, with
        # no way to confirm it's still accurate today. Per the user
        # (2026-09-06), Apply is blocked entirely on an unverified bid (see
        # propose_bid_change/propose_bid_change_multi_period) since the %
        # change would be calculated from a baseline that might not be real
        # anymore.
        for outer_key, winner in winners.items():
            if winner["bid"] and winner["adGroupName"]:
                continue
            for candidate in candidates_by_outer[outer_key]:
                if candidate is winner:
                    continue
                if not winner["bid"] and candidate["bid"]:
                    winner["bid"] = candidate["bid"]
                    winner["bidUnverified"] = True
                if not winner["adGroupName"] and candidate["adGroupName"]:
                    winner["adGroupName"] = candidate["adGroupName"]
        keywords = winners

        # Third pass: fold a manual-import synthetic placeholder row (no
        # real ad_group_id/target_id - see import_manual_ads_report.py's
        # "unknown-{campaign_id}-..." convention) into the real live-
        # pipeline row for the same keyword text/match type in the same
        # campaign, per the user's request (2026-09-06). Unlike the
        # profile-duplication case above, these two rows cover different
        # real time periods of the same keyword's history (the manual
        # import predates live keyword-level tracking), so their numbers
        # are additive, not duplicate readings of the same period - summed
        # rather than picking just one.
        real_by_text = {}
        for bucket in keywords.values():
            if not str(bucket["targetId"]).startswith("unknown-"):
                real_by_text[(bucket["campaignId"], bucket["targetText"].strip().lower(), bucket["matchType"])] = bucket
        for key, bucket in list(keywords.items()):
            if not str(bucket["targetId"]).startswith("unknown-"):
                continue
            real = real_by_text.get((bucket["campaignId"], bucket["targetText"].strip().lower(), bucket["matchType"]))
            if real is None or real is bucket:
                continue
            real["impressions"] += bucket["impressions"]
            real["clicks"] += bucket["clicks"]
            real["spend"] += bucket["spend"]
            real["sales"] += bucket["sales"]
            real["orders"] += bucket["orders"]
            del keywords[key]

        campaign_to_portfolio = fetch_campaign_to_portfolio_name(token)
        campaign_to_profile = fetch_campaign_to_profile_id(token)
        rows = list(keywords.values())
        for row in rows:
            row["portfolioName"] = campaign_to_portfolio.get(row["campaignId"], "")
            # The live ads_campaigns snapshot is more trustworthy than
            # whatever profile_id a stats row happens to carry (a manual
            # import can tag the wrong one of this account's several
            # per-country profiles) - only falls back to the stats row's
            # own profile_id for a campaign no longer in the live snapshot.
            row["profileId"] = campaign_to_profile.get(row["campaignId"], row["profileId"])
        if portfolio:
            rows = [r for r in rows if r["portfolioName"] == portfolio]

        rows.sort(key=lambda k: -k["spend"])
        for row in rows:
            row["acos"] = (row["spend"] / row["sales"] * 100) if row["sales"] else 0
            row.pop("_bidDate", None)
            row.pop("_nameDate", None)
            row.pop("_lastDate", None)

        return json_response({
            "startDate": start_date,
            "endDate": end_date,
            "keywords": rows,
            "portfolios": sorted({p for p in campaign_to_portfolio.values() if p}),
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
