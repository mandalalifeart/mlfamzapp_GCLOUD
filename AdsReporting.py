import gzip
import io
import json
import os
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from AdsAuth import (
    AD_PROFILES,
    ADS_REGION_ENDPOINTS,
    LWA_TOKEN_URL,
    POCKETBASE_ADMIN_EMAIL,
    POCKETBASE_ADMIN_PASSWORD,
    POCKETBASE_URL,
    cors_headers,
    json_response,
)

POCKETBASE_ADS_COLLECTION = os.environ.get("POCKETBASE_ADS_COLLECTION", "ads_connections")
POCKETBASE_ADS_STATS_COLLECTION = os.environ.get("POCKETBASE_ADS_STATS_COLLECTION", "ads_campaign_stats")
POCKETBASE_ADS_CAMPAIGNS_COLLECTION = os.environ.get("POCKETBASE_ADS_CAMPAIGNS_COLLECTION", "ads_campaigns")
POCKETBASE_BATCH_SIZE = int(os.environ.get("POCKETBASE_BATCH_SIZE", "50"))
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

LA_TZ = ZoneInfo("America/Los_Angeles")

# Sponsored Products' reporting columns use a "7d" attribution-window suffix
# (purchases7d/sales7d); Sponsored Brands and Sponsored Display don't - both
# schemas were confirmed live (report submission accepted with no
# "invalid configuration" error) against the real account rather than
# guessed, same as the original spCampaigns validation.
AD_PRODUCTS = [
    {
        "key": "SP",
        "ad_product": "SPONSORED_PRODUCTS",
        "report_type_id": "spCampaigns",
        "columns": ["date", "campaignId", "campaignName", "campaignStatus", "impressions", "clicks", "cost", "purchases7d", "sales7d"],
        "sales_field": "sales7d",
        "purchases_field": "purchases7d",
    },
    {
        "key": "SB",
        "ad_product": "SPONSORED_BRANDS",
        "report_type_id": "sbCampaigns",
        "columns": ["date", "campaignId", "campaignName", "campaignStatus", "impressions", "clicks", "cost", "purchases", "sales"],
        "sales_field": "sales",
        "purchases_field": "purchases",
    },
    {
        "key": "SD",
        "ad_product": "SPONSORED_DISPLAY",
        "report_type_id": "sdCampaigns",
        "columns": ["date", "campaignId", "campaignName", "campaignStatus", "impressions", "clicks", "cost", "purchases", "sales"],
        "sales_field": "sales",
        "purchases_field": "purchases",
    },
]
# Amazon's docs site is a JS SPA that can't be scraped for the exact status
# enum, so both spellings are accepted defensively rather than guessing one.
REPORT_DONE_STATUSES = {"COMPLETED", "SUCCESS"}
REPORT_FAILED_STATUSES = {"FAILURE", "FAILED", "CANCELLED"}
# Reports are submitted for every profile up front, then polled together in
# rounds (rather than one profile fully polled before the next starts) -
# since all pending jobs are checked every round regardless of count, total
# wall time is bounded by the slowest job, not the number of profiles/ad
# products. Confirmed live: a first-time sbCampaigns/sdCampaigns report on
# this account took ~25 minutes to complete (spCampaigns was faster, a few
# minutes), so the poll budget is set close to Cloud Scheduler's 30-minute
# HTTP attemptDeadline ceiling - the real limiting factor - with headroom
# left for the submit phase.
REPORT_POLL_ROUNDS = 165
REPORT_POLL_DELAY_SECONDS = 10


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase auth failed: HTTP {response.status_code} - {response.text}")
    token = response.json().get("token")
    if not token:
        raise RuntimeError("PocketBase auth response missing token")
    return token


def pb_batch(token, batch_requests):
    if not batch_requests:
        return
    response = requests.post(
        f"{POCKETBASE_URL}/api/batch",
        headers={"Authorization": token},
        json={"requests": batch_requests},
        timeout=60,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase batch request failed: HTTP {response.status_code} - {response.text}")
    for entry, result in zip(batch_requests, response.json()):
        status = result.get("status")
        if status is None or status >= 400:
            raise RuntimeError(
                f"PocketBase batch item failed ({entry['method']} {entry['url']}): {result.get('body')}"
            )


def pb_list_connected(token):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_COLLECTION}/records",
        headers={"Authorization": token},
        params={"filter": 'status = "connected"', "perPage": 200},
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("items", [])


def pb_list_stats_ids(token, profile_id, start_date, end_date):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_STATS_COLLECTION}/records",
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


def refresh_access_token(profile_key, refresh_token):
    profile = AD_PROFILES[profile_key]
    response = requests.post(
        LWA_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": profile["client_id"],
            "client_secret": profile["client_secret"],
        },
        timeout=15,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Access token refresh failed: HTTP {response.status_code} - {response.text}")
    access_token = response.json().get("access_token")
    if not access_token:
        raise RuntimeError("Token refresh response missing access_token")
    return access_token


def request_campaign_report(base_url, access_token, client_id, ads_profile_id, start_date, end_date, product, group_by=("campaign",)):
    # Submitting SP+SB+SD reports for every profile up front (rather than one
    # ad product at a time) triggers Amazon's per-account throttling (429) in
    # bursts - retry with backoff rather than treating it as a hard failure.
    # group_by defaults to campaign-level (used by the campaign stats
    # pipeline); AdsKeywordReporting.py reuses this same function with
    # group_by=["targeting"] rather than duplicating the retry/425 handling.
    response = None
    for attempt in range(6):
        response = requests.post(
            f"{base_url}/reporting/reports",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Amazon-Advertising-API-ClientId": client_id,
                "Amazon-Advertising-API-Scope": str(ads_profile_id),
                "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            },
            json={
                "name": f"{product['report_type_id']} {start_date} to {end_date}",
                "startDate": start_date,
                "endDate": end_date,
                "configuration": {
                    "adProduct": product["ad_product"],
                    "groupBy": list(group_by),
                    "columns": product["columns"],
                    "reportTypeId": product["report_type_id"],
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            },
            timeout=30,
        )
        if response.status_code != 429:
            break
        time.sleep(5 * (attempt + 1))

    if response.status_code == 425:
        # Amazon rejects a repeat request for the same profile/date/report
        # type with 425 and names the still-in-flight report's id instead of
        # creating a new one - reuse that id rather than treating it as an
        # error (this fires often since a timed-out client retry still lands
        # the original request server-side).
        match = re.search(r"duplicate of\s*:\s*([\w-]+)", response.text)
        if match:
            return match.group(1)
        raise RuntimeError(f"Report request duplicate (425) but no id found: {response.text}")
    if response.status_code not in (200, 202):
        raise RuntimeError(f"Report request failed: HTTP {response.status_code} - {response.text}")
    report_id = response.json().get("reportId")
    if not report_id:
        raise RuntimeError(f"Report request response missing reportId: {response.text}")
    return report_id


def check_report_status(job):
    response = requests.get(f"{job['base_url']}/reporting/reports/{job['report_id']}", headers=job["headers"], timeout=15)
    if response.status_code == 429:
        # Polling ~3x as many jobs per round (SP+SB+SD) makes transient
        # throttling on the status check itself more likely - treat it as
        # still-pending rather than a hard failure, same round's other jobs
        # aren't affected.
        return "pending", None
    if response.status_code != 200:
        raise RuntimeError(f"Report status check failed: HTTP {response.status_code} - {response.text}")
    body = response.json()
    status = (body.get("status") or "").upper()
    if status in REPORT_DONE_STATUSES:
        download_url = body.get("url") or body.get("location")
        if not download_url:
            raise RuntimeError(f"Report completed but no download url in response: {body}")
        return "done", download_url
    if status in REPORT_FAILED_STATUSES:
        raise RuntimeError(f"Report generation failed: {body}")
    return "pending", None


CAMPAIGN_STATE_KEEP = {"ENABLED", "PAUSED"}


def fetch_sp_campaigns(base_url, access_token, client_id, ads_profile_id):
    response = requests.post(
        f"{base_url}/sp/campaigns/list",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.spCampaign.v3+json",
            "Accept": "application/vnd.spCampaign.v3+json",
        },
        json={"stateFilter": {"include": list(CAMPAIGN_STATE_KEEP)}},
        timeout=30,
    )
    response.raise_for_status()
    campaigns = []
    for c in response.json().get("campaigns", []):
        budget = c.get("budget") or {}
        campaigns.append({
            "campaign_id": str(c.get("campaignId")),
            "campaign_name": c.get("name", ""),
            "campaign_status": (c.get("state") or "").upper(),
            "targeting_type": c.get("targetingType", ""),
            "budget": budget.get("budget", 0),
            "budget_type": budget.get("budgetType", ""),
            "portfolio_id": str(c.get("portfolioId") or ""),
        })
    return campaigns


def fetch_sb_campaigns(base_url, access_token, client_id, ads_profile_id):
    response = requests.post(
        f"{base_url}/sb/v4/campaigns/list",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.sbcampaignresource.v4+json",
            "Accept": "application/vnd.sbcampaignresource.v4+json",
        },
        json={},
        timeout=30,
    )
    response.raise_for_status()
    campaigns = []
    for c in response.json().get("campaigns", []):
        state = (c.get("state") or "").upper()
        if state not in CAMPAIGN_STATE_KEEP:
            continue
        campaigns.append({
            "campaign_id": str(c.get("campaignId")),
            "campaign_name": c.get("name", ""),
            "campaign_status": state,
            "targeting_type": "",
            "budget": c.get("budget", 0),
            "budget_type": c.get("budgetType", ""),
            "portfolio_id": str(c.get("portfolioId") or ""),
        })
    return campaigns


def fetch_sd_campaigns(base_url, access_token, client_id, ads_profile_id):
    # SD's campaign list is still the older, unversioned endpoint - a bare
    # array response with lowercase state values, unlike SP/SB.
    response = requests.get(
        f"{base_url}/sd/campaigns",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/json",
        },
        timeout=30,
    )
    response.raise_for_status()
    campaigns = []
    for c in response.json():
        state = (c.get("state") or "").upper()
        if state not in CAMPAIGN_STATE_KEEP:
            continue
        campaigns.append({
            "campaign_id": str(c.get("campaignId")),
            "campaign_name": c.get("name", ""),
            "campaign_status": state,
            "targeting_type": "",
            "budget": c.get("budget", 0),
            "budget_type": c.get("budgetType", ""),
            "portfolio_id": str(c.get("portfolioId") or ""),
        })
    return campaigns


CAMPAIGN_LIST_FETCHERS = {"SP": fetch_sp_campaigns, "SB": fetch_sb_campaigns, "SD": fetch_sd_campaigns}

POCKETBASE_ADS_PORTFOLIOS_COLLECTION = os.environ.get("POCKETBASE_ADS_PORTFOLIOS_COLLECTION", "ads_portfolios")


def fetch_portfolios(base_url, access_token, client_id, ads_profile_id):
    """Confirmed live: POST /portfolios/list with the v3 portfolio content
    type (GET /portfolios and /portfolios/extended both 404 - portfolios is
    a POST-list-style endpoint like the SP/SB campaign lists, not a plain
    GET)."""
    response = requests.post(
        f"{base_url}/portfolios/list",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.portfolio.v3+json",
            "Accept": "application/vnd.portfolio.v3+json",
        },
        json={},
        timeout=30,
    )
    response.raise_for_status()
    return [
        {
            "portfolio_id": str(p.get("portfolioId")),
            "name": p.get("name", ""),
            "state": (p.get("state") or "").upper(),
        }
        for p in response.json().get("portfolios", [])
    ]


def pb_list_portfolio_ids(token, profile_id):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_PORTFOLIOS_COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": f'profile_id = "{profile_id}"', "fields": "id", "perPage": 200, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        ids.extend(item["id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


def pull_and_store_campaign_lists(pb_token, connections, errors):
    # Written per-profile as soon as that profile's lists are fetched, rather
    # than accumulated into one big list and written at the very end - a
    # single end-of-run batch write loses everything if the request is
    # killed by the platform's own timeout before it gets there (this is
    # exactly what happened to the first 31-day stats backfill attempt).
    ad_product_by_key = {p["key"]: p["ad_product"] for p in AD_PRODUCTS}
    total_written = 0

    for connection in connections:
        profile_key = connection.get("region")
        refresh_token = connection.get("refresh_token")
        if profile_key not in AD_PROFILES or not refresh_token:
            continue

        try:
            access_token = refresh_access_token(profile_key, refresh_token)
        except Exception as exc:
            errors.append(f"{profile_key}: token refresh failed (campaign list): {exc}")
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

            profile_campaigns = []
            for key, fetcher in CAMPAIGN_LIST_FETCHERS.items():
                try:
                    campaigns = fetcher(base_url, access_token, client_id, ads_profile_id)
                except Exception as exc:
                    errors.append(f"{profile_key}/{ads_profile_id} ({ads_profile.get('countryCode')}) {key} list: {exc}")
                    time.sleep(1)
                    continue
                for c in campaigns:
                    profile_campaigns.append({
                        **c,
                        "profile_id": str(ads_profile_id),
                        "ad_product": ad_product_by_key[key],
                        "country_code": ads_profile.get("countryCode", ""),
                        "currency_code": ads_profile.get("currencyCode", ""),
                    })
                time.sleep(1)

            try:
                existing_ids = pb_list_campaign_ids(pb_token, str(ads_profile_id))
                ops = [
                    {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ADS_CAMPAIGNS_COLLECTION}/records/{rid}"}
                    for rid in existing_ids
                ]
                ops.extend(
                    {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_CAMPAIGNS_COLLECTION}/records", "body": c}
                    for c in profile_campaigns
                )
                for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
                    pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])
                total_written += len(profile_campaigns)
            except Exception as exc:
                errors.append(f"{profile_key}/{ads_profile_id} ({ads_profile.get('countryCode')}): campaign list write failed: {exc}")

            # Portfolios (id -> name, e.g. "Pareo"/"POUF") - same
            # snapshot-refresh pattern as the campaign list, added 2026-08-29
            # per the user's request for a portfolio filter on the bid
            # optimizer page.
            try:
                portfolios = fetch_portfolios(base_url, access_token, client_id, ads_profile_id)
                portfolio_rows = [
                    {**p, "profile_id": str(ads_profile_id), "country_code": ads_profile.get("countryCode", "")}
                    for p in portfolios
                ]
                existing_portfolio_ids = pb_list_portfolio_ids(pb_token, str(ads_profile_id))
                ops = [
                    {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ADS_PORTFOLIOS_COLLECTION}/records/{rid}"}
                    for rid in existing_portfolio_ids
                ]
                ops.extend(
                    {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_PORTFOLIOS_COLLECTION}/records", "body": p}
                    for p in portfolio_rows
                )
                for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
                    pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])
            except Exception as exc:
                errors.append(f"{profile_key}/{ads_profile_id} ({ads_profile.get('countryCode')}): portfolio list failed: {exc}")
            time.sleep(1)

    return total_written


def pb_list_campaign_ids(token, profile_id):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_CAMPAIGNS_COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": f'profile_id = "{profile_id}"', "fields": "id", "perPage": 200, "page": page},
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


def download_report_rows(download_url):
    response = requests.get(download_url, timeout=60)
    response.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        return json.loads(gz.read().decode("utf-8"))


def campaign_row_to_body(ads_profile, row, product):
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
        "campaign_status": row.get("campaignStatus", ""),
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


def submit_report_jobs(connections, start_date, end_date, errors):
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
            # Amazon Attribution "agency" profiles don't support sponsored-ads
            # campaign reports (HTTP 400 "Invalid Advertiser or Marketplace
            # ID") - skip them rather than retry a permanent failure daily.
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
            for product in AD_PRODUCTS:
                try:
                    report_id = request_campaign_report(
                        base_url, access_token, client_id, ads_profile_id, start_date, end_date, product
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
                # A small pace between submissions avoids triggering Amazon's
                # burst throttling in the first place (observed firing off two
                # requests back-to-back with no gap) - cheaper than relying
                # solely on the 429 retry-with-backoff in request_campaign_report.
                # 1s still let a submission get throttled occasionally in
                # practice across ~30 requests/run, so this is a bit wider.
                time.sleep(2)

    return jobs


def poll_and_store_jobs(pb_token, jobs, errors):
    # Each completed report's rows are written to PocketBase immediately,
    # not accumulated and written once at the end - a wide date range makes
    # report generation slow enough that the whole request can hit the Cloud
    # Function's own timeout, and the platform kills the request outright at
    # that point. A single final batch write would lose every row downloaded
    # during the run; writing as each job finishes means only the
    # still-pending jobs are lost, and everything else survives.
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
                bodies = [campaign_row_to_body(job["ads_profile"], row, job["product"]) for row in rows]
                ops = [
                    {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_STATS_COLLECTION}/records", "body": b}
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


LEGACY_VIDEO_POLL_ROUNDS = 15
LEGACY_VIDEO_POLL_DELAY_SECONDS = 8


# None = default/non-video legacy campaigns (e.g. "USAC SB VIN KW", a
# keyword-targeted legacy campaign, $10.61/month - tiny but real); "video" =
# legacy Sponsored Brands Video. Both are invisible to the v3 Reporting API.
LEGACY_CREATIVE_TYPES = [None, "video"]


def request_legacy_report(base_url, access_token, client_id, ads_profile_id, date_str, creative_type):
    """Legacy Sponsored Brands campaigns (created before this account's Brand
    Registry / SBv4 migration - no brandEntityId) never appear in the modern
    Reporting API v3 SPONSORED_BRANDS report, confirmed empirically
    2026-08-28 (a byte-identical fresh v3 pull still excluded them) and via
    Amazon's own docs (v3 only covers SBv4/"SB2"-format campaigns - legacy SB
    and SBV need the old v2 reporting endpoints). Video creatives specifically
    need `"creativeType": "video"` on the v2 request or they're excluded too;
    non-video legacy campaigns need the field omitted entirely. v2 reports
    are per single day (`reportDate`), not a date range like v3, but generate
    far faster (~15-30s vs 15-25min for v3 SB)."""
    body = {
        "reportDate": date_str.replace("-", ""),
        "metrics": "campaignId,campaignName,campaignStatus,impressions,clicks,cost,attributedSales14d,attributedConversions14d",
    }
    if creative_type:
        body["creativeType"] = creative_type
    resp = requests.post(
        f"{base_url}/v2/hsa/campaigns/report",
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
        raise RuntimeError(f"Legacy report request failed (creativeType={creative_type}): HTTP {resp.status_code} - {resp.text}")
    return resp.json()["reportId"]


def poll_legacy_report(base_url, headers, report_id):
    for _ in range(LEGACY_VIDEO_POLL_ROUNDS):
        resp = requests.get(f"{base_url}/v2/reports/{report_id}", headers=headers, timeout=15)
        data = resp.json()
        status = data.get("status")
        if status == "SUCCESS":
            download = requests.get(data["location"], headers=headers, timeout=30)
            try:
                return json.loads(gzip.GzipFile(fileobj=io.BytesIO(download.content)).read())
            except Exception:
                return download.json()
        if status == "FAILURE":
            raise RuntimeError(f"Legacy video report generation failed: {data}")
        time.sleep(LEGACY_VIDEO_POLL_DELAY_SECONDS)
    raise RuntimeError(f"Legacy video report {report_id} timed out")


def legacy_row_to_body(ads_profile, row, date_str):
    return {
        "profile_id": str(ads_profile.get("profileId")),
        "campaign_id": str(row.get("campaignId")),
        "campaign_name": row.get("campaignName", ""),
        "campaign_status": (row.get("campaignStatus") or "").upper(),
        "country_code": ads_profile.get("countryCode", ""),
        "currency_code": ads_profile.get("currencyCode", ""),
        "ad_product": "SPONSORED_BRANDS",
        "date": date_str,
        "month": int(date_str[5:7]),
        "year": int(date_str[0:4]),
        "impressions": row.get("impressions", 0),
        "clicks": row.get("clicks", 0),
        "spend": row.get("cost", 0),
        "sales": row.get("attributedSales14d", 0),
        "orders": row.get("attributedConversions14d", 0),
    }


def pull_and_store_legacy_sb_stats(pb_token, connections, start_date, end_date, errors):
    """Supplements the modern SB pull with legacy-only campaigns (both video
    and non-video, see LEGACY_CREATIVE_TYPES) for every day in the range, per
    profile - skips any campaign_id already written by the v3 pull for that
    (profile, date), since the v2 legacy report also includes modern SBv4
    campaigns already captured correctly there and summing both would
    double-count them."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_strs = []
    d = start
    while d <= end:
        date_strs.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    rows_written = 0
    for connection in connections:
        profile_key = connection.get("region")
        refresh_token = connection.get("refresh_token")
        if profile_key not in AD_PROFILES or not refresh_token:
            continue
        try:
            access_token = refresh_access_token(profile_key, refresh_token)
        except Exception as exc:
            errors.append(f"legacy-video {profile_key}: token refresh failed: {exc}")
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
                        known_ids = pb_known_campaign_ids(pb_token, str(ads_profile_id), "SPONSORED_BRANDS", date_str)
                        report_id = request_legacy_report(base_url, access_token, client_id, ads_profile_id, date_str, creative_type)
                        rows = poll_legacy_report(base_url, headers, report_id)
                        bodies = [
                            legacy_row_to_body(ads_profile, row, date_str)
                            for row in rows
                            if str(row.get("campaignId")) not in known_ids and (row.get("cost") or row.get("attributedSales14d"))
                        ]
                        ops = [
                            {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_STATS_COLLECTION}/records", "body": b}
                            for b in bodies
                        ]
                        for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
                            pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])
                        rows_written += len(bodies)
                    except Exception as exc:
                        errors.append(f"legacy({creative_type}) {profile_key}/{ads_profile_id} {date_str}: {exc}")

    return rows_written


def pb_known_campaign_ids(token, profile_id, ad_product, date_str):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_STATS_COLLECTION}/records",
        headers={"Authorization": token},
        params={
            "filter": f'(profile_id = "{profile_id}" && ad_product = "{ad_product}" && date = "{date_str}")',
            "perPage": 500,
            "fields": "campaign_id",
        },
        timeout=30,
    )
    response.raise_for_status()
    return {item["campaign_id"] for item in response.json().get("items", [])}


def pull_and_store_campaign_stats(start_date, end_date):
    pb_token = pb_authenticate()
    connections = pb_list_connected(pb_token)
    errors = []

    # Campaign lists first (fast, and independent of the date range) so that
    # data is safely persisted even if the slower report-polling phase below
    # times out.
    campaigns_written = pull_and_store_campaign_lists(pb_token, connections, errors)

    # Clear this date range's existing stats up front, before any report
    # completes - each completed job's rows are then inserted incrementally
    # as they arrive (see poll_and_store_jobs), so nothing is deleted after
    # the fact.
    profile_ids = set()
    for connection in connections:
        for ads_profile in connection.get("profiles", []) or []:
            if ads_profile.get("accountType") != "agency" and ads_profile.get("profileId"):
                profile_ids.add(str(ads_profile.get("profileId")))
    for profile_id in profile_ids:
        existing_ids = pb_list_stats_ids(pb_token, profile_id, start_date, end_date)
        ops = [
            {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ADS_STATS_COLLECTION}/records/{rid}"}
            for rid in existing_ids
        ]
        for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
            pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])

    jobs = submit_report_jobs(connections, start_date, end_date, errors)
    rows_written = poll_and_store_jobs(pb_token, jobs, errors)

    # Legacy SB campaigns (no brandEntityId - both video and non-video) are
    # invisible to the v3 pull above regardless of filters - supplement with
    # the old v2 endpoint, deduped against what v3 already wrote. See
    # pull_and_store_legacy_sb_stats.
    legacy_rows_written = pull_and_store_legacy_sb_stats(pb_token, connections, start_date, end_date, errors)

    return {
        "rowsWritten": rows_written + legacy_rows_written,
        "legacyRowsWritten": legacy_rows_written,
        "profilesPulled": len(profile_ids),
        "campaignsListed": campaigns_written,
        "errors": errors,
    }


def last_recorded_date(token, collection=None):
    """Most recent `date` already in the given stats collection (defaults to
    ads_campaign_stats), or None if empty. Lets a scheduled pull catch up
    automatically after a missed run (e.g. the local cron machine was
    asleep) instead of only ever covering a fixed lookback window."""
    resp = requests.get(
        f"{POCKETBASE_URL}/api/collections/{collection or POCKETBASE_ADS_STATS_COLLECTION}/records",
        headers={"Authorization": token},
        params={"perPage": 1, "sort": "-date", "fields": "date"},
        timeout=30,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    return items[0]["date"] if items else None


def UpdateAdsCampaignStats(request):
    if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
        return json_response({"error": "Unauthorized"}, 401)

    now_la = datetime.now(LA_TZ)
    yesterday = (now_la - timedelta(days=1)).strftime("%Y-%m-%d")

    if request.args.get("start_date"):
        # Explicit range requested (e.g. a manual backfill) - unchanged behavior.
        start_date = request.args["start_date"]
        end_date = request.args.get("end_date", start_date)
    else:
        # No explicit range - default to "everything since the last
        # recorded day, through yesterday" instead of always just
        # yesterday, so a missed run (machine asleep, etc.) gets backfilled
        # automatically by the next successful one.
        start_date = yesterday
        try:
            token = pb_authenticate()
            last_date = last_recorded_date(token)
            if last_date:
                gap_start = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                # Amazon's Reporting API caps a single request at 31 days -
                # if the gap is bigger than that, just take the most recent
                # 31 days; anything older needs a manual backfill anyway.
                floor_date = (now_la - timedelta(days=31)).strftime("%Y-%m-%d")
                start_date = max(gap_start, floor_date)
        except Exception:
            pass  # fall back to "just yesterday" if the catch-up lookup itself fails
        end_date = yesterday

    if start_date > end_date:
        # Already caught up (e.g. this got invoked twice in one day) -
        # nothing new to pull.
        return json_response({"startDate": start_date, "endDate": end_date, "skipped": "already up to date"})

    try:
        result = pull_and_store_campaign_stats(start_date, end_date)
        return json_response({"startDate": start_date, "endDate": end_date, **result})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetAdsAccountSummary(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    month = request.args.get("month", type=int) if hasattr(request, "args") else None
    year = request.args.get("year", type=int) if hasattr(request, "args") else None
    if not month or not year:
        now_la = datetime.now(LA_TZ)
        month, year = month or now_la.month, year or now_la.year

    try:
        token = pb_authenticate()
        summary = {}
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_STATS_COLLECTION}/records",
                headers={"Authorization": token},
                params={"filter": f"(month = {month} && year = {year})", "perPage": 200, "page": page},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                key = item.get("country_code") or "UNKNOWN"
                bucket = summary.setdefault(key, {
                    "countryCode": key,
                    "currencyCode": item.get("currency_code", ""),
                    "impressions": 0, "clicks": 0, "spend": 0, "sales": 0, "orders": 0,
                })
                bucket["impressions"] += item.get("impressions", 0)
                bucket["clicks"] += item.get("clicks", 0)
                bucket["spend"] += item.get("spend", 0)
                bucket["sales"] += item.get("sales", 0)
                bucket["orders"] += item.get("orders", 0)
            if page >= data.get("totalPages", 1):
                break
            page += 1

        accounts = sorted(summary.values(), key=lambda a: -a["spend"])
        for account in accounts:
            account["acos"] = (account["spend"] / account["sales"] * 100) if account["sales"] else 0

        return json_response({"month": month, "year": year, "accounts": accounts})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetAdsCampaignStats(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    start_date = request.args.get("start_date") if hasattr(request, "args") else None
    end_date = request.args.get("end_date") if hasattr(request, "args") else None
    country_code = request.args.get("country_code") if hasattr(request, "args") else None

    if not start_date or not end_date:
        # Back-compat: no explicit range means "this month" - same default
        # the frontend used before the date-range picker was added.
        month = request.args.get("month", type=int) if hasattr(request, "args") else None
        year = request.args.get("year", type=int) if hasattr(request, "args") else None
        now_la = datetime.now(LA_TZ)
        month, year = month or now_la.month, year or now_la.year
        start_date = f"{year:04d}-{month:02d}-01"
        next_month_first = datetime(year + (month == 12), (month % 12) + 1, 1)
        end_date = (next_month_first - timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        token = pb_authenticate()
        campaigns = {}

        def make_bucket(item):
            return {
                "campaignId": item.get("campaign_id"),
                "campaignName": item.get("campaign_name", ""),
                "campaignStatus": item.get("campaign_status", ""),
                "adProduct": item.get("ad_product", ""),
                "countryCode": item.get("country_code", ""),
                "currencyCode": item.get("currency_code", ""),
                "impressions": 0, "clicks": 0, "spend": 0, "sales": 0, "orders": 0,
            }

        # Seed from the current campaign list first, so a zero-activity
        # ENABLED campaign still shows up with zero stats - a performance
        # report never emits a row for a campaign with no impressions in the
        # period. Only ENABLED campaigns are seeded (paused/archived are
        # deliberately excluded from this view), and the stats merge below
        # only adds to campaigns already seeded here rather than creating new
        # entries for a since-paused campaign's historical spend.
        campaign_filter = 'campaign_status = "ENABLED"'
        if country_code:
            campaign_filter += f' && country_code = "{country_code}"'
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_CAMPAIGNS_COLLECTION}/records",
                headers={"Authorization": token},
                params={k: v for k, v in {"filter": campaign_filter, "perPage": 200, "page": page}.items() if v},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                campaigns[(item.get("profile_id"), item.get("campaign_id"))] = make_bucket(item)
            if page >= data.get("totalPages", 1):
                break
            page += 1

        filter_str = f'(date >= "{start_date}" && date <= "{end_date}")'
        if country_code:
            filter_str += f' && country_code = "{country_code}"'

        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_STATS_COLLECTION}/records",
                headers={"Authorization": token},
                params={"filter": filter_str, "perPage": 200, "page": page},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                key = (item.get("profile_id"), item.get("campaign_id"))
                # Only add to a campaign already seeded as ENABLED above -
                # otherwise a since-paused campaign's historical spend would
                # create a new (non-ENABLED) entry and defeat the filter.
                bucket = campaigns.get(key)
                if not bucket:
                    continue
                bucket["impressions"] += item.get("impressions", 0)
                bucket["clicks"] += item.get("clicks", 0)
                bucket["spend"] += item.get("spend", 0)
                bucket["sales"] += item.get("sales", 0)
                bucket["orders"] += item.get("orders", 0)
            if page >= data.get("totalPages", 1):
                break
            page += 1

        rows = sorted(campaigns.values(), key=lambda c: -c["spend"])
        for row in rows:
            row["acos"] = (row["spend"] / row["sales"] * 100) if row["sales"] else 0

        return json_response({"startDate": start_date, "endDate": end_date, "campaigns": rows})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
