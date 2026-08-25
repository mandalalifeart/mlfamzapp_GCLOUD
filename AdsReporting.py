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
POCKETBASE_BATCH_SIZE = int(os.environ.get("POCKETBASE_BATCH_SIZE", "50"))
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

LA_TZ = ZoneInfo("America/Los_Angeles")

AD_PRODUCT = "SPONSORED_PRODUCTS"
REPORT_TYPE_ID = "spCampaigns"
REPORT_COLUMNS = [
    "date", "campaignId", "campaignName", "campaignStatus",
    "impressions", "clicks", "cost", "purchases7d", "sales7d",
]
# Amazon's docs site is a JS SPA that can't be scraped for the exact status
# enum, so both spellings are accepted defensively rather than guessing one.
REPORT_DONE_STATUSES = {"COMPLETED", "SUCCESS"}
REPORT_FAILED_STATUSES = {"FAILURE", "FAILED", "CANCELLED"}
# Reports are submitted for every profile up front, then polled together in
# rounds (rather than one profile fully polled before the next starts) - a
# dozen profiles at ~30-90s each would otherwise blow well past the Cloud
# Function timeout if handled strictly sequentially.
REPORT_POLL_ROUNDS = 30
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


def request_campaign_report(base_url, access_token, client_id, ads_profile_id, start_date, end_date):
    response = requests.post(
        f"{base_url}/reporting/reports",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
        },
        json={
            "name": f"spCampaigns {start_date} to {end_date}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": AD_PRODUCT,
                "groupBy": ["campaign"],
                "columns": REPORT_COLUMNS,
                "reportTypeId": REPORT_TYPE_ID,
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            },
        },
        timeout=30,
    )
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


def download_report_rows(download_url):
    response = requests.get(download_url, timeout=60)
    response.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        return json.loads(gz.read().decode("utf-8"))


def campaign_row_to_body(ads_profile, row):
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
        "ad_product": AD_PRODUCT,
        "date": date_str,
        "month": month,
        "year": year,
        "impressions": row.get("impressions", 0),
        "clicks": row.get("clicks", 0),
        "spend": row.get("cost", 0),
        "sales": row.get("sales7d", 0),
        "orders": row.get("purchases7d", 0),
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
            try:
                report_id = request_campaign_report(base_url, access_token, client_id, ads_profile_id, start_date, end_date)
                jobs.append({
                    "profile_key": profile_key,
                    "ads_profile": ads_profile,
                    "base_url": base_url,
                    "headers": headers,
                    "report_id": report_id,
                })
            except Exception as exc:
                errors.append(f"{profile_key}/{ads_profile_id} ({ads_profile.get('countryCode')}): {exc}")

    return jobs


def poll_jobs_until_done(jobs, errors):
    pending = list(jobs)
    all_rows = []

    for _ in range(REPORT_POLL_ROUNDS):
        if not pending:
            break
        still_pending = []
        for job in pending:
            try:
                state, download_url = check_report_status(job)
            except Exception as exc:
                errors.append(f"{job['profile_key']}/{job['ads_profile'].get('profileId')}: {exc}")
                continue
            if state == "pending":
                still_pending.append(job)
                continue
            try:
                rows = download_report_rows(download_url)
                all_rows.extend(campaign_row_to_body(job["ads_profile"], row) for row in rows)
            except Exception as exc:
                errors.append(f"{job['profile_key']}/{job['ads_profile'].get('profileId')}: download failed: {exc}")
        pending = still_pending
        if pending:
            time.sleep(REPORT_POLL_DELAY_SECONDS)

    for job in pending:
        errors.append(
            f"{job['profile_key']}/{job['ads_profile'].get('profileId')} "
            f"({job['ads_profile'].get('countryCode')}): report did not complete after {REPORT_POLL_ROUNDS} polls"
        )

    return all_rows


def pull_and_store_campaign_stats(start_date, end_date):
    pb_token = pb_authenticate()
    connections = pb_list_connected(pb_token)

    errors = []
    jobs = submit_report_jobs(connections, start_date, end_date, errors)
    all_rows = poll_jobs_until_done(jobs, errors)

    profile_ids = {row["profile_id"] for row in all_rows}
    ops = []
    for profile_id in profile_ids:
        existing_ids = pb_list_stats_ids(pb_token, profile_id, start_date, end_date)
        ops.extend(
            {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ADS_STATS_COLLECTION}/records/{rid}"}
            for rid in existing_ids
        )
    ops.extend(
        {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_STATS_COLLECTION}/records", "body": row}
        for row in all_rows
    )

    for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
        pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])

    return {"rowsWritten": len(all_rows), "profilesPulled": len(profile_ids), "errors": errors}


def UpdateAdsCampaignStats(request):
    if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
        return json_response({"error": "Unauthorized"}, 401)

    now_la = datetime.now(LA_TZ)
    default_date = (now_la - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = request.args.get("start_date", default_date)
    end_date = request.args.get("end_date", start_date)

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

    month = request.args.get("month", type=int) if hasattr(request, "args") else None
    year = request.args.get("year", type=int) if hasattr(request, "args") else None
    country_code = request.args.get("country_code") if hasattr(request, "args") else None
    if not month or not year:
        now_la = datetime.now(LA_TZ)
        month, year = month or now_la.month, year or now_la.year

    try:
        token = pb_authenticate()
        filter_str = f"(month = {month} && year = {year})"
        if country_code:
            filter_str += f' && country_code = "{country_code}"'

        campaigns = {}
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
                bucket = campaigns.setdefault(key, {
                    "campaignId": item.get("campaign_id"),
                    "campaignName": item.get("campaign_name", ""),
                    "campaignStatus": item.get("campaign_status", ""),
                    "countryCode": item.get("country_code", ""),
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

        rows = sorted(campaigns.values(), key=lambda c: -c["spend"])
        for row in rows:
            row["acos"] = (row["spend"] / row["sales"] * 100) if row["sales"] else 0

        return json_response({"month": month, "year": year, "campaigns": rows})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
