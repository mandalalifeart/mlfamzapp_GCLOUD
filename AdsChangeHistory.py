import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

from AdsAuth import AD_PROFILES, ADS_REGION_ENDPOINTS, cors_headers, json_response
from AdsReporting import (
    ADMIN_KEY,
    POCKETBASE_BATCH_SIZE,
    POCKETBASE_URL,
    pb_authenticate,
    pb_batch,
    pb_list_connected,
    refresh_access_token,
)

SYSTEM_TZ = ZoneInfo("Asia/Jerusalem")  # matches this machine's local cron timezone, not Amazon's
POCKETBASE_ADS_CHANGE_HISTORY_COLLECTION = os.environ.get("POCKETBASE_ADS_CHANGE_HISTORY_COLLECTION", "ads_change_history")
POCKETBASE_ADS_CAMPAIGNS_COLLECTION = os.environ.get("POCKETBASE_ADS_CAMPAIGNS_COLLECTION", "ads_campaigns")

# Amazon's real Change History API (POST /history) only accepts a curated
# subset of change types as request-side `filters` per entity type - e.g.
# "PORTFOLIO" is a valid changeType in Amazon's own response schema but is
# rejected as an invalid filter value on the request side (confirmed live
# 2026-09-06). Scoped to exactly what was asked for: campaign budget/
# status/name, and ad group default bid. Amazon's response schema also
# supports BID_AMOUNT changes for individual KEYWORD/PRODUCT_TARGETING
# entities (not exposed as a request-side filter for those types, but
# returned unfiltered) - not pulled here since our own applied keyword bid
# changes are already tracked in ads_bid_change_log; add "KEYWORD"/
# "PRODUCT_TARGETING" keys below (parents: [{"useProfileIdAdvertiser": True}])
# to also catch manual keyword/target bid edits made directly in Seller
# Central.
EVENT_TYPES_QUERY = {
    "CAMPAIGN": {"filters": ["STATUS", "BUDGET_AMOUNT", "NAME"], "parents": [{"useProfileIdAdvertiser": True}]},
    "AD_GROUP": {"filters": ["DEFAULT_BID_AMOUNT"], "parents": [{"useProfileIdAdvertiser": True}]},
}

# Amazon's real 90-day retention rolls forward from "now" like the other Ads
# reporting endpoints - a day of headroom avoids a boundary 400 the way a
# literal 90*86400*1000 would risk. fromDate/toDate are UTC epoch
# MILLISECONDS (confirmed live 2026-09-06 via a real 400 error naming the
# exact ms cutoff) - every date param elsewhere in this codebase's Ads
# integrations is a YYYY-MM-DD string, so this is an easy unit mistake to
# repeat.
MAX_HISTORY_LOOKBACK_MS = 89 * 86400 * 1000


def fetch_campaign_names(pb_token):
    names = {}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_CAMPAIGNS_COLLECTION}/records",
            headers={"Authorization": pb_token},
            params={"perPage": 500, "page": page, "fields": "campaign_id,campaign_name"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            names[str(item["campaign_id"])] = item.get("campaign_name", "")
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return names


def last_recorded_change_ms(pb_token):
    """Most recent `changed_at` already stored, in epoch ms, or None if the
    collection is empty - lets a scheduled pull catch up automatically after
    a missed run instead of only ever covering a fixed lookback window (same
    intent as AdsReporting.last_recorded_date, but keyed on `changed_at`
    rather than a plain `date` column since this collection needs
    time-of-day precision, not just a calendar day)."""
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_CHANGE_HISTORY_COLLECTION}/records",
        headers={"Authorization": pb_token},
        params={"perPage": 1, "sort": "-changed_at", "fields": "changed_at"},
        timeout=30,
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    if not items:
        return None
    dt = datetime.fromisoformat(items[0]["changed_at"])
    return int(dt.timestamp() * 1000)


def fetch_history_events(base_url, access_token, client_id, ads_profile_id, from_ms, to_ms):
    """Paginates POST /history via nextToken until exhausted."""
    events = []
    next_token = None
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": client_id,
        "Amazon-Advertising-API-Scope": str(ads_profile_id),
        "Content-Type": "application/json",
    }
    while True:
        body = {"fromDate": from_ms, "toDate": to_ms, "count": 200, "eventTypes": EVENT_TYPES_QUERY}
        if next_token:
            body["nextToken"] = next_token
        response = None
        for attempt in range(6):
            response = requests.post(f"{base_url}/history", headers=headers, json=body, timeout=30)
            if response.status_code != 429:
                break
            time.sleep(5 * (attempt + 1))
        if response.status_code != 200:
            raise RuntimeError(f"Change History request failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        page_events = data.get("events") or []
        events.extend(page_events)
        next_token = data.get("nextToken")
        if not next_token or not page_events:
            break
    return events


def event_to_body(ads_profile, event, campaign_names):
    entity_type = event.get("entityType", "")
    entity_id = str(event.get("entityId", ""))
    metadata = event.get("metadata") or {}
    campaign_id = entity_id if entity_type == "CAMPAIGN" else str(metadata.get("campaignId") or "")
    changed_at = datetime.fromtimestamp(event["timestamp"] / 1000, tz=timezone.utc)
    return {
        "profile_id": str(ads_profile["profileId"]),
        "country_code": ads_profile.get("countryCode", ""),
        "entity_type": entity_type,
        "entity_id": entity_id,
        "campaign_id": campaign_id,
        "campaign_name": campaign_names.get(campaign_id, "") if campaign_id else "",
        "change_type": event.get("changeType", ""),
        "previous_value": str(event.get("previousValue", "")),
        "new_value": str(event.get("newValue", "")),
        "changed_at": changed_at.isoformat(),
        "month": changed_at.month,
        "year": changed_at.year,
    }


def UpdateAdsChangeHistory(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
        return json_response({"error": "Unauthorized"}, 401)

    try:
        pb_token = pb_authenticate()
        connections = pb_list_connected(pb_token)
        campaign_names = fetch_campaign_names(pb_token)

        now_ms = int(time.time() * 1000)
        last_ms = last_recorded_change_ms(pb_token)
        from_ms = max(last_ms + 1, now_ms - MAX_HISTORY_LOOKBACK_MS) if last_ms else now_ms - MAX_HISTORY_LOOKBACK_MS

        errors = []
        rows_written = 0
        profiles_pulled = 0

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
                base_url = ADS_REGION_ENDPOINTS.get(ads_profile.get("region"))
                ads_profile_id = ads_profile.get("profileId")
                if not base_url or not ads_profile_id:
                    continue
                try:
                    events = fetch_history_events(base_url, access_token, client_id, ads_profile_id, from_ms, now_ms)
                    profiles_pulled += 1
                    bodies = [event_to_body(ads_profile, event, campaign_names) for event in events]
                    ops = [
                        {"method": "POST", "url": f"/api/collections/{POCKETBASE_ADS_CHANGE_HISTORY_COLLECTION}/records", "body": b}
                        for b in bodies
                    ]
                    for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
                        pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])
                    rows_written += len(bodies)
                except Exception as exc:
                    errors.append(f"{profile_key}/{ads_profile_id} ({ads_profile.get('countryCode')}): {exc}")

        return json_response({
            "rowsWritten": rows_written,
            "profilesPulled": profiles_pulled,
            "fromDate": datetime.fromtimestamp(from_ms / 1000, tz=timezone.utc).isoformat(),
            "toDate": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
            "errors": errors,
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetAdsChangeHistory(request):
    """Read-only: stored change history rows, optionally filtered by
    country_code/campaign_id/change_type/month/year. No Amazon calls."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    country_code = request.args.get("country_code")
    campaign_id = request.args.get("campaign_id")
    change_type = request.args.get("change_type")
    month = request.args.get("month")
    year = request.args.get("year")

    filters = []
    if country_code:
        filters.append(f'country_code = "{country_code}"')
    if campaign_id:
        filters.append(f'campaign_id = "{campaign_id}"')
    if change_type:
        filters.append(f'change_type = "{change_type}"')
    if month:
        filters.append(f"month = {int(month)}")
    if year:
        filters.append(f"year = {int(year)}")

    try:
        pb_token = pb_authenticate()
        items = []
        page = 1
        while True:
            params = {"perPage": 500, "page": page, "sort": "-changed_at"}
            if filters:
                params["filter"] = " && ".join(filters)
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_CHANGE_HISTORY_COLLECTION}/records",
                headers={"Authorization": pb_token},
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            items.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1
        return json_response({"events": items})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
