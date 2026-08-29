"""Real, live Amazon Ads bid-write path - added 2026-08-29 at the user's
explicit, repeated request ("add apply button to apply keyword change").
Single-target only, human-clicked, double-gated (ADMIN_KEY + confirm=yes) -
same safety posture as CreateMcfOrderForReceipt/DeleteAmazonListingItem
elsewhere in this project: a real, consequential, money-affecting write, but
scoped to one explicit item per call rather than any bulk/automatic path.

Amazon Ads API v3 SP keyword/target bid update endpoints and request shapes
are implemented per Amazon's documented v3 spec (PUT /sp/keywords,
PUT /sp/targets) but had NOT been exercised against this account before this
was written - unlike most other Amazon API integrations in this project,
which were built by probing the live API with deliberately-bad values first.
The first real Apply click is this feature's live test; if Amazon's response
shape doesn't match what's coded here, the error text returned should say so
clearly rather than mask it as generic failure.
"""
import json
import os
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
POCKETBASE_BID_LOG_COLLECTION = os.environ.get("POCKETBASE_BID_LOG_COLLECTION", "ads_bid_change_log")
# Deliberately its own narrow secret, not the shared ADMIN_KEY (which also
# gates MCF order placement, listing deletes, etc.) - this is the one write
# endpoint in the project meant to be called directly from the public
# frontend bundle (an Apply button), so a leaked key here should only ever
# be able to move one bid at a time, nothing else.
BID_APPLY_KEY = os.environ.get("BID_APPLY_KEY", "")
LA_TZ = ZoneInfo("America/Los_Angeles")


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["token"]


def pb_list_connected(token):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_COLLECTION}/records",
        headers={"Authorization": token},
        params={"filter": 'status = "connected"', "perPage": 200},
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("items", [])


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


def find_profile_connection(pb_token, profile_id):
    """Locate which ads_connections row (USA/EU) owns a given Ads profile id,
    and that profile's own region (for the right regional API host) - same
    connections/profiles shape AdsReporting.py's pull_and_store_campaign_lists
    already walks, just resolving a single known profile_id instead of
    iterating every profile."""
    for connection in pb_list_connected(pb_token):
        profile_key = connection.get("region")
        refresh_token = connection.get("refresh_token")
        if profile_key not in AD_PROFILES or not refresh_token:
            continue
        for ads_profile in connection.get("profiles", []) or []:
            if str(ads_profile.get("profileId")) == str(profile_id):
                region = ads_profile.get("region")
                base_url = ADS_REGION_ENDPOINTS.get(region)
                if not base_url:
                    continue
                return {
                    "profile_key": profile_key,
                    "refresh_token": refresh_token,
                    "client_id": AD_PROFILES[profile_key]["client_id"],
                    "base_url": base_url,
                }
    return None


def update_sp_keyword_bid(base_url, access_token, client_id, ads_profile_id, keyword_id, new_bid):
    response = requests.put(
        f"{base_url}/sp/keywords",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.spKeyword.v3+json",
            "Accept": "application/vnd.spKeyword.v3+json",
        },
        json={"keywords": [{"keywordId": str(keyword_id), "bid": new_bid}]},
        timeout=30,
    )
    return _check_sp_write_response(response, "keywords")


def update_sp_target_bid(base_url, access_token, client_id, ads_profile_id, target_id, new_bid):
    response = requests.put(
        f"{base_url}/sp/targets",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.spTargetingClause.v3+json",
            "Accept": "application/vnd.spTargetingClause.v3+json",
        },
        json={"targetingClauses": [{"targetId": str(target_id), "bid": new_bid}]},
        timeout=30,
    )
    return _check_sp_write_response(response, "targetingClauses")


def _check_sp_write_response(response, result_key):
    """A non-error HTTP status is not the same as a successful write - the
    same lesson learned the hard way with the Listings API elsewhere in this
    project (put_listings_item can return 200 with status: INVALID). Amazon's
    v3 keyword/target write responses carry a per-item success/error list
    under the collection's own key - treat anything other than a real
    success entry as a failure, and surface the raw body either way so a
    genuinely different response shape (this endpoint has never been
    exercised against this account before) is visible, not swallowed."""
    try:
        body = response.json()
    except ValueError:
        body = {"raw": response.text}
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code}: {body}")
    items = body.get(result_key, {})
    success = items.get("success") if isinstance(items, dict) else None
    error = items.get("error") if isinstance(items, dict) else None
    if error:
        raise RuntimeError(f"Amazon rejected the bid update: {error}")
    if not success:
        raise RuntimeError(f"Unexpected response shape (no success/error list found): {body}")
    return body


def record_bid_change(pb_token, body):
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_BID_LOG_COLLECTION}/records",
        headers={"Authorization": pb_token},
        json=body,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def last_bid_change_date(pb_token, target_id):
    """Most recent changed_at date for this target, or None if it's never
    been changed - used to skip proposing a new recommendation for a target
    that was already changed inside the current lookback window."""
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_BID_LOG_COLLECTION}/records",
        headers={"Authorization": pb_token},
        params={"filter": f'target_id = "{target_id}" && status = "applied"',
                "sort": "-changed_at", "perPage": 1, "fields": "changed_at"},
        timeout=30,
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    return items[0]["changed_at"] if items else None


def ApplyBidChange(request):
    """Applies ONE real bid change to Amazon for ONE target/keyword.
    Double-gated: ADMIN_KEY (query param `key`) + `confirm=yes`. Required
    params: target_id, campaign_id, campaign_name, target_text, ad_product,
    profile_id, country_code, old_bid, new_bid. Optional: reason (stored for
    the change-log display)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if BID_APPLY_KEY and (not hasattr(request, "args") or request.args.get("key") != BID_APPLY_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    args = request.args if hasattr(request, "args") else {}
    if args.get("confirm") != "yes":
        return json_response({"error": "Pass confirm=yes to actually apply this real bid change"}, 400)

    target_id = args.get("target_id")
    campaign_id = args.get("campaign_id")
    profile_id = args.get("profile_id")
    ad_product = (args.get("ad_product") or "").upper()
    target_type = args.get("target_type") or ""
    match_type = args.get("match_type") or ""
    try:
        old_bid = float(args.get("old_bid"))
        new_bid = float(args.get("new_bid"))
    except (TypeError, ValueError):
        return json_response({"error": "old_bid and new_bid must be numbers"}, 400)

    if not target_id or not campaign_id or not profile_id:
        return json_response({"error": "target_id, campaign_id, and profile_id are required"}, 400)

    log_body = {
        "target_id": target_id,
        "campaign_id": campaign_id,
        "campaign_name": args.get("campaign_name", ""),
        "target_text": args.get("target_text", ""),
        "ad_product": ad_product,
        "country_code": args.get("country_code", ""),
        "profile_id": profile_id,
        "old_bid": old_bid,
        "new_bid": new_bid,
        "changed_at": datetime.now(LA_TZ).strftime("%Y-%m-%d"),
        "reason": args.get("reason", ""),
    }

    try:
        if ad_product != "SPONSORED_PRODUCTS":
            raise RuntimeError(f"Live bid updates are only implemented for Sponsored Products so far (got {ad_product or 'unknown'})")

        pb_token = pb_authenticate()
        connection = find_profile_connection(pb_token, profile_id)
        if not connection:
            raise RuntimeError(f"No connected Ads account owns profile_id {profile_id}")

        access_token = refresh_access_token(connection["profile_key"], connection["refresh_token"])

        # A keyword row has a match_type (EXACT/PHRASE/BROAD); a product/auto
        # target row doesn't - same distinction ads_keyword_stats already
        # tracks per the AdsKeywordReporting.py note on SP's column split.
        if match_type:
            amazon_response = update_sp_keyword_bid(
                connection["base_url"], access_token, connection["client_id"], profile_id, target_id, new_bid
            )
        else:
            amazon_response = update_sp_target_bid(
                connection["base_url"], access_token, connection["client_id"], profile_id, target_id, new_bid
            )

        log_body["status"] = "applied"
        log_body["amazon_response"] = json.dumps(amazon_response)[:2000]
        record_bid_change(pb_token, log_body)
        return json_response({"applied": True, "targetId": target_id, "newBid": new_bid, "amazonResponse": amazon_response})
    except Exception as exc:
        try:
            log_body["status"] = "failed"
            log_body["amazon_response"] = str(exc)[:2000]
            record_bid_change(pb_authenticate(), log_body)
        except Exception:
            pass
        return json_response({"applied": False, "error": str(exc), "type": exc.__class__.__name__}, 500)
