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
POCKETBASE_ADS_KEYWORD_COLLECTION = os.environ.get("POCKETBASE_ADS_KEYWORD_COLLECTION", "ads_keyword_stats")
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


def update_sp_keyword_bid(base_url, access_token, client_id, ads_profile_id, keyword_id, new_bid, state=None):
    """state, when given, must be SP's uppercase form ("ENABLED"/"PAUSED") -
    confirmed from Amazon's own official Postman example body, which is a
    real, non-obvious detail: SB uses lowercase for the same concept (see
    update_sb_keyword_bid) - copy-pasting one case convention onto the
    other endpoint would silently fail or be rejected."""
    item = {"keywordId": str(keyword_id), "bid": new_bid}
    if state:
        item["state"] = state
    response = requests.put(
        f"{base_url}/sp/keywords",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.spKeyword.v3+json",
            "Accept": "application/vnd.spKeyword.v3+json",
        },
        json={"keywords": [item]},
        timeout=30,
    )
    return _check_sp_write_response(response, "keywords")


def update_sp_target_bid(base_url, access_token, client_id, ads_profile_id, target_id, new_bid, state=None):
    item = {"targetId": str(target_id), "bid": new_bid}
    if state:
        item["state"] = state
    response = requests.put(
        f"{base_url}/sp/targets",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.spTargetingClause.v3+json",
            "Accept": "application/vnd.spTargetingClause.v3+json",
        },
        json={"targetingClauses": [item]},
        timeout=30,
    )
    return _check_sp_write_response(response, "targetingClauses")


def update_sb_keyword_bid(base_url, access_token, client_id, ads_profile_id, keyword_id, campaign_id, ad_group_id, new_bid, state="enabled"):
    """SB's PUT /sb/keywords - confirmed exact endpoint/body/response shape
    2026-08-31 from Amazon's own official Postman collection
    (github.com/amzn/ads-advanced-tools-docs/postman), the same "check the
    real documented spec, don't guess" standard this project holds itself
    to elsewhere - not exercised against this account until the first real
    SB Apply click. Body is a flat JSON array (unlike SP's {"keywords": [...]}
    wrapper), and campaignId/adGroupId are both required alongside
    keywordId, not just the keyword id itself. state is lowercase for SB
    ("enabled"/"paused") - SP uses uppercase for the same concept, confirmed
    from Amazon's own separate official examples for each endpoint."""
    response = requests.put(
        f"{base_url}/sb/keywords",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/json",
        },
        json=[{
            "adGroupId": int(ad_group_id),
            "campaignId": int(campaign_id),
            "keywordId": int(keyword_id),
            "state": state,
            "bid": new_bid,
        }],
        timeout=30,
    )
    return _check_sb_keyword_write_response(response)


def update_sb_target_bid(base_url, access_token, client_id, ads_profile_id, target_id, campaign_id, ad_group_id, new_bid, state="enabled"):
    """SB's PUT /sb/targets - same official-Postman-collection source as
    update_sb_keyword_bid. Response shape is a `updateTargetSuccessResults`/
    `updateTargetErrorResults` pair, not a single per-item `code` field like
    the keyword endpoint - the two SB write endpoints don't share one
    response convention, confirmed from Amazon's own saved example
    responses rather than assumed to match."""
    response = requests.put(
        f"{base_url}/sb/targets",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        },
        json={"targets": [{
            "targetId": int(target_id),
            "adGroupId": int(ad_group_id),
            "campaignId": int(campaign_id),
            "state": state,
            "bid": new_bid,
        }]},
        timeout=30,
    )
    return _check_sb_target_write_response(response)


def _check_sb_keyword_write_response(response):
    try:
        body = response.json()
    except ValueError:
        body = {"raw": response.text}
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code}: {body}")
    if not isinstance(body, list) or not body:
        raise RuntimeError(f"Unexpected response shape (expected a non-empty list): {body}")
    if body[0].get("code") != "SUCCESS":
        raise RuntimeError(f"Amazon rejected the bid update: {body[0]}")
    return body


def _check_sb_target_write_response(response):
    try:
        body = response.json()
    except ValueError:
        body = {"raw": response.text}
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code}: {body}")
    errors = body.get("updateTargetErrorResults") or []
    successes = body.get("updateTargetSuccessResults") or []
    if errors:
        raise RuntimeError(f"Amazon rejected the bid update: {errors}")
    if not successes:
        raise RuntimeError(f"Unexpected response shape (no success/error list found): {body}")
    return body


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


def write_bid_or_state(connection, access_token, profile_id, ad_product, is_keyword, target_id, campaign_id, ad_group_id, bid, state=None):
    """Shared routing for the 4 real write endpoints (SP/SB x
    keyword/target), used by both ApplyBidChange (bid only) and
    DisableBidTarget (state=PAUSED/paused, same bid unchanged) - keeps the
    keyword-vs-target and SP-vs-SB branching in one place rather than
    duplicated per action."""
    if ad_product == "SPONSORED_PRODUCTS":
        sp_state = state.upper() if state else None
        if is_keyword:
            return update_sp_keyword_bid(connection["base_url"], access_token, connection["client_id"], profile_id, target_id, bid, state=sp_state)
        return update_sp_target_bid(connection["base_url"], access_token, connection["client_id"], profile_id, target_id, bid, state=sp_state)
    else:
        sb_state = state.lower() if state else "enabled"
        if is_keyword:
            return update_sb_keyword_bid(connection["base_url"], access_token, connection["client_id"], profile_id, target_id, campaign_id, ad_group_id, bid, state=sb_state)
        return update_sb_target_bid(connection["base_url"], access_token, connection["client_id"], profile_id, target_id, campaign_id, ad_group_id, bid, state=sb_state)


def ApplyBidChange(request):
    """Applies ONE real bid change to Amazon for ONE target/keyword.
    Double-gated: ADMIN_KEY (query param `key`) + `confirm=yes`. Required
    params: target_id, campaign_id, campaign_name, target_text, ad_product,
    profile_id, country_code, old_bid, new_bid. Optional: reason (stored for
    the change-log display). ad_group_id is required when ad_product is
    SPONSORED_BRANDS (SB's write endpoints need it in the body; SP's don't).
    Supports SPONSORED_PRODUCTS and SPONSORED_BRANDS as of 2026-08-31 -
    Sponsored Display still has no per-target bid to write (see the
    keywordBid schema-probe note elsewhere in this project: SD uses
    algorithmic bidding, not a settable bid at all)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if BID_APPLY_KEY and (not hasattr(request, "args") or request.args.get("key") != BID_APPLY_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    args = request.args if hasattr(request, "args") else {}
    if args.get("confirm") != "yes":
        return json_response({"error": "Pass confirm=yes to actually apply this real bid change"}, 400)

    target_id = args.get("target_id")
    campaign_id = args.get("campaign_id")
    ad_group_id = args.get("ad_group_id") or ""
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
        if ad_product not in ("SPONSORED_PRODUCTS", "SPONSORED_BRANDS"):
            raise RuntimeError(f"Live bid updates are only implemented for Sponsored Products and Sponsored Brands so far (got {ad_product or 'unknown'})")
        if ad_product == "SPONSORED_BRANDS" and not ad_group_id:
            raise RuntimeError("ad_group_id is required for Sponsored Brands bid updates")

        pb_token = pb_authenticate()
        connection = find_profile_connection(pb_token, profile_id)
        if not connection:
            raise RuntimeError(f"No connected Ads account owns profile_id {profile_id}")

        access_token = refresh_access_token(connection["profile_key"], connection["refresh_token"])

        # A keyword row's match_type is one of the real keyword match types
        # (EXACT/PHRASE/BROAD); a product/category/audience target row's
        # isn't - just checking match_type is truthy isn't enough, because
        # SB populates match_type with "TARGETING_EXPRESSION" even for a
        # non-keyword product/category target (confirmed live 2026-08-31:
        # a "category=..." target came back with match_type set to that
        # literal string, which a bare truthiness check would have
        # misrouted to the keyword endpoint instead of the target one).
        is_keyword = match_type in ("EXACT", "PHRASE", "BROAD")
        amazon_response = write_bid_or_state(
            connection, access_token, profile_id, ad_product, is_keyword,
            target_id, campaign_id, ad_group_id, new_bid
        )

        log_body["action"] = "bid_change"
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


def DisableBidTarget(request):
    """Pauses ONE keyword/target on Amazon (state=PAUSED/paused) instead of
    changing its bid - added 2026-08-31 at the user's explicit request for a
    "disable bid" option next to Apply, for a target performing badly enough
    that turning it off entirely is the right call rather than another bid
    cut. Same double-gating and per-product/per-type routing as
    ApplyBidChange (shares write_bid_or_state), current_bid is resubmitted
    unchanged alongside the state change since these write endpoints take a
    bid value regardless. Reversible - re-enabling is a normal Seller
    Central/Ads Console action, not something this endpoint does."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if BID_APPLY_KEY and (not hasattr(request, "args") or request.args.get("key") != BID_APPLY_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    args = request.args if hasattr(request, "args") else {}
    if args.get("confirm") != "yes":
        return json_response({"error": "Pass confirm=yes to actually pause this target on Amazon"}, 400)

    target_id = args.get("target_id")
    campaign_id = args.get("campaign_id")
    ad_group_id = args.get("ad_group_id") or ""
    profile_id = args.get("profile_id")
    ad_product = (args.get("ad_product") or "").upper()
    match_type = args.get("match_type") or ""
    try:
        current_bid = float(args.get("current_bid"))
    except (TypeError, ValueError):
        return json_response({"error": "current_bid must be a number"}, 400)

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
        "old_bid": current_bid,
        "new_bid": current_bid,
        "changed_at": datetime.now(LA_TZ).strftime("%Y-%m-%d"),
        "reason": args.get("reason", "Disabled from Bid Optimizer/Keywords page"),
        "action": "disable",
    }

    try:
        if ad_product not in ("SPONSORED_PRODUCTS", "SPONSORED_BRANDS"):
            raise RuntimeError(f"Live state updates are only implemented for Sponsored Products and Sponsored Brands so far (got {ad_product or 'unknown'})")
        if ad_product == "SPONSORED_BRANDS" and not ad_group_id:
            raise RuntimeError("ad_group_id is required for Sponsored Brands state updates")

        pb_token = pb_authenticate()
        connection = find_profile_connection(pb_token, profile_id)
        if not connection:
            raise RuntimeError(f"No connected Ads account owns profile_id {profile_id}")

        access_token = refresh_access_token(connection["profile_key"], connection["refresh_token"])
        is_keyword = match_type in ("EXACT", "PHRASE", "BROAD")
        amazon_response = write_bid_or_state(
            connection, access_token, profile_id, ad_product, is_keyword,
            target_id, campaign_id, ad_group_id, current_bid, state="PAUSED"
        )

        log_body["status"] = "applied"
        log_body["amazon_response"] = json.dumps(amazon_response)[:2000]
        record_bid_change(pb_token, log_body)
        return json_response({"disabled": True, "targetId": target_id, "amazonResponse": amazon_response})
    except Exception as exc:
        try:
            log_body["status"] = "failed"
            log_body["amazon_response"] = str(exc)[:2000]
            record_bid_change(pb_authenticate(), log_body)
        except Exception:
            pass
        return json_response({"disabled": False, "error": str(exc), "type": exc.__class__.__name__}, 500)


def _aggregate_window(token, target_id, start_date, end_date):
    """Sum spend/sales/clicks/orders for one target across [start_date,
    end_date] from ads_keyword_stats - used to build before/after windows
    around a bid change's changed_at date."""
    totals = {"spend": 0.0, "sales": 0.0, "clicks": 0, "orders": 0}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_KEYWORD_COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": f'target_id = "{target_id}" && date >= "{start_date}" && date <= "{end_date}"',
                    "fields": "spend,sales,clicks,orders", "perPage": 200, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            totals["spend"] += item.get("spend", 0) or 0
            totals["sales"] += item.get("sales", 0) or 0
            totals["clicks"] += item.get("clicks", 0) or 0
            totals["orders"] += item.get("orders", 0) or 0
        if page >= data.get("totalPages", 1):
            break
        page += 1
    totals["acos"] = (totals["spend"] / totals["sales"] * 100) if totals["sales"] else None
    return totals


def GetBidChangePerformance(request):
    """Read-only: for every applied bid change, shows how that keyword
    performed in an equal-length window before vs. after the change - added
    2026-08-29 at the user's request to track outcomes of every bid change
    made through the Apply button, not just log that it happened."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    window_days = int(request.args.get("window_days", 14)) if hasattr(request, "args") else 14

    try:
        token = pb_authenticate()
        changes = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_BID_LOG_COLLECTION}/records",
                headers={"Authorization": token},
                params={"filter": 'status = "applied"', "sort": "-changed_at", "perPage": 200, "page": page},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            changes.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1

        today = datetime.now(LA_TZ).strftime("%Y-%m-%d")
        results = []
        for change in changes:
            changed_at = change["changed_at"]
            changed_dt = datetime.strptime(changed_at, "%Y-%m-%d")
            pre_start = (changed_dt - timedelta(days=window_days)).strftime("%Y-%m-%d")
            pre_end = (changed_dt - timedelta(days=1)).strftime("%Y-%m-%d")
            post_start = changed_at
            post_end = min(today, (changed_dt + timedelta(days=window_days)).strftime("%Y-%m-%d"))

            before = _aggregate_window(token, change["target_id"], pre_start, pre_end)
            after = _aggregate_window(token, change["target_id"], post_start, post_end)

            results.append({
                "targetId": change["target_id"],
                "targetText": change.get("target_text", ""),
                "campaignName": change.get("campaign_name", ""),
                "countryCode": change.get("country_code", ""),
                "oldBid": change.get("old_bid"),
                "newBid": change.get("new_bid"),
                "changedAt": changed_at,
                "reason": change.get("reason", ""),
                "daysSinceChange": (datetime.strptime(today, "%Y-%m-%d") - changed_dt).days,
                "before": before,
                "after": after,
            })

        return json_response({"windowDays": window_days, "changes": results})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def find_sp_product_ad_id(base_url, access_token, client_id, ads_profile_id, campaign_id, ad_group_id, asin):
    """SP's POST /sp/productAds/list - resolves the real adId for one
    campaign/ad-group/ASIN, needed because ads_advertised_product_stats
    (built for reporting) never captured adId itself.

    CONFIRMED BUG (2026-09-01, found via a real pause going to the wrong
    product): asinFilter does NOT actually filter the response - a request
    with campaignIdFilter+adGroupIdFilter+asinFilter still returns every ad
    in that ad group (confirmed live: 19 ads back for a 1-ASIN filter,
    campaignId/adGroupId did correctly narrow the set, asin did not). The
    original code trusted ads[0], which silently paused an arbitrary
    (usually unrelated, already-alphabetically/id-first) ad instead of the
    one actually requested - it "succeeded" every time since Amazon has no
    way to know a wrong-but-valid adId was submitted. This must filter
    client-side by ASIN and never fall back to "just take the first one"."""
    response = requests.post(
        f"{base_url}/sp/productAds/list",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.spProductAd.v3+json",
            "Accept": "application/vnd.spProductAd.v3+json",
        },
        json={
            "campaignIdFilter": {"include": [str(campaign_id)]},
            "adGroupIdFilter": {"include": [str(ad_group_id)]},
            "asinFilter": {"include": [str(asin)]},
        },
        timeout=30,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Product ad lookup failed: HTTP {response.status_code}: {response.text}")
    body = response.json()
    ads = body.get("productAds") or []
    matches = [
        a for a in ads
        if str(a.get("asin")) == str(asin)
        and str(a.get("campaignId")) == str(campaign_id)
        and str(a.get("adGroupId")) == str(ad_group_id)
    ]
    if not matches:
        raise RuntimeError(
            f"No product ad found for campaign {campaign_id}, ad group {ad_group_id}, ASIN {asin} "
            f"(Amazon returned {len(ads)} ad(s) for this campaign/ad-group, none matching this ASIN - "
            f"asinFilter does not reliably filter, so this is checked client-side)"
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"Ambiguous: {len(matches)} product ads found for campaign {campaign_id}, ad group {ad_group_id}, "
            f"ASIN {asin} - refusing to guess which one to pause"
        )
    return matches[0]["adId"]


def update_sp_product_ad_state(base_url, access_token, client_id, ads_profile_id, ad_id, state):
    response = requests.put(
        f"{base_url}/sp/productAds",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(ads_profile_id),
            "Content-Type": "application/vnd.spProductAd.v3+json",
            "Accept": "application/vnd.spProductAd.v3+json",
            "Prefer": "return=representation",
        },
        json={"productAds": [{"adId": str(ad_id), "state": state}]},
        timeout=30,
    )
    try:
        body = response.json()
    except ValueError:
        body = {"raw": response.text}
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code}: {body}")
    result = body.get("productAds", {})
    if result.get("error"):
        raise RuntimeError(f"Amazon rejected the pause: {result['error']}")
    if not result.get("success"):
        raise RuntimeError(f"Unexpected response shape (no success/error list found): {body}")
    return body


def _set_product_ad_state(request, target_state, reason, action, response_key):
    """Shared body for PauseProductAd/EnableProductAd - both are the same
    lookup-then-write flow, differing only in the target state and the
    logged reason/action. Kept as two separate named endpoints (rather than
    one generic one) to match this project's existing pattern (ApplyBidChange
    vs DisableBidTarget) and so the frontend can call each explicitly."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if BID_APPLY_KEY and (not hasattr(request, "args") or request.args.get("key") != BID_APPLY_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    args = request.args if hasattr(request, "args") else {}
    if args.get("confirm") != "yes":
        return json_response({"error": f"Pass confirm=yes to actually {target_state.lower()} this product ad on Amazon"}, 400)

    campaign_id = args.get("campaign_id")
    ad_group_id = args.get("ad_group_id")
    asin = args.get("asin")
    profile_id = args.get("profile_id")
    ad_product = (args.get("ad_product") or "SPONSORED_PRODUCTS").upper()

    if not campaign_id or not ad_group_id or not asin or not profile_id:
        return json_response({"error": "campaign_id, ad_group_id, asin, and profile_id are required"}, 400)

    log_body = {
        "target_id": args.get("sku") or asin,
        "campaign_id": campaign_id,
        "campaign_name": args.get("campaign_name", ""),
        "target_text": asin,
        "ad_product": ad_product,
        "country_code": args.get("country_code", ""),
        "profile_id": profile_id,
        "old_bid": 0,
        "new_bid": 0,
        "changed_at": datetime.now(LA_TZ).strftime("%Y-%m-%d"),
        "reason": reason,
        "action": action,
    }

    try:
        if ad_product != "SPONSORED_PRODUCTS":
            raise RuntimeError(f"Product ad state changes are only implemented for Sponsored Products so far (got {ad_product})")

        pb_token = pb_authenticate()
        connection = find_profile_connection(pb_token, profile_id)
        if not connection:
            raise RuntimeError(f"No connected Ads account owns profile_id {profile_id}")

        access_token = refresh_access_token(connection["profile_key"], connection["refresh_token"])
        ad_id = find_sp_product_ad_id(
            connection["base_url"], access_token, connection["client_id"], profile_id, campaign_id, ad_group_id, asin
        )
        amazon_response = update_sp_product_ad_state(
            connection["base_url"], access_token, connection["client_id"], profile_id, ad_id, target_state
        )

        log_body["status"] = "applied"
        log_body["amazon_response"] = json.dumps(amazon_response)[:2000]
        record_bid_change(pb_token, log_body)
        return json_response({response_key: True, "adId": ad_id, "amazonResponse": amazon_response})
    except Exception as exc:
        try:
            log_body["status"] = "failed"
            log_body["amazon_response"] = str(exc)[:2000]
            record_bid_change(pb_authenticate(), log_body)
        except Exception:
            pass
        return json_response({response_key: False, "error": str(exc), "type": exc.__class__.__name__}, 500)


def PauseProductAd(request):
    """Pauses ONE advertised product (a SKU's product ad within one
    campaign/ad-group) - added 2026-09-01 at the user's request for a
    checkbox+bulk-pause option on the Advertised Products page. SP only for
    now (this page's own data is already SP+SD only; SD product-ad pause
    isn't implemented here yet). Same double-gating as every other real
    write in this project (ADMIN_KEY + confirm=yes) - the frontend loops
    this per selected row for a "pause all checked" action rather than a
    separate bulk endpoint, matching how Apply already works one row at a
    time."""
    return _set_product_ad_state(
        request, "PAUSED", "Paused from Advertised Products page", "pause_product_ad", "paused"
    )


def EnableProductAd(request):
    """Re-enables ONE advertised product ad - added 2026-09-01 alongside
    PauseProductAd at the user's request for a second bulk action (checked
    rows can be paused OR enabled), same lookup-then-write flow via
    _set_product_ad_state."""
    return _set_product_ad_state(
        request, "ENABLED", "Enabled from Advertised Products page", "enable_product_ad", "enabled"
    )
