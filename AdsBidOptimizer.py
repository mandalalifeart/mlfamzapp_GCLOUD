"""Target-ACOS bid optimizer - dry-run only, no live Amazon writes yet.
Computes proposed bid changes per keyword/target from historical
ads_keyword_stats and returns/reports them for review. See CLAUDE.md for the
algorithm write-up and the safety posture (dry-run first, matching the
MCF-order-placement precedent in this project for real-money-affecting
actions)."""
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from AdsBidWriter import POCKETBASE_BID_LOG_COLLECTION

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_ADS_KEYWORD_COLLECTION = os.environ.get("POCKETBASE_ADS_KEYWORD_COLLECTION", "ads_keyword_stats")
POCKETBASE_ADS_CAMPAIGNS_COLLECTION = os.environ.get("POCKETBASE_ADS_CAMPAIGNS_COLLECTION", "ads_campaigns")
POCKETBASE_ADS_PORTFOLIOS_COLLECTION = os.environ.get("POCKETBASE_ADS_PORTFOLIOS_COLLECTION", "ads_portfolios")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
LA_TZ = ZoneInfo("America/Los_Angeles")

# --- Tunable rules (all overridable via query params for experimentation) ---
DEFAULT_TARGET_ACOS = 30.0          # % - the "break-even-ish" ACOS to steer toward
DEFAULT_LOOKBACK_DAYS = 30          # how much history to evaluate per keyword
DEFAULT_ATTRIBUTION_LAG_DAYS = 7    # exclude the most recent N days - Amazon's own
                                     # attribution window means very recent clicks
                                     # may not have converted yet
DEFAULT_MIN_CLICKS = 15             # below this, there's not enough signal to adjust
DEFAULT_ZERO_SALES_CLICKS = 20      # this many clicks with $0 sales = cut hard
DEFAULT_TOLERANCE_PCT = 10          # within this % of target ACOS, leave bid alone
DEFAULT_MAX_CHANGE_PCT = 20         # cap on how much a single adjustment can move a bid
DEFAULT_MIN_BID = 0.10
DEFAULT_MAX_BID = 5.00
ZERO_SALES_CUT_PCT = 30             # bid cut applied when zero-sales-with-clicks fires


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["token"]


def fetch_keyword_aggregates(token, start_date, end_date, country_code=None):
    """Same aggregation shape as GetAdsKeywordStats (sum impressions/clicks/
    spend/sales/orders per target_id, keep the most recent bid) - kept as its
    own copy here rather than importing across files, since the two call
    sites' filtering needs (date range vs live "current" bid) are subtly
    different and likely to diverge further as bid-write logic gets added."""
    filter_str = f'(date >= "{start_date}" && date <= "{end_date}")'
    if country_code:
        filter_str += f' && country_code = "{country_code}"'

    targets = {}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_KEYWORD_COLLECTION}/records",
            headers={"Authorization": token},
            params={
                "filter": filter_str,
                "perPage": 500,
                "page": page,
                "fields": "profile_id,campaign_id,campaign_name,ad_group_id,target_id,"
                          "target_text,target_type,match_type,ad_product,country_code,"
                          "impressions,clicks,spend,sales,orders,bid,date",
            },
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            key = (item.get("profile_id"), item.get("campaign_id"), item.get("ad_group_id"), item.get("target_id"))
            bucket = targets.setdefault(key, {
                "profileId": item.get("profile_id"),
                "campaignId": item.get("campaign_id"),
                "campaignName": item.get("campaign_name", ""),
                "adGroupId": item.get("ad_group_id", ""),
                "targetId": item.get("target_id"),
                "targetText": item.get("target_text", ""),
                "targetType": item.get("target_type", ""),
                "matchType": item.get("match_type", ""),
                "adProduct": item.get("ad_product", ""),
                "countryCode": item.get("country_code", ""),
                "impressions": 0, "clicks": 0, "spend": 0.0, "sales": 0.0, "orders": 0,
                "bid": None, "_bidDate": "", "_nameDate": "",
            })
            bucket["impressions"] += item.get("impressions", 0)
            bucket["clicks"] += item.get("clicks", 0)
            bucket["spend"] += item.get("spend", 0) or 0
            bucket["sales"] += item.get("sales", 0) or 0
            bucket["orders"] += item.get("orders", 0)
            if item.get("bid") is not None and item.get("date", "") >= bucket["_bidDate"]:
                bucket["bid"] = item.get("bid")
                bucket["_bidDate"] = item.get("date", "")
            # campaignName can change mid-window if the campaign gets
            # renamed - show the name from the most recent day in range.
            if item.get("campaign_name") and item.get("date", "") >= bucket["_nameDate"]:
                bucket["campaignName"] = item.get("campaign_name")
                bucket["_nameDate"] = item.get("date", "")
        if page >= data.get("totalPages", 1):
            break
        page += 1

    for bucket in targets.values():
        bucket.pop("_bidDate", None)
        bucket.pop("_nameDate", None)
    return list(targets.values())


def fetch_campaign_to_portfolio_name(token):
    """campaign_id -> portfolio name (e.g. "Pareo"/"POUF"), joined from the
    ads_campaigns snapshot's portfolio_id through ads_portfolios' id->name -
    added 2026-08-29 for the bid optimizer's portfolio filter."""
    portfolio_names = {}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_PORTFOLIOS_COLLECTION}/records",
            headers={"Authorization": token},
            params={"perPage": 500, "page": page, "fields": "portfolio_id,name"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            portfolio_names[item.get("portfolio_id")] = item.get("name", "")
        if page >= data.get("totalPages", 1):
            break
        page += 1

    campaign_to_portfolio = {}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_CAMPAIGNS_COLLECTION}/records",
            headers={"Authorization": token},
            params={"perPage": 500, "page": page, "fields": "campaign_id,portfolio_id"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            portfolio_id = item.get("portfolio_id")
            if portfolio_id:
                campaign_to_portfolio[item.get("campaign_id")] = portfolio_names.get(portfolio_id, "")
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return campaign_to_portfolio


def fetch_recently_changed_target_ids(token, since_date):
    """target_ids with an applied bid change on/after since_date - a single
    bulk query, not one per proposal, so the recently-changed check stays
    cheap regardless of how many proposals come out of a run."""
    ids = set()
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_BID_LOG_COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": f'status = "applied" && changed_at >= "{since_date}"',
                    "fields": "target_id", "perPage": 500, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        ids.update(item["target_id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


def propose_bid_change(row, target_acos, min_clicks, zero_sales_clicks, tolerance_pct, max_change_pct, min_bid, max_bid):
    """Returns a proposal dict if this target's bid should change, else None."""
    current_bid = row.get("bid")
    if current_bid is None:
        return None  # no known current bid to adjust from - skip (SD has no per-target bid at all, see CLAUDE.md)

    clicks = row["clicks"]
    spend = row["spend"]
    sales = row["sales"]

    if clicks < min_clicks:
        return None  # not enough signal yet

    if sales == 0 and clicks >= zero_sales_clicks:
        new_bid = round(current_bid * (1 - ZERO_SALES_CUT_PCT / 100), 2)
        new_bid = max(min_bid, min(max_bid, new_bid))
        if new_bid == current_bid:
            return None
        return {
            **row,
            "actualAcos": None,
            "reason": f"{clicks} clicks, $0 sales - cutting bid {ZERO_SALES_CUT_PCT}%",
            "currentBid": current_bid,
            "proposedBid": new_bid,
        }

    if sales == 0:
        return None  # some clicks but below the zero-sales-cut threshold - leave alone

    actual_acos = spend / sales * 100
    deviation_pct = (actual_acos - target_acos) / target_acos * 100
    if abs(deviation_pct) <= tolerance_pct:
        return None  # within tolerance band - no change

    # Efficient (low ACOS) -> raise bid; inefficient (high ACOS) -> lower bid.
    # Move proportionally to how far off target we are, capped at max_change_pct.
    raw_change_pct = -deviation_pct  # negative deviation (ACOS below target) -> positive change (raise bid)
    change_pct = max(-max_change_pct, min(max_change_pct, raw_change_pct))
    new_bid = round(current_bid * (1 + change_pct / 100), 2)
    new_bid = max(min_bid, min(max_bid, new_bid))
    if new_bid == current_bid:
        return None

    direction = "raising" if new_bid > current_bid else "lowering"
    return {
        **row,
        "actualAcos": round(actual_acos, 1),
        "reason": f"ACOS {actual_acos:.1f}% vs target {target_acos:.1f}% - {direction} bid {abs(change_pct):.0f}%",
        "currentBid": current_bid,
        "proposedBid": new_bid,
    }


def RunBidOptimizerDryRun(request):
    """Read-only: computes proposed bid changes from historical
    ads_keyword_stats and returns them - does NOT write anything to Amazon.
    Query params (all optional): target_acos, lookback_days, attribution_lag_days,
    min_clicks, zero_sales_clicks, tolerance_pct, max_change_pct, min_bid,
    max_bid, country_code."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    args = request.args if hasattr(request, "args") else {}

    def get_float(name, default):
        try:
            return float(args.get(name, default))
        except (TypeError, ValueError):
            return default

    def get_int(name, default):
        try:
            return int(args.get(name, default))
        except (TypeError, ValueError):
            return default

    target_acos = get_float("target_acos", DEFAULT_TARGET_ACOS)
    lookback_days = get_int("lookback_days", DEFAULT_LOOKBACK_DAYS)
    attribution_lag_days = get_int("attribution_lag_days", DEFAULT_ATTRIBUTION_LAG_DAYS)
    min_clicks = get_int("min_clicks", DEFAULT_MIN_CLICKS)
    zero_sales_clicks = get_int("zero_sales_clicks", DEFAULT_ZERO_SALES_CLICKS)
    tolerance_pct = get_float("tolerance_pct", DEFAULT_TOLERANCE_PCT)
    max_change_pct = get_float("max_change_pct", DEFAULT_MAX_CHANGE_PCT)
    min_bid = get_float("min_bid", DEFAULT_MIN_BID)
    max_bid = get_float("max_bid", DEFAULT_MAX_BID)
    country_code = args.get("country_code") if hasattr(args, "get") else None
    portfolio = args.get("portfolio") if hasattr(args, "get") else None

    now_la = datetime.now(LA_TZ)
    end_date = (now_la - timedelta(days=attribution_lag_days)).strftime("%Y-%m-%d")
    start_date = (now_la - timedelta(days=attribution_lag_days + lookback_days)).strftime("%Y-%m-%d")

    try:
        token = pb_authenticate()
        rows = fetch_keyword_aggregates(token, start_date, end_date, country_code)
        campaign_to_portfolio = fetch_campaign_to_portfolio_name(token)
        for row in rows:
            row["portfolioName"] = campaign_to_portfolio.get(row.get("campaignId"), "")
        if portfolio:
            rows = [r for r in rows if r["portfolioName"] == portfolio]

        # Don't re-recommend a target whose bid was already changed inside
        # the current lookback window - not enough fresh data has
        # accumulated yet to fairly judge the change that was just made.
        recently_changed_ids = fetch_recently_changed_target_ids(token, start_date)

        proposals = []
        skipped_recently_changed = 0
        for row in rows:
            proposal = propose_bid_change(
                row, target_acos, min_clicks, zero_sales_clicks, tolerance_pct, max_change_pct, min_bid, max_bid
            )
            if not proposal:
                continue
            if row["targetId"] in recently_changed_ids:
                skipped_recently_changed += 1
                continue
            proposals.append(proposal)

        proposals.sort(key=lambda p: -p["spend"])

        return json_response({
            "dryRun": True,
            "startDate": start_date,
            "endDate": end_date,
            "rules": {
                "targetAcos": target_acos,
                "lookbackDays": lookback_days,
                "attributionLagDays": attribution_lag_days,
                "minClicks": min_clicks,
                "zeroSalesClicks": zero_sales_clicks,
                "tolerancePct": tolerance_pct,
                "maxChangePct": max_change_pct,
                "minBid": min_bid,
                "maxBid": max_bid,
                "portfolio": portfolio or "",
            },
            "portfolios": sorted({p for p in campaign_to_portfolio.values() if p}),
            "targetsEvaluated": len(rows),
            "proposalsCount": len(proposals),
            "skippedRecentlyChanged": skipped_recently_changed,
            "proposals": proposals,
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
