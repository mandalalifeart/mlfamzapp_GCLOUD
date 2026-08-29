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

# --- Multi-period evaluation (added 2026-08-29 at the user's request to
# consider more than one window instead of a single lookback_days snapshot).
# Three periods, all ending at the same attribution-lag-adjusted end_date:
#   - RECENT (default 7d): a trend signal, not a decision driver on its own.
#   - lookback_days (default 30d, existing param): the actual decision
#     driver - same target-ACOS math as before, just now trend-adjusted.
#   - BASELINE (default 60d): a data-sufficiency gate, and a "is this a new
#     problem or a long-standing one" signal surfaced in the reason text.
DEFAULT_RECENT_DAYS = 7
DEFAULT_BASELINE_DAYS = 60
TREND_DAMPEN_PCT = 50                # when the recent period disagrees with
                                      # the decision period, apply only this
                                      # % of the otherwise-calculated change
TRENDING_GAP_PCT = 15                # baseline vs decision-period ACOS gap
                                      # (percentage points) big enough to
                                      # call out as "recent" vs "long-standing"


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


def compute_change_pct(row, target_acos, min_clicks, zero_sales_clicks, tolerance_pct, max_change_pct):
    """Pure decision math for one period's aggregated row - shared by the
    single- and multi-period paths. Returns (change_pct, kind, actual_acos)
    or None if there's not enough signal / no change warranted. `kind` is
    "zero_sales" or "acos", used to build the reason text. `actual_acos` is
    None for the zero_sales case (there's no meaningful ACOS to report)."""
    clicks = row["clicks"]
    sales = row["sales"]
    if clicks < min_clicks:
        return None
    if sales == 0 and clicks >= zero_sales_clicks:
        return (-ZERO_SALES_CUT_PCT, "zero_sales", None)
    if sales == 0:
        return None  # some clicks but below the zero-sales-cut threshold - leave alone

    actual_acos = row["spend"] / sales * 100
    deviation_pct = (actual_acos - target_acos) / target_acos * 100
    if abs(deviation_pct) <= tolerance_pct:
        return None  # within tolerance band - no change

    # Efficient (low ACOS) -> raise bid; inefficient (high ACOS) -> lower bid.
    # Move proportionally to how far off target we are, capped at max_change_pct.
    raw_change_pct = -deviation_pct  # negative deviation (ACOS below target) -> positive change (raise bid)
    change_pct = max(-max_change_pct, min(max_change_pct, raw_change_pct))
    return (change_pct, "acos", actual_acos)


def propose_bid_change(row, target_acos, min_clicks, zero_sales_clicks, tolerance_pct, max_change_pct, min_bid, max_bid):
    """Single-period version - still used when multi-period data isn't
    available. See propose_bid_change_multi_period for the 3-window version."""
    current_bid = row.get("bid")
    if current_bid is None:
        return None  # no known current bid to adjust from - skip (SD has no per-target bid at all, see CLAUDE.md)

    decision = compute_change_pct(row, target_acos, min_clicks, zero_sales_clicks, tolerance_pct, max_change_pct)
    if not decision:
        return None
    change_pct, kind, actual_acos = decision

    new_bid = round(current_bid * (1 + change_pct / 100), 2)
    new_bid = max(min_bid, min(max_bid, new_bid))
    if new_bid == current_bid:
        return None

    if kind == "zero_sales":
        reason = f"{row['clicks']} clicks, $0 sales - cutting bid {abs(change_pct):.0f}%"
    else:
        direction = "raising" if new_bid > current_bid else "lowering"
        reason = f"ACOS {actual_acos:.1f}% vs target {target_acos:.1f}% - {direction} bid {abs(change_pct):.0f}%"

    return {
        **row,
        "actualAcos": round(actual_acos, 1) if actual_acos is not None else None,
        "reason": reason,
        "currentBid": current_bid,
        "proposedBid": new_bid,
    }


def propose_bid_change_multi_period(row30, row7, row60, target_acos, min_clicks, zero_sales_clicks,
                                     tolerance_pct, max_change_pct, min_bid, max_bid):
    """3-window version, added 2026-08-29 at the user's request ("suggest a
    more complex bid recommendation mechanism... 3-4 periods, not only
    one"). row30/row7/row60 are the same target's aggregates over three
    trailing windows all ending at the same attribution-lag-adjusted date:
      - row60 (~60d, BASELINE): a data-sufficiency gate - if even 60 days of
        history doesn't reach min_clicks, there's simply not enough signal.
        Also used as a "is this new or long-standing" comparison point.
      - row30 (~30d, the existing lookback_days default, DECISION): drives
        the actual bid math via the same target-ACOS logic as
        propose_bid_change - unless row30 itself is below min_clicks, in
        which case row60 is used as the decision row instead (noted in the
        reason text).
      - row7 (~7d, RECENT): a trend signal only, never a decision driver on
        its own. If it agrees with the decision row's direction, the full
        calculated change applies. If it disagrees (already trending the
        other way), the change is damped to TREND_DAMPEN_PCT of its
        calculated size - don't overcorrect on top of a shift that may have
        already resolved itself."""
    current_bid = row30.get("bid") or row60.get("bid")
    if current_bid is None:
        return None

    if row60["clicks"] < min_clicks:
        return None  # not enough signal even at the longest window

    decision_row, decision_label = (row30, "30d") if row30["clicks"] >= min_clicks else (row60, "60d")
    decision = compute_change_pct(decision_row, target_acos, min_clicks, zero_sales_clicks, tolerance_pct, max_change_pct)
    if not decision:
        return None
    change_pct, kind, actual_acos = decision

    notes = []
    if kind == "acos":
        # Trend check against the recent window - only meaningful once it
        # has enough of its own signal.
        recent_min_clicks = max(3, min_clicks // 3)
        if row7["clicks"] >= recent_min_clicks and row7["sales"] > 0:
            recent_acos = row7["spend"] / row7["sales"] * 100
            recent_deviation = recent_acos - target_acos
            decision_deviation = actual_acos - target_acos
            agrees = (recent_deviation > 0) == (decision_deviation > 0)
            if not agrees:
                change_pct = change_pct * TREND_DAMPEN_PCT / 100
                shift = "improving" if abs(recent_deviation) < abs(decision_deviation) else "reversing"
                notes.append(f"7d trend {shift} ({recent_acos:.1f}%) - dampened")

        # "New problem or long-standing" signal vs the baseline window.
        if decision_label == "30d" and row60["clicks"] >= min_clicks and row60["sales"] > 0:
            baseline_acos = row60["spend"] / row60["sales"] * 100
            if abs(actual_acos - baseline_acos) >= TRENDING_GAP_PCT:
                notes.append(f"vs 60d baseline {baseline_acos:.1f}% - recent shift")

    new_bid = round(current_bid * (1 + change_pct / 100), 2)
    new_bid = max(min_bid, min(max_bid, new_bid))
    if new_bid == current_bid:
        return None

    note_suffix = f" ({'; '.join(notes)})" if notes else ""
    if kind == "zero_sales":
        reason = f"{decision_row['clicks']} clicks ({decision_label}), $0 sales - cutting bid {abs(change_pct):.0f}%{note_suffix}"
    else:
        direction = "raising" if new_bid > current_bid else "lowering"
        label_note = "" if decision_label == "30d" else " (30d had too few clicks, used 60d)"
        reason = (f"ACOS {actual_acos:.1f}% vs target {target_acos:.1f}% ({decision_label}){label_note} - "
                  f"{direction} bid {abs(change_pct):.0f}%{note_suffix}")

    return {
        **decision_row,
        "actualAcos": round(actual_acos, 1) if actual_acos is not None else None,
        "reason": reason,
        "currentBid": current_bid,
        "proposedBid": new_bid,
    }


def RunBidOptimizerDryRun(request):
    """Read-only: computes proposed bid changes from historical
    ads_keyword_stats and returns them - does NOT write anything to Amazon.
    Query params (all optional): target_acos, lookback_days, attribution_lag_days,
    min_clicks, zero_sales_clicks, tolerance_pct, max_change_pct, min_bid,
    max_bid, country_code, portfolio, recent_days, baseline_days,
    multi_period (default true; pass "false" to fall back to the original
    single-window evaluation - see propose_bid_change_multi_period for the
    3-window algorithm)."""
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
    recent_days = get_int("recent_days", DEFAULT_RECENT_DAYS)
    baseline_days = get_int("baseline_days", DEFAULT_BASELINE_DAYS)
    multi_period = (args.get("multi_period") if hasattr(args, "get") else None) != "false"
    country_code = args.get("country_code") if hasattr(args, "get") else None
    portfolio = args.get("portfolio") if hasattr(args, "get") else None

    now_la = datetime.now(LA_TZ)
    end_date = (now_la - timedelta(days=attribution_lag_days)).strftime("%Y-%m-%d")
    start_date = (now_la - timedelta(days=attribution_lag_days + lookback_days)).strftime("%Y-%m-%d")

    try:
        token = pb_authenticate()
        campaign_to_portfolio = fetch_campaign_to_portfolio_name(token)

        def with_portfolio(rows):
            for row in rows:
                row["portfolioName"] = campaign_to_portfolio.get(row.get("campaignId"), "")
            if portfolio:
                rows = [r for r in rows if r["portfolioName"] == portfolio]
            return rows

        # Don't re-recommend a target whose bid was already changed inside
        # the current lookback window - not enough fresh data has
        # accumulated yet to fairly judge the change that was just made.
        recently_changed_ids = fetch_recently_changed_target_ids(token, start_date)

        proposals = []
        skipped_recently_changed = 0

        if multi_period:
            recent_start = (now_la - timedelta(days=attribution_lag_days + recent_days)).strftime("%Y-%m-%d")
            baseline_start = (now_la - timedelta(days=attribution_lag_days + baseline_days)).strftime("%Y-%m-%d")

            rows30 = with_portfolio(fetch_keyword_aggregates(token, start_date, end_date, country_code))
            rows7 = {r["targetId"]: r for r in fetch_keyword_aggregates(token, recent_start, end_date, country_code)}
            rows60 = {r["targetId"]: r for r in fetch_keyword_aggregates(token, baseline_start, end_date, country_code)}

            targets_evaluated = len(rows60) if rows60 else len(rows30)
            for row30 in rows30:
                target_id = row30["targetId"]
                row60 = rows60.get(target_id)
                if not row60:
                    continue  # not present at all in the baseline window - no signal
                row7 = rows7.get(target_id, {**row30, "clicks": 0, "spend": 0.0, "sales": 0.0})
                proposal = propose_bid_change_multi_period(
                    row30, row7, row60, target_acos, min_clicks, zero_sales_clicks,
                    tolerance_pct, max_change_pct, min_bid, max_bid
                )
                if not proposal:
                    continue
                if target_id in recently_changed_ids:
                    skipped_recently_changed += 1
                    continue
                proposals.append(proposal)
        else:
            rows30 = with_portfolio(fetch_keyword_aggregates(token, start_date, end_date, country_code))
            targets_evaluated = len(rows30)
            for row in rows30:
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
            "multiPeriod": multi_period,
            "startDate": start_date,
            "endDate": end_date,
            "rules": {
                "targetAcos": target_acos,
                "lookbackDays": lookback_days,
                "attributionLagDays": attribution_lag_days,
                "recentDays": recent_days,
                "baselineDays": baseline_days,
                "minClicks": min_clicks,
                "zeroSalesClicks": zero_sales_clicks,
                "tolerancePct": tolerance_pct,
                "maxChangePct": max_change_pct,
                "minBid": min_bid,
                "maxBid": max_bid,
                "portfolio": portfolio or "",
            },
            "portfolios": sorted({p for p in campaign_to_portfolio.values() if p}),
            "targetsEvaluated": targets_evaluated,
            "proposalsCount": len(proposals),
            "skippedRecentlyChanged": skipped_recently_changed,
            "proposals": proposals,
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
