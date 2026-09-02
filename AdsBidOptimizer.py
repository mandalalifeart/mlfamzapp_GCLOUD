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
DEFAULT_MIN_SPEND = 5.00            # $ spent, below this there's not enough signal to
                                     # adjust - a money threshold, not a click count, same
                                     # reasoning as DEFAULT_ZERO_SALES_SPEND above
DEFAULT_ZERO_SALES_SPEND = 3.00     # $ spent with $0 sales = cut hard - a money threshold,
                                     # not a click count, since 20 clicks at $0.10 ($2) is a
                                     # very different risk than 20 clicks at $1 ($20) - the
                                     # user's own point, corrected 2026-08-29 from the
                                     # original click-count version of this rule
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


def fetch_keyword_raw_rows(token, start_date, end_date, country_code=None):
    """Fetches un-aggregated per-day rows (kept as their own copy rather
    than importing across files - see aggregate_keyword_rows below).
    Split out from the aggregation step 2026-08-29: the multi-period
    algorithm needs the same underlying rows sliced into 3 different
    sub-windows (7d/30d/60d) - fetching once over the widest window and
    aggregating in-memory 3 ways avoids 3 separate paginated network round
    trips, which is what caused RunBidOptimizerDryRun to start timing out
    (HTTP 504) the moment multi-period evaluation shipped."""
    filter_str = f'(date >= "{start_date}" && date <= "{end_date}")'
    if country_code:
        filter_str += f' && country_code = "{country_code}"'

    rows = []
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
            timeout=90,
        )
        response.raise_for_status()
        data = response.json()
        rows.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return rows


def aggregate_keyword_rows(raw_rows, start_date, end_date):
    """Same aggregation shape as GetAdsKeywordStats (sum impressions/clicks/
    spend/sales/orders per target_id, keep the most recent bid) - pure
    in-memory, filters raw_rows (already fetched) down to [start_date,
    end_date] first so the same fetch can back several different windows."""
    targets = {}
    for item in raw_rows:
        item_date = item.get("date", "")
        if item_date < start_date or item_date > end_date:
            continue
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
        if item.get("bid") is not None and item_date >= bucket["_bidDate"]:
            bucket["bid"] = item.get("bid")
            bucket["_bidDate"] = item_date
        # campaignName can change mid-window if the campaign gets renamed -
        # show the name from the most recent day in range.
        if item.get("campaign_name") and item_date >= bucket["_nameDate"]:
            bucket["campaignName"] = item.get("campaign_name")
            bucket["_nameDate"] = item_date

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


def fetch_last_change_per_target(token):
    """Most recent applied bid change per target_id, regardless of how long
    ago - a single bulk query (paginated, sorted oldest-first so later pages
    overwrite earlier ones and each key ends up holding its true most-recent
    row), not one per proposal. Added 2026-08-29 for the "consider bid
    change history" feature: unlike fetch_recently_changed_target_ids (which
    only looks inside the current lookback window and fully excludes a
    match), this looks at ALL history so a change from outside the window
    that already tried the same direction and didn't fix the ACOS can still
    inform (dampen) a new proposal rather than being invisible to it."""
    last_change = {}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_BID_LOG_COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": 'status = "applied"', "sort": "changed_at",
                    "fields": "target_id,old_bid,new_bid,changed_at", "perPage": 500, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            last_change[item["target_id"]] = item
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return last_change


def compute_change_pct(row, target_acos, min_spend, zero_sales_spend, tolerance_pct, max_change_pct):
    """Pure decision math for one period's aggregated row - shared by the
    single- and multi-period paths. Returns (change_pct, kind, actual_acos)
    or None if there's not enough signal / no change warranted. `kind` is
    "zero_sales" or "acos", used to build the reason text. `actual_acos` is
    None for the zero_sales case (there's no meaningful ACOS to report).
    Both signal-sufficiency and zero-sales-cut gates are $ spend, not click
    count - the user's own point (2026-08-29): 20 clicks at $0.10 is $2 of
    real risk, 20 clicks at $1 is $20 - a click count alone conflates very
    different amounts of money at stake."""
    sales = row["sales"]
    spend = row["spend"]
    if spend < min_spend:
        return None
    if sales == 0 and spend >= zero_sales_spend:
        return (-ZERO_SALES_CUT_PCT, "zero_sales", None)
    if sales == 0:
        return None  # some spend but below the zero-sales-cut threshold - leave alone

    actual_acos = spend / sales * 100
    deviation_pct = (actual_acos - target_acos) / target_acos * 100
    if abs(deviation_pct) <= tolerance_pct:
        return None  # within tolerance band - no change

    # Efficient (low ACOS) -> raise bid; inefficient (high ACOS) -> lower bid.
    # Move proportionally to how far off target we are, capped at max_change_pct.
    raw_change_pct = -deviation_pct  # negative deviation (ACOS below target) -> positive change (raise bid)
    change_pct = max(-max_change_pct, min(max_change_pct, raw_change_pct))
    return (change_pct, "acos", actual_acos)


def propose_bid_change(row, target_acos, min_spend, zero_sales_spend, tolerance_pct, max_change_pct, min_bid, max_bid):
    """Single-period version - still used when multi-period data isn't
    available. See propose_bid_change_multi_period for the 3-window version."""
    current_bid = row.get("bid")
    if current_bid is None:
        return None  # no known current bid to adjust from - skip (SD has no per-target bid at all, see CLAUDE.md)

    decision = compute_change_pct(row, target_acos, min_spend, zero_sales_spend, tolerance_pct, max_change_pct)
    if not decision:
        return None
    change_pct, kind, actual_acos = decision

    new_bid = round(current_bid * (1 + change_pct / 100), 2)
    new_bid = max(min_bid, min(max_bid, new_bid))
    if new_bid == current_bid:
        return None

    if kind == "zero_sales":
        reason = f"${row['spend']:.2f} spent ({row['clicks']} clicks), $0 sales - cutting bid {abs(change_pct):.0f}%"
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


def propose_bid_change_multi_period(row30, row7, row60, target_acos, min_spend, zero_sales_spend,
                                     tolerance_pct, max_change_pct, min_bid, max_bid, last_change=None):
    """3-window version, added 2026-08-29 at the user's request ("suggest a
    more complex bid recommendation mechanism... 3-4 periods, not only
    one"). row30/row7/row60 are the same target's aggregates over three
    trailing windows all ending at the same attribution-lag-adjusted date:
      - row60 (~60d, BASELINE): a data-sufficiency gate - if even 60 days of
        history doesn't reach min_spend, there's simply not enough signal.
        Also used as a "is this new or long-standing" comparison point.
      - row30 (~30d, the existing lookback_days default, DECISION): drives
        the actual bid math via the same target-ACOS logic as
        propose_bid_change - unless row30 itself is below min_spend, in
        which case row60 is used as the decision row instead (noted in the
        reason text).
      - row7 (~7d, RECENT): a trend signal only, never a decision driver on
        its own. If it agrees with the decision row's direction, the full
        calculated change applies. If it disagrees (already trending the
        other way), the change is damped to TREND_DAMPEN_PCT of its
        calculated size - don't overcorrect on top of a shift that may have
        already resolved itself.
    `last_change` (optional) is this target's most recent applied bid
    change, from anywhere in its history - added 2026-08-29 at the user's
    request to factor a keyword's own change history into new proposals
    (option "b": if a same-direction change already happened and ACOS still
    hasn't recovered, that's evidence the problem may not be bid-driven at
    all, so the new move is dampened with a note rather than just repeating
    the same lever at full size). Only a change already inside the current
    decision window matters here as "already tried and failing" in a
    meaningful sense - a change from within the window is instead fully
    excluded from getting any new proposal at all by the caller's separate
    recently-changed skip, so by the time a `last_change` reaches this
    function it can be trusted to be old enough for the ACOS data here to
    reflect its outcome."""
    current_bid = row30.get("bid") or row60.get("bid")
    if current_bid is None:
        return None

    if row60["spend"] < min_spend:
        return None  # not enough signal even at the longest window

    decision_row, decision_label = (row30, "30d") if row30["spend"] >= min_spend else (row60, "60d")
    decision = compute_change_pct(decision_row, target_acos, min_spend, zero_sales_spend, tolerance_pct, max_change_pct)
    if not decision:
        return None
    change_pct, kind, actual_acos = decision
    capped_decision_change_pct = change_pct  # before any trend/last-change dampening below

    calc_steps = []
    if kind == "acos":
        deviation_pct = (actual_acos - target_acos) / target_acos * 100
        raw_change_pct = -deviation_pct
        cap_note = f", capped at ±{max_change_pct:.0f}%" if abs(raw_change_pct) > max_change_pct else ""
        calc_steps.append(
            f"{decision_label} ACOS {actual_acos:.1f}% is {abs(deviation_pct):.0f}% "
            f"{'above' if deviation_pct > 0 else 'below'} the {target_acos:.0f}% target -> "
            f"raw change {raw_change_pct:+.1f}%{cap_note} -> {capped_decision_change_pct:+.1f}%"
        )
    else:
        calc_steps.append(f"{decision_label}: $0 sales on real spend -> flat {-change_pct:.0f}% cut, no ACOS math involved")

    notes = []
    if kind == "acos":
        # Trend check against the recent window - only meaningful once it
        # has enough of its own signal (spend, same money-based reasoning).
        recent_min_spend = max(0.50, min_spend / 3)
        if row7["spend"] >= recent_min_spend and row7["sales"] > 0:
            recent_acos = row7["spend"] / row7["sales"] * 100
            recent_deviation = recent_acos - target_acos
            decision_deviation = actual_acos - target_acos
            agrees = (recent_deviation > 0) == (decision_deviation > 0)
            if not agrees:
                change_pct = change_pct * TREND_DAMPEN_PCT / 100
                shift = "improving" if abs(recent_deviation) < abs(decision_deviation) else "reversing"
                notes.append(f"7d trend {shift} ({recent_acos:.1f}%) - dampened")
                calc_steps.append(
                    f"7d ACOS {recent_acos:.1f}% disagrees with {decision_label}'s direction ({shift}) -> "
                    f"dampened to {TREND_DAMPEN_PCT:.0f}% of that = {change_pct:+.1f}%"
                )

        # "New problem or long-standing" signal vs the baseline window.
        if decision_label == "30d" and row60["spend"] >= min_spend and row60["sales"] > 0:
            baseline_acos = row60["spend"] / row60["sales"] * 100
            if abs(actual_acos - baseline_acos) >= TRENDING_GAP_PCT:
                notes.append(f"vs 60d baseline {baseline_acos:.1f}% - recent shift")

        # Bid change history - a same-direction change already tried before
        # (and old enough that this window's ACOS reflects its outcome)
        # that still hasn't fixed the ACOS is evidence bid alone isn't the
        # lever that will fix this, so dampen rather than repeat it blindly.
        if last_change:
            prev_direction = "raised" if last_change["new_bid"] > last_change["old_bid"] else "lowered"
            new_direction = "raised" if change_pct > 0 else "lowered"
            if prev_direction == new_direction:
                change_pct = change_pct * TREND_DAMPEN_PCT / 100
                notes.append(
                    f"already {prev_direction} on {last_change['changed_at']} "
                    f"(${last_change['old_bid']:.2f}→${last_change['new_bid']:.2f}) - ACOS still off, dampened"
                )
                calc_steps.append(
                    f"already {prev_direction} on {last_change['changed_at']}, same direction again -> "
                    f"dampened to {TREND_DAMPEN_PCT:.0f}% of that = {change_pct:+.1f}%"
                )

    new_bid = round(current_bid * (1 + change_pct / 100), 2)
    new_bid = max(min_bid, min(max_bid, new_bid))
    if new_bid == current_bid:
        return None

    clamp_note = ""
    if round(current_bid * (1 + change_pct / 100), 2) != new_bid:
        clamp_note = f", clamped to bid range [${min_bid:.2f}, ${max_bid:.2f}]"
    calc_steps.append(
        f"final change {change_pct:+.1f}% -> ${current_bid:.2f} -> ${new_bid:.2f}{clamp_note}"
    )

    note_suffix = f" ({'; '.join(notes)})" if notes else ""
    if kind == "zero_sales":
        reason = f"${decision_row['spend']:.2f} spent ({decision_row['clicks']} clicks, {decision_label}), $0 sales - cutting bid {abs(change_pct):.0f}%{note_suffix}"
    else:
        direction = "raising" if new_bid > current_bid else "lowering"
        label_note = "" if decision_label == "30d" else " (30d had too few clicks, used 60d)"
        reason = (f"ACOS {actual_acos:.1f}% vs target {target_acos:.1f}% ({decision_label}){label_note} - "
                  f"{direction} bid {abs(change_pct):.0f}%{note_suffix}")

    def period_summary(row):
        """Each period's own raw contribution is purely informational (what
        THAT window alone would suggest, capped the same way the real
        decision is) - never a decision input itself except for whichever
        window was actually picked as decision_row above."""
        acos = round(row["spend"] / row["sales"] * 100, 1) if row["sales"] else None
        contribution = None
        if acos is not None:
            deviation = (acos - target_acos) / target_acos * 100
            suggested = max(-max_change_pct, min(max_change_pct, -deviation))
            contribution = {"deviationPct": round(deviation, 1), "suggestedChangePct": round(suggested, 1)}
        return {
            "spend": round(row["spend"], 2),
            "sales": round(row["sales"], 2),
            "clicks": row["clicks"],
            "cpc": round(row["spend"] / row["clicks"], 2) if row["clicks"] else None,
            "acos": acos,
            "contribution": contribution,
        }

    last_change_note = next((n for n in notes if n.startswith("already")), None)
    trend_note = next((n for n in notes if not n.startswith("already")), None)

    return {
        **decision_row,
        "actualAcos": round(actual_acos, 1) if actual_acos is not None else None,
        "reason": reason,
        "currentBid": current_bid,
        "proposedBid": new_bid,
        "periods": {"7d": period_summary(row7), "30d": period_summary(row30), "60d": period_summary(row60)},
        "decisionWindow": decision_label,
        "trendNote": trend_note,
        "lastChangeNote": last_change_note,
        "calculation": calc_steps,
    }


def RunBidOptimizerDryRun(request):
    """Read-only: computes proposed bid changes from historical
    ads_keyword_stats and returns them - does NOT write anything to Amazon.
    Query params (all optional): target_acos, lookback_days, attribution_lag_days,
    min_spend, zero_sales_spend, tolerance_pct, max_change_pct, min_bid,
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
    min_spend = get_float("min_spend", DEFAULT_MIN_SPEND)
    zero_sales_spend = get_float("zero_sales_spend", DEFAULT_ZERO_SALES_SPEND)
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
        # Full change history (any age) - lets a same-direction change from
        # before this window, that still hasn't fixed the ACOS, dampen a
        # new proposal instead of being invisible to it (see
        # propose_bid_change_multi_period's `last_change` docstring).
        last_change_by_target = fetch_last_change_per_target(token)

        proposals = []
        skipped_recently_changed = 0

        if multi_period:
            recent_start = (now_la - timedelta(days=attribution_lag_days + recent_days)).strftime("%Y-%m-%d")
            baseline_start = (now_la - timedelta(days=attribution_lag_days + baseline_days)).strftime("%Y-%m-%d")
            year_start = f"{now_la.year:04d}-01-01"

            # One fetch over the widest window this year needs (YTD, which
            # is always >= the baseline window) covers the 60d/30d/7d/YTD
            # aggregates below without 4 separate network round trips - same
            # reasoning as the original single-baseline-fetch fix.
            raw_rows = fetch_keyword_raw_rows(token, min(year_start, baseline_start), end_date, country_code)
            rows30 = with_portfolio(aggregate_keyword_rows(raw_rows, start_date, end_date))
            rows7 = {r["targetId"]: r for r in aggregate_keyword_rows(raw_rows, recent_start, end_date)}
            rows60 = {r["targetId"]: r for r in aggregate_keyword_rows(raw_rows, baseline_start, end_date)}
            rows_year_this = {r["targetId"]: r for r in aggregate_keyword_rows(raw_rows, year_start, end_date)}

            # Same-day-of-year comparison a year back, added 2026-08-31 at
            # the user's request - one separate fetch (this range has no
            # overlap with the one above) covering last year's YTD-
            # equivalent, from which the 60d/30d/7d/year sub-windows are
            # aggregated the same in-memory way. As of this build,
            # ads_keyword_stats has no rows before 2026-01-01 at all, so
            # every lastYear cell below will legitimately come back empty
            # until real prior-year history accumulates - that's a real
            # data gap, not a bug, and the frontend shows it as "no data"
            # rather than a misleading 0.
            a_year_ago = now_la - timedelta(days=365)  # plain 365-day shift, not .replace(year=) - sidesteps Feb 29 edge cases
            last_year = a_year_ago.year
            last_year_end = (a_year_ago - timedelta(days=attribution_lag_days)).strftime("%Y-%m-%d")
            last_year_60_start = (a_year_ago - timedelta(days=attribution_lag_days + baseline_days)).strftime("%Y-%m-%d")
            last_year_30_start = (a_year_ago - timedelta(days=attribution_lag_days + lookback_days)).strftime("%Y-%m-%d")
            last_year_7_start = (a_year_ago - timedelta(days=attribution_lag_days + recent_days)).strftime("%Y-%m-%d")
            last_year_start = f"{last_year:04d}-01-01"
            raw_rows_last_year = fetch_keyword_raw_rows(token, last_year_start, last_year_end, country_code)
            rows_year_last = {r["targetId"]: r for r in aggregate_keyword_rows(raw_rows_last_year, last_year_start, last_year_end)}
            rows60_last = {r["targetId"]: r for r in aggregate_keyword_rows(raw_rows_last_year, last_year_60_start, last_year_end)}
            rows30_last = {r["targetId"]: r for r in aggregate_keyword_rows(raw_rows_last_year, last_year_30_start, last_year_end)}
            rows7_last = {r["targetId"]: r for r in aggregate_keyword_rows(raw_rows_last_year, last_year_7_start, last_year_end)}

            def yoy_cell(row):
                """Same shape (and same contribution math) as period_summary
                above, reused here for This Year/Last Year x Year/60d/30d/7d
                so the frontend's comparison table can show the same level
                of detail (spend/clicks/sales/ACOS/contribution) that used
                to live in a separate set of period-summary lines, before
                those were folded into this table per the user's request."""
                if not row or not row.get("spend"):
                    return None
                acos = round(row["spend"] / row["sales"] * 100, 1) if row.get("sales") else None
                contribution = None
                if acos is not None:
                    deviation = (acos - target_acos) / target_acos * 100
                    suggested = max(-max_change_pct, min(max_change_pct, -deviation))
                    contribution = {"deviationPct": round(deviation, 1), "suggestedChangePct": round(suggested, 1)}
                clicks = row.get("clicks", 0)
                return {
                    "spend": round(row["spend"], 2),
                    "sales": round(row.get("sales", 0), 2),
                    "clicks": clicks,
                    "cpc": round(row["spend"] / clicks, 2) if clicks else None,
                    "acos": acos,
                    "contribution": contribution,
                }

            targets_evaluated = len(rows60) if rows60 else len(rows30)
            for row30 in rows30:
                target_id = row30["targetId"]
                row60 = rows60.get(target_id)
                if not row60:
                    continue  # not present at all in the baseline window - no signal
                row7 = rows7.get(target_id, {**row30, "clicks": 0, "spend": 0.0, "sales": 0.0})
                proposal = propose_bid_change_multi_period(
                    row30, row7, row60, target_acos, min_spend, zero_sales_spend,
                    tolerance_pct, max_change_pct, min_bid, max_bid,
                    last_change=last_change_by_target.get(target_id)
                )
                if not proposal:
                    continue
                if target_id in recently_changed_ids:
                    skipped_recently_changed += 1
                    continue
                proposal["yearOverYear"] = {
                    "year": {"thisYear": yoy_cell(rows_year_this.get(target_id)), "lastYear": yoy_cell(rows_year_last.get(target_id))},
                    "60d": {"thisYear": yoy_cell(row60), "lastYear": yoy_cell(rows60_last.get(target_id))},
                    "30d": {"thisYear": yoy_cell(row30), "lastYear": yoy_cell(rows30_last.get(target_id))},
                    "7d": {"thisYear": yoy_cell(row7), "lastYear": yoy_cell(rows7_last.get(target_id))},
                }
                proposals.append(proposal)
        else:
            raw_rows = fetch_keyword_raw_rows(token, start_date, end_date, country_code)
            rows30 = with_portfolio(aggregate_keyword_rows(raw_rows, start_date, end_date))
            targets_evaluated = len(rows30)
            for row in rows30:
                proposal = propose_bid_change(
                    row, target_acos, min_spend, zero_sales_spend, tolerance_pct, max_change_pct, min_bid, max_bid
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
                "minSpend": min_spend,
                "zeroSalesSpend": zero_sales_spend,
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
