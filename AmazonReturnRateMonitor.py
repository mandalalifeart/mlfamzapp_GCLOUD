"""Weekly Amazon return-rate monitor, added 2026-09-16 at the user's request
("monitor every week return patterns and rate, goal is 10 percent").

Two separate things get computed and sent in one Telegram digest
(@baba_social_bot, same bot as every other digest/notification in this
project - see CLAUDE.md "email is alerts/errors only, Telegram gets
everything"):

1. Return RATE per ASIN, against the user's 10% goal. A single calendar
   week's returns can't be sanely divided by that same week's sales (a
   return this week is very often for something sold weeks/months ago -
   the same attribution-lag problem AdsBidOptimizer.py already handles for
   click-to-conversion lag), so the rate is computed over a rolling
   trailing window (ROLLING_WINDOW_DAYS, default 90) recomputed fresh every
   week, not "this week's returns / this week's sales."
2. Return PATTERNS - the actual reason breakdown for returns that arrived
   in the real past 7 days (this part IS calendar-week-scoped, since
   "what did people say this week" is a real, meaningful question on its
   own, unlike a bare rate).

No new PocketBase collection yet - this reads straight from sku_sales and
amazon_returns (both already populated) and sends a Telegram message, same
minimal shape as DailyAdsPerformanceDigest.py. If trend-over-time (is the
rate improving week to week) is wanted later, that needs a small history
collection to snapshot into - not built yet since a single week's answer to
"are we over 10%" doesn't require it.
"""
import os
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from AdsAuth import cors_headers, json_response
from AdsReporting import ADMIN_KEY, POCKETBASE_URL, pb_authenticate

TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")
SYSTEM_TZ = ZoneInfo("Asia/Jerusalem")

RETURN_RATE_GOAL_PCT = float(os.environ.get("RETURN_RATE_GOAL_PCT", "10"))
ROLLING_WINDOW_DAYS = int(os.environ.get("RETURN_RATE_WINDOW_DAYS", "90"))
MIN_SALES_IN_WINDOW = int(os.environ.get("RETURN_RATE_MIN_SALES", "15"))

# Same conventions as the /returns page and the ad-hoc ASIN-return-rate
# analysis this digest is based on: fold Canada/Mexico sales into "usa",
# exclude the literal "eu" aggregate row (it duplicates the real per-country
# EU rows) and the Etsy marketplace tags (amazon_returns has no Etsy data to
# compare against).
USA_MARKETPLACES = {"usa", "ca", "mex"}
EXCLUDED_SALES_MARKETPLACES = {"eu", "etsy_eu", "etsy_usa"}


def fetch_all(token, collection, params):
    items, page = [], 1
    while True:
        p = dict(params)
        p["page"] = page
        p["perPage"] = 500
        resp = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params=p,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return items


def compute_return_rates(token, now):
    """Returns (overall_rate_pct, overall_returned, overall_sold, per_asin
    list sorted by rate desc, sku_by_asin map) over the trailing
    ROLLING_WINDOW_DAYS, current year's sku_sales only (fine for a 90-day
    window - it never spans a year boundary by more than a few days, and
    year-boundary undercounting there is a rare, small edge case)."""
    window_start = (now - timedelta(days=ROLLING_WINDOW_DAYS)).strftime("%Y-%m-%d")

    sales_rows = fetch_all(token, "sku_sales", {"filter": f"year = {now.year}", "fields": "ASIN,quantity,marketplace,sku"})
    sold_usa, sold_eu = defaultdict(int), defaultdict(int)
    sku_by_asin = {}
    for r in sales_rows:
        asin, mkt = r.get("ASIN"), r.get("marketplace")
        if not asin or mkt in EXCLUDED_SALES_MARKETPLACES:
            continue
        qty = int(r.get("quantity") or 0)
        (sold_usa if mkt in USA_MARKETPLACES else sold_eu)[asin] += qty
        if asin not in sku_by_asin and r.get("sku"):
            sku_by_asin[asin] = r["sku"]

    return_rows = fetch_all(
        token,
        "amazon_returns",
        {
            "filter": f'return_date >= "{window_start}"',
            "fields": "asin,quantity,marketplace,reason,return_date",
        },
    )
    returned_usa, returned_eu = defaultdict(int), defaultdict(int)
    for r in return_rows:
        asin, mkt = r.get("asin"), r.get("marketplace")
        if not asin:
            continue
        qty = int(r.get("quantity") or 0)
        if mkt == "usa":
            returned_usa[asin] += qty
        elif mkt == "eu":
            returned_eu[asin] += qty

    all_asins = set(sold_usa) | set(sold_eu)
    per_asin = []
    total_sold_all = total_returned_all = 0
    for asin in all_asins:
        sold = sold_usa.get(asin, 0) + sold_eu.get(asin, 0)
        returned = returned_usa.get(asin, 0) + returned_eu.get(asin, 0)
        total_sold_all += sold
        total_returned_all += returned
        if sold < MIN_SALES_IN_WINDOW:
            continue
        per_asin.append((asin, returned / sold * 100, returned, sold))
    per_asin.sort(key=lambda x: -x[1])

    overall_rate = (total_returned_all / total_sold_all * 100) if total_sold_all else 0.0
    return overall_rate, total_returned_all, total_sold_all, per_asin, sku_by_asin, return_rows


def compute_week_reasons(return_rows_window, now):
    """Reason breakdown for returns actually dated in the real past 7 days
    (a subset of the wider rolling-window rows already fetched above, so no
    extra PocketBase call needed)."""
    week_start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    reason_counts = Counter()
    week_total = 0
    for r in return_rows_window:
        if (r.get("return_date") or "") < week_start:
            continue
        reason = r.get("reason") or "NO_REASON_GIVEN"
        qty = int(r.get("quantity") or 0)
        reason_counts[reason] += qty
        week_total += qty
    return week_total, reason_counts


def build_digest_text(now):
    token = pb_authenticate()
    overall_rate, total_returned, total_sold, per_asin, sku_by_asin, return_rows_window = compute_return_rates(token, now)
    week_total, week_reasons = compute_week_reasons(return_rows_window, now)

    over_goal = [row for row in per_asin if row[1] >= RETURN_RATE_GOAL_PCT]

    status_emoji = "✅" if overall_rate <= RETURN_RATE_GOAL_PCT else "⚠️"
    lines = [
        f"📦 Weekly Return Rate Monitor - {now.strftime('%Y-%m-%d')}",
        "",
        f"{status_emoji} Overall return rate (trailing {ROLLING_WINDOW_DAYS}d): {overall_rate:.1f}% "
        f"({total_returned}/{total_sold}) - goal: {RETURN_RATE_GOAL_PCT:.0f}%",
        "",
    ]

    if over_goal:
        lines.append(f"ASINs at/above the {RETURN_RATE_GOAL_PCT:.0f}% goal ({len(over_goal)} of {len(per_asin)} tracked, min {MIN_SALES_IN_WINDOW} sold):")
        for asin, rate, returned, sold in over_goal[:15]:
            sku = sku_by_asin.get(asin, asin)
            lines.append(f"  {sku} ({asin}): {rate:.1f}% ({returned}/{sold})")
        if len(over_goal) > 15:
            lines.append(f"  ...and {len(over_goal) - 15} more")
    else:
        lines.append(f"No tracked ASIN is at/above the {RETURN_RATE_GOAL_PCT:.0f}% goal. 🎉")

    lines.append("")
    lines.append(f"Return reasons, last 7 days ({week_total} returns):")
    if week_reasons:
        for reason, count in week_reasons.most_common(6):
            lines.append(f"  {reason.replace('_', ' ').title()}: {count}")
    else:
        lines.append("  (no returns recorded this week)")

    return "\n".join(lines)


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if len(text) > 3900:
        text = text[:3900] + "\n...(truncated)"
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=15,
    )


def SendWeeklyReturnRateDigest(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    try:
        now = datetime.now(SYSTEM_TZ)
        text = build_digest_text(now)
        send_telegram(text)
        return json_response({"sent": True, "text": text})
    except Exception as exc:
        return json_response({"sent": False, "error": str(exc)}, 500)
