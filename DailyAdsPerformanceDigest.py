"""Daily Ads performance digest to Telegram (@baba_social_bot, reusing the
same MCF_TELEGRAM_BOT_TOKEN/CHAT_ID credentials already used elsewhere in
this project - see EtsyMcfFulfillment.py) - added 2026-09-02 at the user's
request for a per-marketplace spend/sales/ACOS summary, both yesterday and
month-to-date, sent every morning after the daily campaign-stats pull.

Reads straight from ads_campaign_stats (already daily-granularity, no new
collection needed) - no Amazon API calls of its own, so it's fast and safe
to run daily without any rate-limit/cost concerns.
"""
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from AdsAuth import cors_headers, json_response
from AdsReporting import ADMIN_KEY, POCKETBASE_URL, last_recorded_date, pb_authenticate

TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")
SYSTEM_TZ = ZoneInfo("Asia/Jerusalem")

# ads_campaign_stats.country_code -> a friendly marketplace label.
COUNTRY_LABELS = {
    "US": "USA", "CA": "Canada", "MX": "Mexico", "UK": "UK",
    "DE": "Germany", "FR": "France", "IT": "Italy", "ES": "Spain",
    "JP": "Japan",
}


def fetch_country_totals(token, start_date, end_date):
    """{country_code: {"spend":..., "sales":..., "currency":...}} for
    ads_campaign_stats rows in [start_date, end_date]."""
    totals = {}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/ads_campaign_stats/records",
            headers={"Authorization": token},
            params={
                "filter": f'(date >= "{start_date}" && date <= "{end_date}")',
                "perPage": 500,
                "page": page,
                "fields": "country_code,currency_code,spend,sales",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            code = item.get("country_code")
            if not code:
                continue
            bucket = totals.setdefault(code, {"spend": 0.0, "sales": 0.0, "currency": item.get("currency_code", "")})
            bucket["spend"] += float(item.get("spend") or 0)
            bucket["sales"] += float(item.get("sales") or 0)
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return totals


def format_section(title, totals):
    lines = [title]
    real = {k: v for k, v in totals.items() if v["spend"] or v["sales"]}
    if not real:
        lines.append("  (no spend recorded)")
        return "\n".join(lines)
    for code in sorted(real, key=lambda c: -real[c]["spend"]):
        vals = real[code]
        label = COUNTRY_LABELS.get(code, code)
        acos = (vals["spend"] / vals["sales"] * 100) if vals["sales"] else None
        acos_text = f"{acos:.1f}%" if acos is not None else "n/a"
        currency = vals["currency"] or "USD"
        lines.append(f"  {label}: {currency} {vals['spend']:,.2f} spend / {currency} {vals['sales']:,.2f} sales (ACOS {acos_text})")
    return "\n".join(lines)


def build_digest_text(now):
    token = pb_authenticate()

    # The daily campaign pull can lag a real calendar day behind (a known
    # LA-timezone quirk in when that job computes "yesterday" - see
    # CLAUDE.md), so calendar-yesterday's data isn't always in yet when this
    # digest runs. Use the actual most-recent date ads_campaign_stats has,
    # not a blind calendar calculation, so "yesterday" here always means
    # "the latest day we really have numbers for".
    latest_date = last_recorded_date(token) or (now - timedelta(days=1)).strftime("%Y-%m-%d")

    # Per the user's explicit rule: the monthly section is always the
    # CURRENT month, except on the 1st of the month, when it shows LAST
    # month instead (the just-closed month's final numbers, since day 1
    # has no current-month data of its own yet) - same "day==1 -> last
    # month" convention already used by run_daily_monthly_update.py.
    if now.day == 1:
        last_day_prev_month = now.replace(day=1) - timedelta(days=1)
        month_start = last_day_prev_month.replace(day=1).strftime("%Y-%m-%d")
        month_end = last_day_prev_month.strftime("%Y-%m-%d")
    else:
        month_start = now.replace(day=1).strftime("%Y-%m-%d")
        month_end = latest_date

    yesterday_totals = fetch_country_totals(token, latest_date, latest_date)
    mtd_totals = fetch_country_totals(token, month_start, month_end)

    parts = [
        f"📊 Ads Daily Digest - {now.strftime('%Y-%m-%d')}",
        "",
        format_section(f"Latest day ({latest_date}):", yesterday_totals),
        "",
        format_section(f"{'Last month' if now.day == 1 else 'Month to date'} ({month_start} to {month_end}):", mtd_totals),
    ]
    return "\n".join(parts)


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


def SendDailyAdsDigest(request):
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
