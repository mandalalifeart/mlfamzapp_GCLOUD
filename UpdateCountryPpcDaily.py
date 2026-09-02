"""Daily per-country PPC breakdown, distinct from country_sales' monthly-only
ppc_spend/ppc_sales fields (which still get updated the same way, from the
same source - see UpdateSkuSalesMonth.merge_ppc_into_country_rows). The user
wants a genuinely daily view of Ads spend/sales per marketplace, not just a
running monthly total, so this writes one row per (marketplace, date) into
its own collection (country_ppc_daily) rather than overloading country_sales,
whose schema/downstream readers assume exactly one row per marketplace per
month/year.

Source is ads_campaign_stats, which is already daily - this just re-groups
it by (country_code -> marketplace, date) instead of by (country_code,
month, year), and folds SE/PL into a converted-to-EUR "eu" row per day, the
same way the monthly merge already does.

Catch-up-safe like every other ads pipeline here: defaults to since-the-
last-recorded-day through yesterday, capped so a first-ever/long-gap run
doesn't try to reprocess months of ads_campaign_stats data in one call.
"""
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from AdsAuth import cors_headers, json_response
from AdsReporting import (
    ADMIN_KEY,
    POCKETBASE_BATCH_SIZE,
    POCKETBASE_URL,
    last_recorded_date,
    pb_authenticate,
    pb_batch,
)
from UpdateSkuSalesMonth import (
    ADS_COUNTRY_TO_MARKETPLACE,
    EU_MARKETPLACE_CURRENCY,
    get_fx_rate_to_eur,
)

SYSTEM_TZ = ZoneInfo("Asia/Jerusalem")  # matches this machine's local cron timezone, not Amazon's
POCKETBASE_COUNTRY_PPC_DAILY_COLLECTION = os.environ.get(
    "POCKETBASE_COUNTRY_PPC_DAILY_COLLECTION", "country_ppc_daily"
)
MAX_LOOKBACK_DAYS = 92  # ~3 months - a first-ever run shouldn't try to reprocess all of history in one call


def fetch_ppc_daily_totals(token, start_date, end_date):
    """{(marketplace, date): {"spend":..., "sales":..., "currency":...}}
    from ads_campaign_stats, for the given date range."""
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
                "fields": "country_code,currency_code,date,spend,sales",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            marketplace_code = ADS_COUNTRY_TO_MARKETPLACE.get(item.get("country_code"))
            date_str = item.get("date")
            if not marketplace_code or not date_str:
                continue
            key = (marketplace_code, date_str)
            bucket = totals.setdefault(key, {"spend": 0.0, "sales": 0.0, "currency": item.get("currency_code", "")})
            bucket["spend"] += float(item.get("spend") or 0)
            bucket["sales"] += float(item.get("sales") or 0)
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return totals


def build_daily_rows(totals):
    """Per-marketplace rows plus a combined "eu" row per date, SE/PL
    converted to EUR - mirrors merge_ppc_into_country_rows's EU handling."""
    eu_by_date = {}
    rows = []
    for (marketplace, date_str), vals in totals.items():
        rows.append({
            "marketplace": marketplace,
            "date": date_str,
            "ppc_spend": round(vals["spend"], 2),
            "ppc_sales": round(vals["sales"], 2),
            "currency": vals["currency"],
        })
        if marketplace in EU_MARKETPLACE_CURRENCY:
            year, month = int(date_str[:4]), int(date_str[5:7])
            rate = get_fx_rate_to_eur(EU_MARKETPLACE_CURRENCY[marketplace], month, year)
            bucket = eu_by_date.setdefault(date_str, {"spend": 0.0, "sales": 0.0})
            bucket["spend"] += vals["spend"] * rate
            bucket["sales"] += vals["sales"] * rate

    for date_str, vals in eu_by_date.items():
        rows.append({
            "marketplace": "eu",
            "date": date_str,
            "ppc_spend": round(vals["spend"], 2),
            "ppc_sales": round(vals["sales"], 2),
            "currency": "EUR",
        })
    return rows


def pb_delete_range(token, start_date, end_date):
    """Deletes any existing country_ppc_daily rows in [start_date, end_date]
    before writing fresh ones, so a re-run of an already-covered range
    (e.g. two overlapping catch-up calls) doesn't duplicate rows."""
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_COUNTRY_PPC_DAILY_COLLECTION}/records",
            headers={"Authorization": token},
            params={
                "filter": f'(date >= "{start_date}" && date <= "{end_date}")',
                "fields": "id",
                "perPage": 200,
                "page": page,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        ids.extend(item["id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1

    ops = [
        {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_COUNTRY_PPC_DAILY_COLLECTION}/records/{rid}"}
        for rid in ids
    ]
    for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
        pb_batch(token, ops[i:i + POCKETBASE_BATCH_SIZE])
    return len(ids)


def pull_and_store_country_ppc_daily(start_date, end_date):
    token = pb_authenticate()
    deleted = pb_delete_range(token, start_date, end_date)

    totals = fetch_ppc_daily_totals(token, start_date, end_date)
    rows = build_daily_rows(totals)

    ops = [
        {"method": "POST", "url": f"/api/collections/{POCKETBASE_COUNTRY_PPC_DAILY_COLLECTION}/records", "body": row}
        for row in rows
    ]
    for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
        pb_batch(token, ops[i:i + POCKETBASE_BATCH_SIZE])

    return {"rowsDeleted": deleted, "rowsWritten": len(rows)}


def UpdateCountryPpcDaily(request):
    if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
        return json_response({"error": "Unauthorized"}, 401)

    now_local = datetime.now(SYSTEM_TZ)
    yesterday = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    if request.args.get("start_date"):
        start_date = request.args["start_date"]
        end_date = request.args.get("end_date", yesterday)
    else:
        start_date = (now_local - timedelta(days=MAX_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        end_date = yesterday
        try:
            token = pb_authenticate()
            last_date = last_recorded_date(token, POCKETBASE_COUNTRY_PPC_DAILY_COLLECTION)
            if last_date:
                gap_start = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                floor_date = (now_local - timedelta(days=MAX_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
                start_date = max(gap_start, floor_date)
        except Exception:
            pass

    if start_date > end_date:
        return json_response({"startDate": start_date, "endDate": end_date, "skipped": "already up to date"})

    try:
        result = pull_and_store_country_ppc_daily(start_date, end_date)
        return json_response({"startDate": start_date, "endDate": end_date, **result})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetCountryPpcDaily(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    start_date = request.args.get("start_date") if hasattr(request, "args") else None
    end_date = request.args.get("end_date") if hasattr(request, "args") else None
    marketplace = request.args.get("marketplace") if hasattr(request, "args") else None

    if not start_date or not end_date:
        now_local = datetime.now(SYSTEM_TZ)
        end_date = now_local.strftime("%Y-%m-%d")
        start_date = (now_local - timedelta(days=30)).strftime("%Y-%m-%d")

    try:
        token = pb_authenticate()
        filter_str = f'(date >= "{start_date}" && date <= "{end_date}")'
        if marketplace:
            filter_str += f' && marketplace = "{marketplace}"'

        rows = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_COUNTRY_PPC_DAILY_COLLECTION}/records",
                headers={"Authorization": token},
                params={"filter": filter_str, "perPage": 500, "page": page, "sort": "date"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                rows.append({
                    "marketplace": item.get("marketplace"),
                    "date": item.get("date"),
                    "ppcSpend": item.get("ppc_spend", 0),
                    "ppcSales": item.get("ppc_sales", 0),
                    "currency": item.get("currency", ""),
                })
            if page >= data.get("totalPages", 1):
                break
            page += 1

        return json_response({"startDate": start_date, "endDate": end_date, "rows": rows})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
