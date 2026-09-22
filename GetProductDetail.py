import json
import os
from collections import defaultdict
from datetime import date, datetime
from zoneinfo import ZoneInfo

import requests

from GetNextOrderData import (
    REGION_RECO_CONFIGS,
    apply_category_min_order,
    compute_region_recommendations,
    compute_trailing_total_prorated,
    shipment_window_months,
    trailing_months_with_current,
)
from NextOrderSettings import get_or_create_next_shipment_date

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_SKU_COLLECTION = os.environ.get("POCKETBASE_SKU_COLLECTION", "sku_sales")
POCKETBASE_STATS_COLLECTION = os.environ.get("POCKETBASE_STATS_COLLECTION", "sku_statistics")
POCKETBASE_MAPPING_COLLECTION = os.environ.get("POCKETBASE_MAPPING_COLLECTION", "asin_group_mapping")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
LA_TZ = ZoneInfo("America/Los_Angeles")

# The "same info as the Sales page" for one product, broken out by these 3
# marketplaces specifically (as requested) - "eu" is the combined EU-region
# bucket (see GetSalesDepartmentReport), not the same thing as sku_statistics'
# per-country "de" stock figure used below for the stock-level row.
DETAIL_MARKETPLACES = ["usa", "eu", "uk"]

# Canada/Mexico sku_sales rows (from the historical bulk import, never their
# own UI marketplace - see CLAUDE.md) are folded into "usa" per the user's
# explicit request (2026-09-04), rather than shown as their own bucket or
# left invisible. sku_sales has no money field (quantity-only), so this is a
# plain quantity merge with no currency-mixing concern.
DETAIL_MARKETPLACE_ATOMIC = {
    "usa": {"usa", "ca", "mex"},
    "eu": {"eu"},
    "uk": {"uk"},
}


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
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
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase auth failed: HTTP {response.status_code} - {response.text}")
    token = response.json().get("token")
    if not token:
        raise RuntimeError("PocketBase auth response missing token")
    return token


def pb_escape(value):
    return value.replace("\\", "\\\\").replace('"', '\\"')


def fetch_all(token, collection, filter_str, fields=None):
    records = []
    page = 1
    while True:
        params = {"filter": filter_str, "perPage": 500, "page": page}
        if fields:
            params["fields"] = fields
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params=params,
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed for {collection}: HTTP {response.status_code} - {response.text}")
        data = response.json()
        records.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return records


def find_one(token, collection, filter_str):
    items = fetch_all(token, collection, filter_str)
    return items[0] if items else None


POCKETBASE_INVENTORY_HISTORY_COLLECTION = os.environ.get("POCKETBASE_INVENTORY_HISTORY_COLLECTION", "sku_inventory_history")


def fetch_sales_by_month_for_asin(token, asin, marketplaces):
    """Same shape as GetNextOrderData.fetch_all_sales_by_asin_month's
    per-ASIN value ({(year, month): qty}), but scoped to just this one ASIN
    - a targeted PocketBase filter instead of a bulk fetch, since the
    product page only ever needs one product's reco, not every SKU's."""
    marketplace_clause = " || ".join(f'marketplace = "{mkt}"' for mkt in marketplaces)
    rows = fetch_all(token, POCKETBASE_SKU_COLLECTION, f'ASIN = "{pb_escape(asin)}" && ({marketplace_clause})', fields="quantity,year,month")
    totals = defaultdict(int)
    for row in rows:
        totals[(row.get("year"), row.get("month"))] += int(row.get("quantity") or 0)
    return totals


def fetch_zero_inventory_weeks_for_asin(token, asin, region, since_date_str):
    rows = fetch_all(
        token, POCKETBASE_INVENTORY_HISTORY_COLLECTION,
        f'asin = "{pb_escape(asin)}" && region = "{region}" && week_date >= "{since_date_str}" && balance <= 0',
        fields="week_date",
    )
    return len({row.get("week_date") for row in rows})


def compute_product_recos(token, asin, sku, stock):
    """Same Reco engine as GetNextOrderData.py (A/B recommendation +
    stockout-week exclusion + category minimum threshold), just scoped to
    this one product's ASIN instead of every SKU - added 2026-09-22 at the
    user's request to show Reco on the product page too. Returns
    {region: {"recommended": int, "recommendedSeasonal": int,
    "seasonalSource": str, "avgMonthly": float, "debug": {...}}}."""
    next_shipment_date_str = get_or_create_next_shipment_date(token)
    next_shipment_date = date.fromisoformat(next_shipment_date_str)
    today = date.today()

    trailing_months = trailing_months_with_current(3, today)
    year1_months = shipment_window_months(next_shipment_date, 1)
    year2_months = shipment_window_months(next_shipment_date, 2)
    trailing_window_start = f"{trailing_months[0][0]:04d}-{trailing_months[0][1]:02d}-01"

    recos = {}
    for region, cfg in REGION_RECO_CONFIGS.items():
        asin_sales = fetch_sales_by_month_for_asin(token, asin, cfg["sales_marketplaces"])
        zero_inventory_weeks = fetch_zero_inventory_weeks_for_asin(token, asin, region, trailing_window_start)
        already_covered = (stock.get(cfg["balance"]) or 0) + (stock.get(cfg["otw"]) or 0) + (stock.get(cfg["next"]) or 0)
        trailing_total, lookback_days, _ = compute_trailing_total_prorated(asin_sales, 3, today)
        reco = compute_region_recommendations(
            asin_sales, already_covered, next_shipment_date, today,
            trailing_total, lookback_days, year1_months, year2_months,
            zero_inventory_weeks,
        )
        recos[region] = {
            "recommended": apply_category_min_order(sku, reco["reco_recent"]),
            "recommendedSeasonal": apply_category_min_order(sku, reco["reco_seasonal"]),
            "seasonalSource": reco["seasonal_source"],
            "avgMonthly": reco["avg_monthly"],
            "debug": {
                "trailingTotal": reco["trailing_total"],
                "year1Total": reco["year1_total"],
                "year2Total": reco["year2_total"],
                "xDays": reco["x_days"],
                "needForXDays": reco["need_for_x_days"],
                "alreadyCovered": reco["already_covered"],
                "avgDailyRecent": reco["avg_daily_recent"],
                "avgDailySeasonal": reco["avg_daily_seasonal"],
                "zeroInventoryWeeks": reco["zero_inventory_weeks"],
            },
        }
    return recos, next_shipment_date_str


def build_year_rows(records, years, current_month):
    year_months = defaultdict(lambda: [0] * 12)
    for rec in records:
        year = int(rec.get("year") or 0)
        if year not in years:
            continue
        month = int(rec.get("month") or 0)
        if not 1 <= month <= 12:
            continue
        year_months[year][month - 1] += int(rec.get("quantity") or 0)

    completed_months = max(current_month - 1, 0)
    year_rows = [{"year": y, "months": year_months.get(y, [0] * 12), "total": sum(year_months.get(y, [0] * 12))} for y in years]
    this_year_partial = sum(year_rows[0]["months"][:completed_months])
    last_year_partial = sum(year_rows[1]["months"][:completed_months]) if len(year_rows) > 1 else 0
    growth_pct = (
        round((this_year_partial - last_year_partial) / last_year_partial * 100, 1)
        if last_year_partial > 0
        else None
    )
    return year_rows, growth_pct


def GetProductDetail(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        body = request.get_json(silent=True) or {}
        asin = (body.get("asin") or "").strip()
        if not asin:
            return json_response({"error": "asin is required"}, 400)

        now = datetime.now(LA_TZ)
        current_year = now.year
        current_month = now.month
        years = [current_year, current_year - 1, current_year - 2, current_year - 3]
        min_year = years[-1]

        token = pb_authenticate()

        mapping_record = find_one(token, POCKETBASE_MAPPING_COLLECTION, f'asin="{pb_escape(asin)}"')
        main_sku = mapping_record.get("sku") if mapping_record else ""
        group = mapping_record.get("group") if mapping_record else ""

        # IGNORE is a valid destination group (excludes a SKU from every report)
        # but real mapping rows rarely use it, so add it explicitly - same
        # reasoning as the Sales page's "Move to Group" dropdown.
        all_mapping_rows = fetch_all(token, POCKETBASE_MAPPING_COLLECTION, "", fields="group")
        all_groups = sorted({(row.get("group") or "").strip() for row in all_mapping_rows if row.get("group")} | {"IGNORE"})

        sales_records = fetch_all(
            token,
            POCKETBASE_SKU_COLLECTION,
            f'(ASIN="{pb_escape(asin)}" && year>={min_year})',
            fields="marketplace,month,year,quantity",
        )

        marketplaces = {}
        for mp in DETAIL_MARKETPLACES:
            atomic = DETAIL_MARKETPLACE_ATOMIC[mp]
            mp_records = [r for r in sales_records if r.get("marketplace") in atomic]
            year_rows, growth_pct = build_year_rows(mp_records, years, current_month)
            marketplaces[mp] = {"yearRows": year_rows, "growthPct": growth_pct}

        stats_record = None
        if main_sku:
            stats_record = find_one(token, POCKETBASE_STATS_COLLECTION, f'sku="{pb_escape(main_sku)}"')

        stock_fields = [
            "uk_balance", "uk_balance_fba", "uk_balance_awd", "uk_on_the_way", "uk_next_shipment",
            "de_balance", "de_balance_fba", "de_balance_awd", "de_balance_lg", "de_on_the_way", "de_next_shipment",
            "usa_balance", "usa_balance_fba", "usa_balance_awd", "usa_on_the_way", "usa_next_shipment",
            "malani_balance", "malani_order", "next_order",
        ]
        stock = {field: (stats_record or {}).get(field) or 0 for field in stock_fields}

        recos, next_shipment_date_str = compute_product_recos(token, asin, main_sku or "", stock)

        return json_response({
            "status": "success",
            "asin": asin,
            "mainSku": main_sku,
            "group": group,
            "allGroups": all_groups,
            "years": years,
            "currentMonth": current_month,
            "marketplaces": marketplaces,
            "stock": stock,
            "recos": recos,
            "nextShipmentDate": next_shipment_date_str,
        })

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
