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
    shipment_window_months,
    trailing_months_with_current,
)
from NextOrderSettings import get_next_shipment_dates, region_cover_months

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


def compute_product_recos(token, asin, sku, stock):
    """Same Reco engine as GetNextOrderData.py (A/B recommendation +
    stockout-week exclusion + category minimum threshold), just scoped to
    this one product's ASIN instead of every SKU - added 2026-09-22 at the
    user's request to show Reco on the product page too. Three-value A/B/C
    split (2026-09-22, per the user - see GetNextOrderData.py's
    compute_region_recommendations docstring). Returns
    {region: {"recommended": int, "recommendedSeasonal1yr": int,
    "recommendedSeasonal2yr": int, "seasonal1yrSource": str,
    "seasonal2yrSource": str, "avgMonthly": float, "debug": {...}}}."""
    shipment_dates = get_next_shipment_dates(token)
    region_dates = {r: date.fromisoformat(shipment_dates[r]) for r in REGION_RECO_CONFIGS}
    pb_count = region_cover_months(token, region_dates)
    next_shipment_date_str = shipment_dates["usa"]
    next_shipment_date = region_dates["usa"]
    today = date.today()

    trailing_months = trailing_months_with_current(3, today)
    year1_months = shipment_window_months(next_shipment_date, 1)
    year2_months = shipment_window_months(next_shipment_date, 2)
    from StockDays import load_stock_days
    from GetNextOrderData import BALANCE_PARTS
    stock_days, covered_days = load_stock_days(token, asin=asin)
    stock = dict(stock)
    for region, parts in BALANCE_PARTS.items():  # Bal = FBA + AWD (+ LG for DE)
        values = [stock.get(f"{region}_balance_{p}") or 0 for p in parts]
        if any(values):
            stock[f"{region}_balance"] = sum(values)

    recos = {}
    for region, cfg in REGION_RECO_CONFIGS.items():
        asin_sales = fetch_sales_by_month_for_asin(token, asin, cfg["sales_marketplaces"])
        # Reco ignores the Next column (per the user 2026-10-03): Bal + OTW only.
        already_covered = (stock.get(cfg["balance"]) or 0) + (stock.get(cfg["otw"]) or 0)
        reco = compute_region_recommendations(
            asin_sales, already_covered, region_dates[region], today, trailing_months,
            shipment_window_months(region_dates[region], 1, pb_count[region]), shipment_window_months(region_dates[region], 2, pb_count[region]), stock_days[region].get(asin, {}), covered_days[region], pb_count[region],
        )
        recos[region] = {
            "recommended": apply_category_min_order(sku, reco["reco_recent"]),
            "recommendedSeasonal1yr": apply_category_min_order(sku, reco["reco_seasonal_1yr"]),
            "recommendedSeasonal2yr": apply_category_min_order(sku, reco["reco_seasonal_2yr"]),
            "seasonal1yrSource": reco["seasonal_1yr_source"],
            "seasonal2yrSource": reco["seasonal_2yr_source"],
            "avgMonthly": reco["avg_monthly"],
            "debug": reco["debug"],
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
        sku_in = (body.get("sku") or "").strip()
        # Per the user (2026-10-12): the product page works by SKU alone - resolve
        # the ASIN from asin_group_mapping when only a SKU is given.
        if sku_in:
            token0 = pb_authenticate()
            rec = find_one(token0, POCKETBASE_MAPPING_COLLECTION, f'sku="{pb_escape(sku_in)}"')
            if rec and rec.get("asin"):
                asin = rec["asin"]
            elif not asin:  # unmapped SKU and no ASIN to fall back on
                return json_response({"error": f"SKU {sku_in} not found in the mapping"}, 404)
        if not asin:
            return json_response({"error": "sku (or asin) is required"}, 400)

        now = datetime.now(LA_TZ)
        current_year = now.year
        current_month = now.month
        years = [current_year, current_year - 1, current_year - 2, current_year - 3]
        min_year = years[-1]

        token = pb_authenticate()

        mapping_record = find_one(token, POCKETBASE_MAPPING_COLLECTION, f'asin="{pb_escape(asin)}"')
        main_sku = mapping_record.get("sku") if mapping_record else ""
        group = mapping_record.get("group") if mapping_record else ""
        # end_of_life is a product-level (ASIN-level) flag, not tied to
        # whichever single SKU spelling this find_one() happened to match -
        # true if ANY mapping row sharing this ASIN is flagged, matching
        # SetEndOfLife's own "apply to every sibling spelling" write and
        # GetNextOrderData's exclusion logic.
        all_asin_rows = fetch_all(token, POCKETBASE_MAPPING_COLLECTION, f'asin="{pb_escape(asin)}"', fields="end_of_life")
        end_of_life = any(row.get("end_of_life") for row in all_asin_rows)

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
            "endOfLife": end_of_life,
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
