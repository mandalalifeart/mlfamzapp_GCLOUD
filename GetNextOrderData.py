import calendar
import json
import os
from collections import defaultdict
from datetime import date

import requests

from NextOrderSettings import get_or_create_next_shipment_date

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_STATS_COLLECTION = os.environ.get("POCKETBASE_STATS_COLLECTION", "sku_statistics")
POCKETBASE_MAPPING_COLLECTION = os.environ.get("POCKETBASE_MAPPING_COLLECTION", "asin_group_mapping")
POCKETBASE_SALES_COLLECTION = os.environ.get("POCKETBASE_SALES_COLLECTION", "sku_sales")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

STATS_FIELDS = [
    "uk_balance", "uk_on_the_way", "uk_next_shipment",
    "de_balance", "de_on_the_way", "de_next_shipment",
    "usa_balance", "usa_on_the_way", "usa_next_shipment",
    "malani_balance", "malani_order", "next_order",
]

# USA Recommendation to Order - see compute_usa_recommendation for the full
# formula. Canada/Mexico sales are folded into "usa" everywhere sales are
# broken out by marketplace elsewhere in this app (GetProductDetail,
# GetSalesDepartmentReport, GetMarketplaceSalesSummary - see CLAUDE.md), so
# the same folding applies to the sales-velocity input here.
USA_SALES_MARKETPLACES = ("usa", "ca", "mex")
# Trailing whole calendar months used for the sales-velocity average -
# excludes the current in-progress month so a half-elapsed month doesn't
# understate velocity. 3 months mirrors the user's own ~3-month shipping
# cadence: the same 3-month total is used both to derive an average-per-day
# rate AND, taken as-is, as the forecast for the next ~3 months of demand
# (the natural assumption that the next cycle looks like the last one).
SALES_LOOKBACK_MONTHS = 3


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def fetch_mapping_records(token):
    records = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_MAPPING_COLLECTION}/records",
            headers={"Authorization": token},
            params={"perPage": 500, "page": page},
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        records.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return records


def load_mapping(token):
    # Same GROUP/IGNORE handling as GetSalesDepartmentReport.load_mapping -
    # IGNORE-group SKUs are deliberately excluded products, not candidates to reorder.
    # sku_to_asin covers every row (IGNORE included) so an sku_statistics record
    # filed under any SKU spelling - even one belonging to an ignored/retired
    # SKU string - can still be resolved to its ASIN for the join below.
    rows = []
    sku_to_asin = {}
    for row in fetch_mapping_records(token):
        sku = (row.get("sku") or "").strip()
        asin = (row.get("asin") or "").strip()
        upc = (row.get("ean") or "").strip()
        supplier_sku = (row.get("supplier_sku") or "").strip()
        group = (row.get("group") or "").strip() or "UNGROUPED"
        if not sku:
            continue
        if asin:
            sku_to_asin[sku] = asin
        if group == "IGNORE":
            continue
        rows.append({"sku": sku, "asin": asin, "upc": upc, "supplier_sku": supplier_sku, "group": group})
    return rows, sku_to_asin


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


def fetch_sku_statistics(token):
    records = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records",
            headers={"Authorization": token},
            params={"perPage": 500, "page": page},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        records.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return records


def trailing_completed_months(n, today=None):
    """Returns the last n whole calendar months before the current
    in-progress one, as (year, month) tuples, oldest first - e.g. on
    2026-09-19 with n=3: [(2026,6), (2026,7), (2026,8)]. Handles a year
    rollover the same way run_daily_monthly_update.py's month-boundary
    helpers do elsewhere in this codebase."""
    today = today or date.today()
    months = []
    year, month = today.year, today.month
    for _ in range(n):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        months.append((year, month))
    return list(reversed(months))


def fetch_usa_sales_by_asin(token, months):
    """Sums sku_sales quantity per ASIN across USA_SALES_MARKETPLACES for the
    given (year, month) pairs. sku_sales rows already carry the real ASIN a
    sale was recorded under regardless of which SKU spelling was used, so
    this sums correctly across e.g. a relisted SKU's old and new spellings
    without needing the mapping table at all."""
    if not months:
        return {}
    month_clause = " || ".join(f'(year = {y} && month = {m})' for y, m in months)
    marketplace_clause = " || ".join(f'marketplace = "{mkt}"' for mkt in USA_SALES_MARKETPLACES)
    filter_str = f"({month_clause}) && ({marketplace_clause})"

    totals = defaultdict(int)
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SALES_COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": filter_str, "perPage": 500, "page": page, "fields": "ASIN,quantity"},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        for row in data.get("items", []):
            asin = row.get("ASIN")
            if asin:
                totals[asin] += int(row.get("quantity") or 0)
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return totals


def days_in_months(months):
    """Exact real day count spanning the given (year, month) pairs (e.g.
    Jun+Jul+Aug = 30+31+31 = 92), used as the denominator for a real
    average-per-day rate instead of a flat 30-day approximation."""
    return sum(calendar.monthrange(y, m)[1] for y, m in months)


def compute_usa_recommendation(total_sales_3mo, lookback_days, item, next_shipment_date, today):
    """Per the user's exact spec (2026-09-19):
        A = today, B = next_shipment_date, x = B - A (days)
        need_for_x_days = x * (average per day over the last 3 months)
        need_for_3_months = the last 3 months' total (used as-is as the
            forecast for the NEXT 3 months, same assumption a 3-month
            shipping cadence already relies on)
        reco = need_for_3_months + need_for_x_days
               - USA_Bal - USA_OTW - USA_Next(already entered)

    x is clamped to >=0 (an overdue next_shipment_date shouldn't reduce the
    recommendation) - not otherwise specified, but the formula assumes B is
    in the future."""
    x_days = max(0, (next_shipment_date - today).days)
    avg_per_day = (total_sales_3mo / lookback_days) if lookback_days else 0
    need_for_x_days = x_days * avg_per_day
    need_for_3_months = total_sales_3mo

    already_covered = (item.get("usa_balance") or 0) + (item.get("usa_on_the_way") or 0) + (item.get("usa_next_shipment") or 0)
    return max(0, round(need_for_3_months + need_for_x_days - already_covered))


def GetNextOrderData(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        token = pb_authenticate()
        mapping_rows, sku_to_asin = load_mapping(token)
        stats_records = fetch_sku_statistics(token)
        stats_by_sku = {rec.get("sku"): rec for rec in stats_records if rec.get("sku")}

        # Index sku_statistics by ASIN (resolved via the mapping, not PocketBase
        # itself - the collection is still keyed by sku) so a record filed under
        # any SKU spelling sharing that ASIN is found, not just an exact SKU match.
        stats_by_asin = {}
        for rec in stats_records:
            asin = sku_to_asin.get(rec.get("sku") or "")
            if asin and asin not in stats_by_asin:
                stats_by_asin[asin] = rec

        next_shipment_date_str = get_or_create_next_shipment_date(token)
        next_shipment_date = date.fromisoformat(next_shipment_date_str)
        today = date.today()
        sales_months = trailing_completed_months(SALES_LOOKBACK_MONTHS, today)
        usa_sales_by_asin = fetch_usa_sales_by_asin(token, sales_months)
        lookback_days = days_in_months(sales_months)

        groups = defaultdict(list)
        for row in mapping_rows:
            stats = stats_by_sku.get(row["sku"]) or stats_by_asin.get(row["asin"]) or {}
            item = {"sku": row["sku"], "asin": row["asin"], "upc": row["upc"], "supplier_sku": row["supplier_sku"]}
            for field in STATS_FIELDS:
                item[field] = stats.get(field) or 0

            total_sales = usa_sales_by_asin.get(row["asin"], 0) if row["asin"] else 0
            item["usa_avg_monthly_sales"] = round(total_sales / SALES_LOOKBACK_MONTHS, 1)
            item["usa_recommended_order"] = compute_usa_recommendation(total_sales, lookback_days, item, next_shipment_date, today)

            groups[row["group"]].append(item)

        group_list = []
        for group, items in groups.items():
            items.sort(key=lambda i: i["sku"])
            group_list.append({"group": group, "items": items})
        group_list.sort(key=lambda g: g["group"])

        return json_response({
            "status": "success",
            "groups": group_list,
            "nextShipmentDate": next_shipment_date_str,
            "salesLookbackMonths": SALES_LOOKBACK_MONTHS,
        })

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
