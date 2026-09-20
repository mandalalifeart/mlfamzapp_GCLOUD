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
POCKETBASE_INVENTORY_HISTORY_COLLECTION = os.environ.get("POCKETBASE_INVENTORY_HISTORY_COLLECTION", "sku_inventory_history")
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

# Per the user (2026-09-19, refined 2026-09-20): never recommend a token/
# small order for these product families - real minimum-practical-order-
# size thresholds, not a calculation input. Refined rule (replaces the
# original "floor up to the minimum" behavior): below half the category
# minimum, the real need is too small to bother with at all, so it's
# zeroed out; at or above half the minimum, the computed value is shown
# AS-IS (NOT rounded up to the full minimum - e.g. a computed 8 for a pouf
# cover, minimum 15, shows 8, not 15; a computed 5 shows 0). Applied AFTER
# the reco formula, and only when the formula already recommends something
# (a real 0 - "already covered" - is left alone). Matched by a case-
# insensitive substring of the SKU (confirmed against the full
# asin_group_mapping table: every SKU in a PAREO_* group contains "pareo",
# every SKU in a pouf-cover group contains "pouf", with zero exceptions),
# so no dependency on the mapping's own group naming.
CATEGORY_MIN_ORDER = (
    ("pareo", 50),
    ("pouf", 15),
)


def apply_category_min_order(sku, reco):
    if reco <= 0:
        return reco
    sku_lower = sku.lower()
    for keyword, minimum in CATEGORY_MIN_ORDER:
        if keyword in sku_lower:
            return 0 if reco < minimum / 2 else reco
    return reco


# Generalized to UK/DE 2026-09-20 at the user's request ("same reco" for
# de/uk). Each region's own sales-marketplace scope: USA folds in Canada/
# Mexico (same convention used everywhere else in this app sales are broken
# out by marketplace - see CLAUDE.md); UK and DE are each their own single
# marketplace, not folded with any other EU country or the "eu" aggregate.
REGION_RECO_CONFIGS = {
    "usa": {"balance": "usa_balance", "otw": "usa_on_the_way", "next": "usa_next_shipment", "sales_marketplaces": ("usa", "ca", "mex")},
    "uk": {"balance": "uk_balance", "otw": "uk_on_the_way", "next": "uk_next_shipment", "sales_marketplaces": ("uk",)},
    "de": {"balance": "de_balance", "otw": "de_on_the_way", "next": "de_next_shipment", "sales_marketplaces": ("de",)},
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


def fetch_all_sales_by_asin_month(token, marketplaces):
    """Every sku_sales row across the given marketplaces, ALL years, summed
    per (asin, year, month) - {asin: {(year, month): qty}}. A single fetch
    per region (only ~5-6k rows total for this app's whole USA-market
    history, less for a single EU country) backs both recommendation
    formulas below: the recent one needs the trailing 3 months, the
    seasonal one needs the same 3 calendar months from 1-2 years ago - both
    are just different (year, month) slices of the same data, so one broad
    fetch is simpler and cheaper than fetching each window separately.
    sku_sales rows already carry the real ASIN a sale was recorded under
    regardless of which SKU spelling was used, so this sums correctly
    across e.g. a relisted SKU's old and new spellings without needing the
    mapping table at all."""
    marketplace_clause = " || ".join(f'marketplace = "{mkt}"' for mkt in marketplaces)

    totals = defaultdict(lambda: defaultdict(int))
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SALES_COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": marketplace_clause, "perPage": 500, "page": page, "fields": "ASIN,quantity,year,month"},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        for row in data.get("items", []):
            asin = row.get("ASIN")
            if not asin:
                continue
            totals[asin][(row.get("year"), row.get("month"))] += int(row.get("quantity") or 0)
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return totals


def fetch_zero_inventory_week_counts_by_asin(token, since_date_str, region):
    """{asin: count of distinct weekly snapshots (sku_inventory_history,
    written every Monday by WeeklyUsaInventorySync.py/WeeklyEuInventorySync.py)
    with balance <= 0 for the given region, on/after since_date_str} - used
    to exclude stockout weeks from the recent sales-velocity denominator
    (see compute_region_recommendations). USA history started 2026-09-20,
    UK/DE the same day - all return all-zero counts (no adjustment) until
    enough weekly history accumulates, expected and fine, not a bug."""
    counts = defaultdict(set)
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_INVENTORY_HISTORY_COLLECTION}/records",
            headers={"Authorization": token},
            params={
                "filter": f'region = "{region}" && week_date >= "{since_date_str}" && balance <= 0',
                "perPage": 500,
                "page": page,
                "fields": "asin,week_date",
            },
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        for row in data.get("items", []):
            asin = row.get("asin")
            if asin:
                counts[asin].add(row.get("week_date"))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return {asin: len(weeks) for asin, weeks in counts.items()}


def days_in_months(months):
    """Exact real day count spanning the given (year, month) pairs (e.g.
    Jun+Jul+Aug = 30+31+31 = 92), used as the denominator for a real
    average-per-day rate instead of a flat 30-day approximation."""
    return sum(calendar.monthrange(y, m)[1] for y, m in months)


def shipment_window_months(shipment_date, years_back=0):
    """The 3 calendar months starting at shipment_date's own month, shifted
    back by years_back full years - e.g. shipment_date=2027-12-01,
    years_back=1 -> [(2026,12), (2027,1), (2027,2)] (Dec+Jan+Feb one year
    earlier: the actual season this shipment needs to cover once it lands,
    as it played out last time that season happened)."""
    year = shipment_date.year - years_back
    month = shipment_date.month
    months = []
    for i in range(3):
        m = month + i
        y = year + (m - 1) // 12
        mm = ((m - 1) % 12) + 1
        months.append((y, mm))
    return months


def sum_window(asin_sales, months):
    return sum(asin_sales.get(ym, 0) for ym in months)


def compute_region_recommendations(asin_sales, already_covered, next_shipment_date, today, trailing_months, lookback_days, year1_months, year2_months, zero_inventory_weeks=0):
    """Generalized across USA/UK/DE (originally USA-only, generalized
    2026-09-20 at the user's request - "same reco" for UK/DE). Two
    recommendations, both confirmed with the user (2026-09-19), both in the
    same "how many MORE units to still add to <region> Next" shape as the
    page's existing Missing column (0 = already covered):

    Reco 1 ("recent"): assumes the next 3 months look like the last 3.
        x = days from today to next_shipment_date
        need_for_x_days = x * (recent 3-month total / real SELLABLE days in
            those months - see zero_inventory_weeks below)
        need_for_3_months = the recent 3-month total, used as-is
        reco = need_for_3_months + need_for_x_days - already_covered (Bal + OTW + Next, all for this region)

    Reco 2 ("seasonal"): a seasonal product's next-3-months can look nothing
    like its last-3-months (e.g. shipping into summer from a winter
    baseline), so instead of the recent total it uses the ACTUAL same
    3-calendar-month window the shipment lands into, from 1 and 2 years ago
    (averaged - smooths out a one-off spike/dip in either single year), and
    keeps the same near-term need_for_x_days term as Reco 1 (that portion is
    the gap before the shipment even arrives, not the season it lands in).
    Falls back to only whichever of the two years has real history if the
    other doesn't (e.g. a newer SKU), or to Reco 1's own number if neither
    year has any - seasonal_source says which case applied.

    zero_inventory_weeks (per the user, 2026-09-20): count of weekly
    snapshots (sku_inventory_history) within the trailing window where this
    SKU had zero stock in this region. A stockout week can't produce real sales no
    matter the true demand, so counting those days as normal sales days
    would understate velocity - each such week is dropped from the
    denominator entirely (both the days AND, implicitly, its ~0 sales).
    Only ever non-zero once enough weekly history has accumulated (this
    tracking started 2026-09-20), and short/sub-week stockouts aren't
    detectable at all with weekly-only snapshots - both known, accepted
    limitations, not bugs."""
    trailing_total = sum_window(asin_sales, trailing_months)
    excluded_days = zero_inventory_weeks * 7
    effective_lookback_days = lookback_days - excluded_days
    if effective_lookback_days <= 0:
        effective_lookback_days = lookback_days  # fully-excluded edge case: fall back rather than divide by ~0
    avg_per_day = (trailing_total / effective_lookback_days) if effective_lookback_days else 0

    x_days = max(0, (next_shipment_date - today).days)
    need_for_x_days = x_days * avg_per_day

    reco_recent = max(0, round(trailing_total + need_for_x_days - already_covered))

    year1_total = sum_window(asin_sales, year1_months)
    year2_total = sum_window(asin_sales, year2_months)
    year1_days = days_in_months(year1_months)
    year2_days = days_in_months(year2_months)
    earliest = min(asin_sales.keys()) if asin_sales else None
    has_year1 = earliest is not None and earliest <= year1_months[0]
    has_year2 = earliest is not None and earliest <= year2_months[0]

    if has_year1 and has_year2:
        seasonal_3mo = (year1_total + year2_total) / 2
        seasonal_lookback_days = (year1_days + year2_days) / 2
        seasonal_source = "2yr_avg"
    elif has_year1:
        seasonal_3mo = year1_total
        seasonal_lookback_days = year1_days
        seasonal_source = "1yr_only"
    elif has_year2:
        seasonal_3mo = year2_total
        seasonal_lookback_days = year2_days
        seasonal_source = "2yr_only"
    else:
        seasonal_3mo = trailing_total
        seasonal_lookback_days = lookback_days
        seasonal_source = "fallback_recent"

    avg_per_day_seasonal = (seasonal_3mo / seasonal_lookback_days) if seasonal_lookback_days else 0
    reco_seasonal = max(0, round(seasonal_3mo + need_for_x_days - already_covered))

    return {
        "avg_monthly": round(trailing_total / SALES_LOOKBACK_MONTHS, 1),
        "reco_recent": reco_recent,
        "reco_seasonal": reco_seasonal,
        "seasonal_source": seasonal_source,
        # Raw intermediates, exposed so the frontend can render an always-
        # accurate worked-example breakdown (and the per-item hover tooltip)
        # without re-deriving any of this.
        "trailing_total": trailing_total,
        "year1_total": year1_total,
        "year2_total": year2_total,
        "x_days": x_days,
        "need_for_x_days": round(need_for_x_days, 1),
        "already_covered": already_covered,
        "avg_daily_recent": round(avg_per_day, 2),
        "avg_daily_seasonal": round(avg_per_day_seasonal, 2),
        "zero_inventory_weeks": zero_inventory_weeks,
        "effective_lookback_days": effective_lookback_days,
    }


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

        trailing_months = trailing_completed_months(SALES_LOOKBACK_MONTHS, today)
        lookback_days = days_in_months(trailing_months)
        year1_months = shipment_window_months(next_shipment_date, 1)
        year2_months = shipment_window_months(next_shipment_date, 2)
        trailing_window_start = f"{trailing_months[0][0]:04d}-{trailing_months[0][1]:02d}-01"

        all_sales_by_region = {
            region: fetch_all_sales_by_asin_month(token, cfg["sales_marketplaces"])
            for region, cfg in REGION_RECO_CONFIGS.items()
        }
        zero_inventory_weeks_by_region = {
            region: fetch_zero_inventory_week_counts_by_asin(token, trailing_window_start, region)
            for region in REGION_RECO_CONFIGS
        }

        groups = defaultdict(list)
        for row in mapping_rows:
            stats = stats_by_sku.get(row["sku"]) or stats_by_asin.get(row["asin"]) or {}
            item = {"sku": row["sku"], "asin": row["asin"], "upc": row["upc"], "supplier_sku": row["supplier_sku"]}
            for field in STATS_FIELDS:
                item[field] = stats.get(field) or 0

            for region, cfg in REGION_RECO_CONFIGS.items():
                asin_sales = all_sales_by_region[region].get(row["asin"], {}) if row["asin"] else {}
                zero_inventory_weeks = zero_inventory_weeks_by_region[region].get(row["asin"], 0) if row["asin"] else 0
                already_covered = (item.get(cfg["balance"]) or 0) + (item.get(cfg["otw"]) or 0) + (item.get(cfg["next"]) or 0)
                reco = compute_region_recommendations(
                    asin_sales, already_covered, next_shipment_date, today,
                    trailing_months, lookback_days, year1_months, year2_months,
                    zero_inventory_weeks,
                )
                item[f"{region}_avg_monthly_sales"] = reco["avg_monthly"]
                item[f"{region}_recommended_order"] = apply_category_min_order(row["sku"], reco["reco_recent"])
                item[f"{region}_recommended_order_seasonal"] = apply_category_min_order(row["sku"], reco["reco_seasonal"])
                item[f"{region}_seasonal_source"] = reco["seasonal_source"]
                item[f"{region}_reco_debug"] = {
                    "trailingTotal": reco["trailing_total"],
                    "year1Total": reco["year1_total"],
                    "year2Total": reco["year2_total"],
                    "xDays": reco["x_days"],
                    "needForXDays": reco["need_for_x_days"],
                    "alreadyCovered": reco["already_covered"],
                    "avgDailyRecent": reco["avg_daily_recent"],
                    "avgDailySeasonal": reco["avg_daily_seasonal"],
                    "zeroInventoryWeeks": reco["zero_inventory_weeks"],
                }

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
            "trailingMonths": trailing_months,
            "trailingLookbackDays": lookback_days,
            "seasonalYear1Months": year1_months,
            "seasonalYear2Months": year2_months,
        })

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
