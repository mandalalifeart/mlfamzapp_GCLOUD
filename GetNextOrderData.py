import time
import calendar
import json
import os
from collections import defaultdict
from datetime import date, timedelta

import requests

from NextOrderSettings import get_next_shipment_dates, get_cover_until_dates, region_cover_months

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
    "au_balance", "au_on_the_way", "au_next_shipment",
    "jp_balance", "jp_on_the_way", "jp_next_shipment",
    "malani_balance", "malani_order", "next_order",
]

# AU (2026-10-20) has FBA only - au_balance is written directly by
# UpdateEuInventory, there are no au_balance_<part> fields.
BALANCE_PARTS = {"usa": ("fba", "awd"), "uk": ("fba", "awd"), "de": ("fba", "awd", "lg"), "au": ("fba",), "jp": ("fba",)}

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
    # Added 2026-10-20 per the user; the page hides AU while AU has no sellable stock.
    "au": {"balance": "au_balance", "otw": "au_on_the_way", "next": "au_next_shipment", "sales_marketplaces": ("au",)},
    # Added 2026-10-30 per the user; collapsed by default on the page. JP
    # stock is entered by hand (no JP SP-API connection yet).
    "jp": {"balance": "jp_balance", "otw": "jp_on_the_way", "next": "jp_next_shipment", "sales_marketplaces": ("jp",)},
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


def load_mapping(token, keep_skus=None):
    # Same GROUP/IGNORE handling as GetSalesDepartmentReport.load_mapping -
    # IGNORE-group SKUs are deliberately excluded products, not candidates to reorder.
    # end_of_life (added 2026-09-22, set via the product page's "EndOfLife
    # List" button) is a SEPARATE exclusion from group - a discontinued
    # product keeps its real group (still shows up in sales reports etc.),
    # it's just flagged as no longer worth reordering, so only THIS page
    # skips it. Excluded by ASIN, not just the one flagged SKU row - the
    # button is set from one specific SKU spelling on the product page, but
    # a product can have multiple mapping rows sharing one ASIN (old/new
    # spellings), and end-of-life is a property of the PRODUCT, not one
    # spelling of it (confirmed live 2026-09-22: without this, Stool260_New
    # got excluded but its sibling spelling Stool260, same ASIN, still
    # showed up).
    # sku_to_asin covers every row (IGNORE and end_of_life included) so an
    # sku_statistics record filed under any SKU spelling - even one
    # belonging to an ignored/retired/end-of-life SKU string - can still be
    # resolved to its ASIN for the join below.
    all_rows = fetch_mapping_records(token)
    end_of_life_asins = {
        (row.get("asin") or "").strip()
        for row in all_rows
        if row.get("end_of_life") and (row.get("asin") or "").strip()
    }

    # keep_skus (2026-11-03, per the user "any product that has positive value
    # in nextxxx field should be exported"): an end-of-life product that still
    # has a positive Next value is shown (and exported), flagged end_of_life.
    keep_skus = keep_skus or set()
    keep_asins = {
        (row.get("asin") or "").strip()
        for row in all_rows
        if (row.get("sku") or "").strip() in keep_skus and (row.get("asin") or "").strip()
    }

    rows = []
    sku_to_asin = {}
    for row in all_rows:
        sku = (row.get("sku") or "").strip()
        asin = (row.get("asin") or "").strip()
        upc = (row.get("ean") or "").strip()
        supplier_sku = (row.get("supplier_sku") or "").strip()
        group = (row.get("group") or "").strip() or "UNGROUPED"
        if not sku:
            continue
        if asin:
            sku_to_asin[sku] = asin
        eol = bool(row.get("end_of_life") or (asin and asin in end_of_life_asins))
        if group == "IGNORE" or (eol and sku not in keep_skus and asin not in keep_asins):
            continue
        rows.append({"sku": sku, "asin": asin, "upc": upc, "supplier_sku": supplier_sku, "group": group, "end_of_life": eol})
    return rows, sku_to_asin


def unique_product_rows(rows, stats_by_sku):
    """One row per product (2026-10-31, per the user: "only unique UPC" on
    /next-order). Several SKU spellings of one product (e.g. CoverPillowPBlue56A
    / CoverPillowPlue56A) share an ASIN/UPC and used to show twice with the
    same stats. Key = UPC, else ASIN; keep the spelling that has its own
    sku_statistics record (that's where Next/Malani edits are stored), then
    the one with a supplier SKU, then the first."""
    best = {}
    order = []
    for row in rows:
        key = row["upc"] or row["asin"] or row["sku"]
        rank = (row["sku"] in stats_by_sku, bool(row["supplier_sku"]))
        if key not in best:
            order.append(key)
            best[key] = (rank, row)
        elif rank > best[key][0]:
            best[key] = (rank, row)
    return [best[k][1] for k in order]


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


def trailing_months_with_current(n, today=None):
    """Same idea as trailing_completed_months, but the window ENDS AT the
    current in-progress month (inclusive) instead of stopping before it -
    e.g. on 2026-09-22 with n=3: [(2026,7), (2026,8), (2026,9)]."""
    today = today or date.today()
    months = []
    year, month = today.year, today.month
    for _ in range(n):
        months.append((year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
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


def days_in_months(months):
    """Exact real day count spanning the given (year, month) pairs (e.g.
    Jun+Jul+Aug = 30+31+31 = 92), used as the denominator for a real
    average-per-day rate instead of a flat 30-day approximation."""
    return sum(calendar.monthrange(y, m)[1] for y, m in months)


def shipment_window_months(shipment_date, years_back=0, count=3):
    """The 3 calendar months starting at shipment_date's own month, shifted
    back by years_back full years - e.g. shipment_date=2027-12-01,
    years_back=1 -> [(2026,12), (2027,1), (2027,2)] (Dec+Jan+Feb one year
    earlier: the actual season this shipment needs to cover once it lands,
    as it played out last time that season happened)."""
    year = shipment_date.year - years_back
    month = shipment_date.month
    months = []
    for i in range(count):
        m = month + i
        y = year + (m - 1) // 12
        mm = ((m - 1) % 12) + 1
        months.append((y, mm))
    return months


def sum_window(asin_sales, months):
    return sum(asin_sales.get(ym, 0) for ym in months)


MIN_IN_STOCK_DAYS = 7
RUNOUT_SALES_SHARE = 0.10
ACACIA_GROUP = "PAREO_ACA"   # Acacia's pareos (ACA + ZEN), as on the page


def window_rate(asin_sales, months, stock_months, covered_months, today):
    """Sales per day over the given calendar months, counting only days the
    product was IN STOCK in the region (per the user 2026-10-03, see
    StockDays.py). Days not covered by Amazon's inventory ledger (older than
    its ~18-month history, or not reported yet) are assumed in stock. The
    current month counts only the days elapsed so far, with its actual units
    (no proration needed - the rate is per in-stock day). Fewer than
    MIN_IN_STOCK_DAYS in-stock days -> all days are used instead (too little
    data). Returns (rate, units, in_stock_days, total_days, all_days_used)."""
    units = in_stock = total = 0
    for (y, m) in months:
        days = today.day if (y, m) == (today.year, today.month) else calendar.monthrange(y, m)[1]
        units += asin_sales.get((y, m), 0)
        cov = min(covered_months.get((y, m), 0), days)
        in_stock += (min(stock_months.get((y, m), 0), cov) if cov else 0) + (days - cov)
        total += days
    all_days = in_stock < MIN_IN_STOCK_DAYS
    denom = total if all_days else in_stock
    return (units / denom if denom else 0.0), units, (total if all_days else in_stock), total, all_days


def months_spanning(start, end):
    """Calendar (year, month) pairs touched by the date range [start, end)."""
    months, y, m = [], start.year, start.month
    last = end - timedelta(days=1)
    while (y, m) <= (last.year, last.month):
        months.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def _shift_years(d, years):
    try:
        return d.replace(year=d.year - years)
    except ValueError:   # 29 Feb
        return d.replace(year=d.year - years, day=28)


def compute_region_recommendations(asin_sales, already_covered, next_shipment_date, today, trailing_months,
                                   year1_months, year2_months, stock_months, covered_months, pb_count=3):
    """Next shipment, per the user (2026-10-03):
        PA = today -> shipment arrival (x days); PB = the 3 months after arrival
        QA = pieces needed during PA, QB = pieces needed during PB, H = Bal + OTW
        at arrival we have max(H - QA, 0)   (running out early is lost sales,
                                              not something to refill - no over-ordering)
        next shipment = QB - max(H - QA, 0)   (floored at 0)
    Three demand forecasts, every rate counting only in-stock days (window_rate):
        A (recent):     QA = recent rate x days(PA),               QB = recent rate x days(PB)
        B (1 year ago): QA = rate in PA's dates 1 year ago x days(PA), QB = rate in PB's months 1 year ago x days(PB)
        C (2 years ago): same with 2 years ago
    B/C use the recent rate for a window with no sales history that far back
    (seasonal_*_source = "fallback_recent"). The Next column is not counted."""
    rate_recent, units_recent, stock_recent, total_recent, all_recent = window_rate(
        asin_sales, trailing_months, stock_months, covered_months, today)
    x_days = max(0, (next_shipment_date - today).days)
    pb_months = shipment_window_months(next_shipment_date, 0, pb_count)
    pb_days = days_in_months(pb_months)
    earliest = min(asin_sales.keys()) if asin_sales else None
    h = already_covered

    def variant(rate_a, rate_b):
        qa, qb = rate_a * x_days, rate_b * pb_days
        left = max(h - qa, 0)
        return {"qa": round(qa, 1), "qb": round(qb, 1), "left": round(left, 1), "reco": max(0, round(qb - left))}

    out = {"recent": variant(rate_recent, rate_recent)}
    windows = {}
    for key, years, pb_window in (("1yr", 1, year1_months), ("2yr", 2, year2_months)):
        pa_months = months_spanning(_shift_years(today, years), _shift_years(next_shipment_date, years)) if x_days else []
        res = {}
        for name, months in (("pa", pa_months), ("pb", pb_window)):
            if months and earliest is not None and earliest <= months[0]:
                rate, units, stock, total, all_days = window_rate(asin_sales, months, stock_months, covered_months, today)
                res[name] = {"rate": rate, "units": units, "inStock": stock, "total": total, "allDays": all_days,
                             "months": months, "source": "actual"}
            else:
                res[name] = {"rate": rate_recent, "units": 0, "inStock": 0, "total": 0, "allDays": False,
                             "months": months, "source": "fallback_recent"}
        windows[key] = res
        out[key] = variant(res["pa"]["rate"], res["pb"]["rate"])

    def win_debug(w):
        return {"rate": round(w["rate"], 2), "units": w["units"], "inStockDays": w["inStock"], "totalDays": w["total"],
                "allDaysUsed": w["allDays"], "source": w["source"],
                "months": [f"{y:04d}-{m:02d}" for y, m in w["months"]]}

    return {
        "avg_monthly": round(rate_recent * 30.4, 1),
        "reco_recent": out["recent"]["reco"],
        "reco_seasonal_1yr": out["1yr"]["reco"],
        "reco_seasonal_2yr": out["2yr"]["reco"],
        "seasonal_1yr_source": windows["1yr"]["pb"]["source"],
        "seasonal_2yr_source": windows["2yr"]["pb"]["source"],
        "debug": {
            "xDays": x_days, "pbDays": pb_days, "have": h, "alreadyCovered": h,
            "recent": {"rate": round(rate_recent, 2), "units": units_recent, "inStockDays": stock_recent,
                       "totalDays": total_recent, "allDaysUsed": all_recent, "source": "actual",
                       "months": [f"{y:04d}-{m:02d}" for y, m in trailing_months]},
            "year1": {"pa": win_debug(windows["1yr"]["pa"]), "pb": win_debug(windows["1yr"]["pb"])},
            "year2": {"pa": win_debug(windows["2yr"]["pa"]), "pb": win_debug(windows["2yr"]["pb"])},
            "A": out["recent"], "B": out["1yr"], "C": out["2yr"],
            # daily rates used for Days of Supply (A recent, B/C = season after arrival)
            "avgDailyRecent": round(rate_recent, 2),
            "avgDailySeasonal1yr": round(windows["1yr"]["pb"]["rate"], 2),
            "avgDailySeasonal2yr": round(windows["2yr"]["pb"]["rate"], 2),
        },
    }


# Ads countries per Next Order region - same scope as each region's sales
# marketplaces in REGION_RECO_CONFIGS (USA folds in CA/MX; DE and UK are
# single marketplaces). Added 2026-10-03 per the user (ACOS per product).
REGION_ADS_COUNTRIES = {"usa": ("US", "CA", "MX"), "de": ("DE",), "uk": ("UK", "GB"), "au": ("AU",), "jp": ("JP",)}
# Only used to put CA/MX spend+sales on the USD scale before taking the
# spend/sales ratio - ACOS itself is unit-free, so an approximate rate only
# shifts the weighting of these tiny marketplaces, never a single-currency ACOS.
APPROX_TO_USD = {"USD": 1.0, "CAD": 0.73, "MXN": 0.055, "EUR": 1.0, "GBP": 1.0, "AUD": 1.0, "JPY": 1.0}


def fetch_ads_acos_by_region_asin(token, year, recent_since):
    """{region: {asin: {"ytdSpend", "ytdSales", "recentSpend", "recentSales"}}}
    from ads_advertised_product_stats (Amazon's own spend/sales7d, i.e. the
    ACOS Seller Central shows). The same campaign can be reported under two
    Ads profiles - one profile is kept (see memory
    amzbot-dedupe-duplicate-profile-campaigns)."""
    rows, page = [], 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/ads_advertised_product_stats/records",
            headers={"Authorization": token},
            params={"filter": f'date >= "{year}-01-01"', "perPage": 500, "page": page,
                    "fields": "asin,sku,country_code,profile_id,campaign_id,ad_group_id,date,spend,sales,orders,"
                              "same_sku_units,has_same_sku,currency_code"},
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        rows.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    best = {}
    for r in rows:
        k = (r.get("campaign_id"), r.get("ad_group_id"), r.get("asin"), r.get("sku"), r.get("date"))
        if k not in best or (r.get("profile_id") or "") < (best[k].get("profile_id") or ""):
            best[k] = r
    out = {region: defaultdict(lambda: {"ytdSpend": 0.0, "ytdSales": 0.0, "recentSpend": 0.0, "recentSales": 0.0,
                                         "ytdUnits": 0, "recentUnits": 0})
           for region in REGION_ADS_COUNTRIES}
    for r in best.values():
        for region, countries in REGION_ADS_COUNTRIES.items():
            if r.get("country_code") in countries and r.get("asin"):
                fx = APPROX_TO_USD.get(r.get("currency_code") or "", 1.0)
                a = out[region][r["asin"]]
                a["ytdSpend"] += (r.get("spend") or 0) * fx
                a["ytdSales"] += (r.get("sales") or 0) * fx
                # units of THIS product sold through ads: exact-SKU units where
                # stored (SP rows since Sep 2026), else Amazon's ad orders
                units = (r.get("same_sku_units") or 0) if r.get("has_same_sku") else (r.get("orders") or 0)
                a["ytdUnits"] += units
                if (r.get("date") or "") >= recent_since:
                    a["recentSpend"] += (r.get("spend") or 0) * fx
                    a["recentSales"] += (r.get("sales") or 0) * fx
                    a["recentUnits"] += units
    return out


REGION_TRAFFIC_MARKETPLACES = {"usa": ("us", "ca", "mx"), "de": ("de",), "uk": ("uk",), "au": ("au",), "jp": ("jp",)}


def fetch_total_sales_by_region_asin(token, year):
    """{region: {asin: total sales this year}} from asin_traffic_monthly
    (Business Reports ordered product sales, all channels - organic + ads),
    the denominator of TACOS (per the user 2026-10-23). CA/MX converted with
    the same approximate rates as the ad spend."""
    out = {region: defaultdict(float) for region in REGION_TRAFFIC_MARKETPLACES}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/asin_traffic_monthly/records",
            headers={"Authorization": token},
            params={"filter": f"year = {year}", "perPage": 500, "page": page,
                    "fields": "asin,marketplace,sales,currency"},
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        for r in data.get("items", []):
            for region, mps in REGION_TRAFFIC_MARKETPLACES.items():
                if r.get("marketplace") in mps and r.get("asin"):
                    out[region][r["asin"]] += (r.get("sales") or 0) * APPROX_TO_USD.get(r.get("currency") or "", 1.0)
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return out


_ADS_CACHE = {}


def cached_ads_and_totals(token, today):
    """Ads ACOS + total sales per region/ASIN, cached 10 minutes in the
    long-running local server (the ads read takes ~14 s)."""
    key = today.isoformat()
    hit = _ADS_CACHE.get(key)
    if hit and time.time() - hit[0] < 600:
        return hit[1], hit[2]
    ads = fetch_ads_acos_by_region_asin(token, today.year, (today - timedelta(days=30)).isoformat())
    totals = fetch_total_sales_by_region_asin(token, today.year)
    _ADS_CACHE.clear()
    _ADS_CACHE[key] = (time.time(), ads, totals)
    return ads, totals


def seasonal_days_left(asin_sales, stock, today, trailing_months, stock_months, covered_months, max_days=730):
    """Pareos only (per the user 2026-11-19): days until the stock runs out,
    selling each coming day at the in-stock rate of the same calendar month
    last year (the last 3 months' rate where there's no history that far
    back)."""
    earliest = min(asin_sales.keys()) if asin_sales else None
    recent = window_rate(asin_sales, trailing_months, stock_months, covered_months, today)[0]
    rates = {}
    left, d = float(stock), 0
    while d < max_days:
        day = today + timedelta(days=d)
        ym = (day.year - 1, day.month)
        if ym not in rates:
            rates[ym] = (window_rate(asin_sales, [ym], stock_months, covered_months, today)[0]
                         if earliest is not None and earliest <= ym else recent)
        left -= rates[ym]
        if left < 0:
            return d
        d += 1
    return max_days


def recommended_shipment_dates(runouts, today):
    recommended_dates = {}
    for region, lst in runouts.items():
        lst.sort()
        total_rate = sum(r for _d, r, _s in lst)
        acc = 0.0
        for i, (days, rate, sku) in enumerate(lst):
            acc += rate
            if acc >= RUNOUT_SALES_SHARE * total_rate:
                when = today + timedelta(days=int(days))
                out = [r for d, r, _s in lst if d <= 0]
                if days <= 0:
                    recommended_dates[region] = {
                        "date": when.isoformat(),
                        "why": (f"Already late: {len(out)} of {len(lst)} selling products are out of stock today "
                                f"(Bal + OTW = 0), {round(sum(out) / total_rate * 100)}% of daily sales "
                                f"(sales of the last 12 months on in-stock days, {round(total_rate, 1)} units/day "
                                f"in total). Ship as soon as possible."),
                    }
                    break
                recommended_dates[region] = {
                    "date": when.isoformat(),
                    "why": (f"{i + 1} of {len(lst)} selling products run out by this date (last one: {sku}); "
                            f"together they are {round(acc / total_rate * 100)}% of daily sales. "
                            + (f"{sum(1 for d, _r, _s in lst if d <= 0)} are already out of stock. "
                               if any(d <= 0 for d, _r, _s in lst) else "")
                            + f"Based on Bal + OTW and sales of the last 12 months on in-stock days "
                            f"({round(total_rate, 1)} units/day in total). Arrive by this date so that no more "
                            f"than {round(RUNOUT_SALES_SHARE * 100)}% of sales are out of stock."),
                }
                break

    # Products behind each date (per the user 2026-11-19 "show me those"):
    # the ones out of stock now and the ones running out by the date.
    for region, rec in recommended_dates.items():
        limit = date.fromisoformat(rec["date"])
        rec["products"] = [
            {"sku": sku, "perMonth": round(rate * 30.4, 1),
             "runOut": (today + timedelta(days=int(d))).isoformat() if d > 0 else None}
            for d, rate, sku in runouts[region] if today + timedelta(days=int(max(d, 0))) <= limit
        ]
    return recommended_dates


def _acos(spend, sales):
    return round(100 * spend / sales, 1) if sales else None


def GetNextOrderData(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    # The page loads in phases (2026-11-19, per the user): "base" = products
    # and stock only (fast), "reco" = recommendations for the shown regions
    # (+ yearly units of every region + the recommended shipment dates),
    # "extra" = ads ACOS/TACOS for an expanded region only. No "parts" = all
    # of it, as before.
    body = request.get_json(silent=True) or {}
    parts = set(body.get("parts") or ["base", "reco", "extra"])
    shown = [r for r in (body.get("regions") or list(REGION_RECO_CONFIGS)) if r in REGION_RECO_CONFIGS]
    try:
        token = pb_authenticate()
        if parts == {"base"}:
            # Warm the ads cache in the background so expanding a region is quick.
            import threading
            threading.Thread(target=lambda: cached_ads_and_totals(token, date.today()), daemon=True).start()
        stats_records = fetch_sku_statistics(token)
        next_positive = {
            rec.get("sku") for rec in stats_records
            if rec.get("sku") and any(
                (k == "next_order" or k.endswith("_next_shipment")) and isinstance(v, (int, float)) and v > 0
                for k, v in rec.items()
            )
        }
        mapping_rows, sku_to_asin = load_mapping(token, keep_skus=next_positive)
        stats_by_sku = {rec.get("sku"): rec for rec in stats_records if rec.get("sku")}

        # Index sku_statistics by ASIN (resolved via the mapping, not PocketBase
        # itself - the collection is still keyed by sku) so a record filed under
        # any SKU spelling sharing that ASIN is found, not just an exact SKU match.
        stats_by_asin = {}
        for rec in stats_records:
            asin = sku_to_asin.get(rec.get("sku") or "")
            if asin and asin not in stats_by_asin:
                stats_by_asin[asin] = rec

        # One arriving date per region (2026-10-18, per the user).
        shipment_dates = get_next_shipment_dates(token)
        region_dates = {r: date.fromisoformat(shipment_dates[r]) for r in REGION_RECO_CONFIGS}
        cover_until = get_cover_until_dates(token)
        pb_count = region_cover_months(token, region_dates)
        year1_by_region = {r: shipment_window_months(d, 1, pb_count[r]) for r, d in region_dates.items()}
        year2_by_region = {r: shipment_window_months(d, 2, pb_count[r]) for r, d in region_dates.items()}
        next_shipment_date_str = shipment_dates["usa"]
        today = date.today()

        trailing_months = trailing_months_with_current(SALES_LOOKBACK_MONTHS, today)
        year1_months = year1_by_region["usa"]
        year2_months = year2_by_region["usa"]

        need_sales = ("reco" in parts) or ("extra" in parts)
        all_sales_by_region = {
            region: fetch_all_sales_by_asin_month(token, cfg["sales_marketplaces"]) if need_sales else {}
            for region, cfg in REGION_RECO_CONFIGS.items()
        }
        if "reco" in parts:
            from StockDays import load_stock_days
            stock_days, covered_days = load_stock_days(token)

        sales_years = [today.year, today.year - 1, today.year - 2]
        if "extra" in parts:
            ads_by_region, total_sales_by_region = cached_ads_and_totals(token, today)

        # Recommended next-shipment arrival date per region (2026-11-19, the
        # user's option A, 12-month rate): each product's run-out date = today
        # + (Bal + OTW) / its daily sales over the last 12 months (in-stock
        # days only); taking products in run-out order, the recommended date
        # is when the ones that make up RUNOUT_SALES_SHARE of the region's
        # daily sales have run out.
        months_12 = trailing_months_with_current(12, today)
        runouts = defaultdict(list)
        supplier_runouts = {"malani": defaultdict(list), "acacia": defaultdict(list)}

        groups = defaultdict(list)
        for row in unique_product_rows(mapping_rows, stats_by_sku):
            stats = stats_by_sku.get(row["sku"]) or stats_by_asin.get(row["asin"]) or {}
            item = {"sku": row["sku"], "asin": row["asin"], "upc": row["upc"], "supplier_sku": row["supplier_sku"], "end_of_life": row.get("end_of_life", False)}
            for field in STATS_FIELDS:
                item[field] = stats.get(field) or 0
            # Balance = FBA + AWD (+ LG warehouse for DE), per the user
            # 2026-10-03 - rebuilt from the parts so a stale combined field
            # can't disagree; parts are returned for the hover breakdown.
            for region, bparts in BALANCE_PARTS.items():
                values = {p: stats.get(f"{region}_balance_{p}") or 0 for p in bparts}
                if any(values.values()):
                    item[f"{region}_balance"] = sum(values.values())
                item[f"{region}_balance_parts"] = values

            for region, cfg in REGION_RECO_CONFIGS.items():
                if "reco" not in parts and "extra" not in parts:
                    break
                asin_sales = all_sales_by_region[region].get(row["asin"], {}) if row["asin"] else {}
                # Reco ignores the Next column (per the user 2026-10-03): Bal + OTW only.
                already_covered = (item.get(cfg["balance"]) or 0) + (item.get(cfg["otw"]) or 0)
                if "reco" in parts:
                    if asin_sales and not row.get("end_of_life"):
                        rate12 = window_rate(asin_sales, months_12, stock_days[region].get(row["asin"], {}),
                                             covered_days[region], today)[0]
                        if rate12 > 0:
                            days_left = already_covered / rate12
                            if row["group"].startswith("PAREO"):
                                days_left = seasonal_days_left(asin_sales, already_covered, today, trailing_months,
                                                               stock_days[region].get(row["asin"], {}),
                                                               covered_days[region])
                            entry = (days_left, rate12, row["sku"])
                            runouts[region].append(entry)
                            # Acacia (PAREO_ACA/ZEN, shipped once a year) vs
                            # Malani - per the user 2026-11-19 each supplier
                            # filter gets its own date.
                            supplier_runouts["acacia" if row["group"] == ACACIA_GROUP else "malani"][region].append(entry)
                    # Per the user (2026-10-03): yearly units (this year to date,
                    # last year, 2 years ago) - every region (GENERAL sums them).
                    item[f"{region}_yearly_sales"] = [
                        sum(q for (y, _m), q in asin_sales.items() if y == yr) for yr in sales_years
                    ]
                if region not in shown:
                    continue
                if "extra" in parts:
                    ads = ads_by_region[region].get(row["asin"]) if row["asin"] else None
                    item[f"{region}_ads"] = {
                        "acosYtd": _acos(ads["ytdSpend"], ads["ytdSales"]),
                        "spendYtd": round(ads["ytdSpend"], 2), "salesYtd": round(ads["ytdSales"], 2),
                        "acos30d": _acos(ads["recentSpend"], ads["recentSales"]),
                        "spend30d": round(ads["recentSpend"], 2), "sales30d": round(ads["recentSales"], 2),
                        "unitsYtd": ads["ytdUnits"], "units30d": ads["recentUnits"],
                    } if ads else None
                    # TACOS = ad spend / ALL sales of the product this year (per the user 2026-10-23)
                    total_sales = total_sales_by_region[region].get(row["asin"], 0.0) if row["asin"] else 0.0
                    item[f"{region}_tacos"] = {
                        "tacosYtd": _acos(ads["ytdSpend"] if ads else 0.0, total_sales),
                        "spendYtd": round(ads["ytdSpend"], 2) if ads else 0.0,
                        "totalSalesYtd": round(total_sales, 2),
                    }
                if "reco" not in parts:
                    continue
                reco = compute_region_recommendations(
                    asin_sales, already_covered, region_dates[region], today, trailing_months,
                    year1_by_region[region], year2_by_region[region],
                    stock_days[region].get(row["asin"], {}) if row["asin"] else {}, covered_days[region],
                    pb_count[region],
                )
                item[f"{region}_avg_monthly_sales"] = reco["avg_monthly"]
                item[f"{region}_recommended_order"] = apply_category_min_order(row["sku"], reco["reco_recent"])
                item[f"{region}_recommended_order_seasonal_1yr"] = apply_category_min_order(row["sku"], reco["reco_seasonal_1yr"])
                item[f"{region}_recommended_order_seasonal_2yr"] = apply_category_min_order(row["sku"], reco["reco_seasonal_2yr"])
                item[f"{region}_seasonal_1yr_source"] = reco["seasonal_1yr_source"]
                item[f"{region}_seasonal_2yr_source"] = reco["seasonal_2yr_source"]
                item[f"{region}_reco_debug"] = reco["debug"]

            groups[row["group"]].append(item)

        recommended_dates = recommended_shipment_dates(runouts, today)
        recommended_by_supplier = {k: recommended_shipment_dates(v, today) for k, v in supplier_runouts.items()}

        group_list = []
        for group, items in groups.items():
            items.sort(key=lambda i: i["sku"])
            group_list.append({"group": group, "items": items})
        group_list.sort(key=lambda g: g["group"])

        if "base" not in parts:
            # Later phases: only the computed fields per SKU, merged by the page.
            skip = set(STATS_FIELDS) | {"sku", "asin", "upc", "supplier_sku", "end_of_life"}
            items = {}
            for g in group_list:
                for it in g["items"]:
                    items[it["sku"]] = {k: v for k, v in it.items()
                                        if k not in skip and not k.endswith("_balance") and not k.endswith("_balance_parts")}
            return json_response({"status": "success", "items": items,
                                  "recommendedShipmentDates": recommended_dates,
                                  "recommendedShipmentDatesBySupplier": recommended_by_supplier,
                                  "salesYears": sales_years})

        return json_response({
            "status": "success",
            "groups": group_list,
            "nextShipmentDate": next_shipment_date_str,
            "nextShipmentDates": shipment_dates,
            "coverUntil": cover_until,
            "coverMonths": pb_count,
            "seasonalYear1MonthsByRegion": year1_by_region,
            "seasonalYear2MonthsByRegion": year2_by_region,
            "salesLookbackMonths": SALES_LOOKBACK_MONTHS,
            "trailingMonths": trailing_months,
            "trailingLookbackDays": days_in_months(trailing_months),
            "seasonalYear1Months": year1_months,
            "seasonalYear2Months": year2_months,
            "salesYears": sales_years,
            "recommendedShipmentDates": recommended_dates,
            "recommendedShipmentDatesBySupplier": recommended_by_supplier,
        })

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
