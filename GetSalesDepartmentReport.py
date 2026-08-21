import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_SKU_COLLECTION = os.environ.get("POCKETBASE_SKU_COLLECTION", "sku_sales")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
LA_TZ = ZoneInfo("America/Los_Angeles")

MAPPING_CSV_PATH = Path(__file__).with_name("asin_group_mapping.csv")

# "eu" is Amazon's own generic EU marketplace code - the ingest pipeline
# (UpdateSkuSalesMonth) writes it as its own row alongside (not instead of)
# each individual EU country's row, so it must be read as its own bucket here
# too rather than re-summed from the individual countries (which would
# double-count going forward, since the two are written independently).
UI_MARKETPLACE_TO_ATOMIC = {
    "usa": {"usa"},
    "uk": {"uk"},
    "jp": {"jp"},
    "au": {"au"},
    "de": {"de"},
    "fr": {"fr"},
    "es": {"es"},
    "it": {"it"},
    "se": {"se"},
    "nl": {"nl"},
    "be": {"be"},
    "ie": {"ie"},
    "pl": {"pl"},
    "eu": {"eu"},
}
ALL_UI_MARKETPLACES = ["usa", "eu", "uk", "de", "fr", "es", "it", "se", "nl", "be", "ie", "pl", "jp", "au"]

LAST_GROUP = "IGNORE"


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


_mapping_cache = None


def load_mapping():
    global _mapping_cache
    if _mapping_cache is not None:
        return _mapping_cache

    sku_to_asin = {}
    asin_to_main_sku = {}
    asin_to_group = {}

    with open(MAPPING_CSV_PATH, newline="") as f:
        for row in csv.DictReader(f):
            sku = (row.get("SKU") or "").strip()
            asin = (row.get("ASIN") or "").strip()
            group = (row.get("GROUP") or "").strip() or "UNGROUPED"
            if not sku or not asin:
                continue
            sku_to_asin[sku] = asin
            asin_to_main_sku.setdefault(asin, sku)
            asin_to_group.setdefault(asin, group)

    _mapping_cache = {
        "sku_to_asin": sku_to_asin,
        "asin_to_main_sku": asin_to_main_sku,
        "asin_to_group": asin_to_group,
    }
    return _mapping_cache


def expand_marketplaces(selected):
    codes = selected if selected else ALL_UI_MARKETPLACES
    atomic = set()
    for code in codes:
        atomic |= UI_MARKETPLACE_TO_ATOMIC.get(str(code).strip().lower(), set())
    return atomic


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


def build_pb_filter(min_year, atomic_marketplaces):
    marketplace_clause = " || ".join(
        f'marketplace="{mp}"' for mp in sorted(atomic_marketplaces)
    )
    return f"(year>={min_year} && ({marketplace_clause}))"


def fetch_sku_sales(token, min_year, atomic_marketplaces):
    if not atomic_marketplaces:
        return []

    records = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SKU_COLLECTION}/records",
            headers={"Authorization": token},
            params={
                "filter": build_pb_filter(min_year, atomic_marketplaces),
                "fields": "sku,ASIN,marketplace,month,year,quantity",
                "perPage": 500,
                "page": page,
            },
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


def build_report(records, mapping, atomic_marketplaces, years, current_month):
    asin_to_main_sku = mapping["asin_to_main_sku"]
    asin_to_group = mapping["asin_to_group"]

    asin_year_months = defaultdict(lambda: defaultdict(lambda: [0] * 12))
    group_year_months = defaultdict(lambda: defaultdict(lambda: [0] * 12))
    unmapped_totals = defaultdict(int)

    for rec in records:
        if rec.get("marketplace") not in atomic_marketplaces:
            continue
        year = int(rec.get("year") or 0)
        if year not in years:
            continue
        month = int(rec.get("month") or 0)
        if not 1 <= month <= 12:
            continue
        sku = rec.get("sku") or ""
        qty = int(rec.get("quantity") or 0)
        # Group by the sale's own ASIN field (set from the order data), not by
        # matching its SKU string against the mapping CSV - a product can be sold
        # under multiple SKU spellings (e.g. Pareo5Blue / Pareo5Blue502) that all
        # share one ASIN, and the CSV won't list every variant.
        asin = (rec.get("ASIN") or "").strip()
        if not asin or asin not in asin_to_group:
            unmapped_totals[sku] += qty
            continue
        asin_year_months[asin][year][month - 1] += qty

    # Comparing a full prior year against a current year that's still in progress
    # skews the % low (it's missing months that haven't happened yet), so growth
    # is measured only over the months already completed this year.
    completed_months = max(current_month - 1, 0)

    def make_item(asin, year_months):
        year_rows = [
            {"year": year, "months": year_months.get(year, [0] * 12), "total": sum(year_months.get(year, [0] * 12))}
            for year in years
        ]
        this_year_partial = sum(year_rows[0]["months"][:completed_months])
        last_year_partial = sum(year_rows[1]["months"][:completed_months]) if len(year_rows) > 1 else 0
        growth_pct = (
            round((this_year_partial - last_year_partial) / last_year_partial * 100, 1)
            if last_year_partial > 0
            else None
        )
        return {
            "asin": asin,
            "mainSku": asin_to_main_sku.get(asin, ""),
            "years": year_rows,
            "growthPct": growth_pct,
        }

    groups = defaultdict(list)
    covered_asins = set()
    for asin, year_months in asin_year_months.items():
        group = asin_to_group.get(asin, "UNGROUPED")
        groups[group].append(make_item(asin, year_months))
        covered_asins.add(asin)
        for year, months in year_months.items():
            if year not in years:
                continue
            acc = group_year_months[group][year]
            for i, qty in enumerate(months):
                acc[i] += qty

    # Include catalog ASINs with zero sales in this window too, so groups show the full lineup.
    for asin, group in asin_to_group.items():
        if asin in covered_asins:
            continue
        groups[group].append(make_item(asin, {}))

    group_list = []
    for group, items in groups.items():
        items.sort(key=lambda item: item["years"][0]["total"], reverse=True)
        group_summary = make_item(None, group_year_months.get(group, {}))
        group_list.append({
            "group": group,
            "totalThisYear": sum(item["years"][0]["total"] for item in items),
            "yearRows": group_summary["years"],
            "growthPct": group_summary["growthPct"],
            "items": items,
        })
    group_list.sort(key=lambda g: (g["group"] == LAST_GROUP, -g["totalThisYear"]))

    unmapped = [
        {"sku": sku, "totalQuantity": qty}
        for sku, qty in sorted(unmapped_totals.items(), key=lambda kv: kv[1], reverse=True)
    ][:50]

    return group_list, unmapped


def GetSalesDepartmentReport(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        body = request.get_json(silent=True) or {}

        selected_marketplaces = body.get("marketplaces")
        if selected_marketplaces is not None and not isinstance(selected_marketplaces, list):
            return json_response({"error": "marketplaces must be an array"}, 400)

        atomic_marketplaces = expand_marketplaces(selected_marketplaces)

        now = datetime.now(LA_TZ)
        current_year = now.year
        current_month = now.month
        years = [current_year, current_year - 1, current_year - 2, current_year - 3]

        mapping = load_mapping()
        token = pb_authenticate()
        records = fetch_sku_sales(token, min_year=years[-1], atomic_marketplaces=atomic_marketplaces)
        groups, unmapped = build_report(records, mapping, atomic_marketplaces, years, current_month)

        return json_response({
            "status": "success",
            "years": years,
            "currentMonth": current_month,
            "marketplaces": selected_marketplaces or ALL_UI_MARKETPLACES,
            "groups": groups,
            "unmapped": unmapped,
        })

    except PermissionError as exc:
        return json_response({"error": str(exc)}, 403)
    except FileNotFoundError:
        return json_response({"error": "ASIN mapping file (asin_group_mapping.csv) is missing"}, 500)
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
