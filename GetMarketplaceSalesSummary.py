import json
import os
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_COUNTRY_COLLECTION = os.environ.get("POCKETBASE_COUNTRY_COLLECTION", "country_sales")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
LA_TZ = ZoneInfo("America/Los_Angeles")

# Same UI-marketplace-to-atomic-marketplace mapping as GetSalesDepartmentReport.py.
# Kept in sync manually - each Cloud Function file here is deployed independently.
EU_ATOMIC = {"de", "fr", "it", "es", "se", "be", "pl", "nl", "ie"}
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
    "eu": EU_ATOMIC,
}
ALL_UI_MARKETPLACES = ["usa", "eu", "uk", "de", "fr", "es", "it", "se", "nl", "be", "ie", "pl", "jp", "au"]


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


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


def fetch_country_sales(token, min_year, atomic_marketplaces):
    if not atomic_marketplaces:
        return []

    records = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_COUNTRY_COLLECTION}/records",
            headers={"Authorization": token},
            params={
                "filter": build_pb_filter(min_year, atomic_marketplaces),
                "fields": "marketplace,quantity,sales,month,year",
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


def build_summary(records, atomic_marketplaces, years, current_month):
    quantity_year_months = defaultdict(lambda: [0] * 12)
    sales_year_months = defaultdict(lambda: [0.0] * 12)

    for rec in records:
        if rec.get("marketplace") not in atomic_marketplaces:
            continue
        year = int(rec.get("year") or 0)
        if year not in years:
            continue
        month = int(rec.get("month") or 0)
        if not 1 <= month <= 12:
            continue
        quantity_year_months[year][month - 1] += rec.get("quantity") or 0
        sales_year_months[year][month - 1] += rec.get("sales") or 0

    # Comparing a full prior year against a current year that's still in progress
    # skews the % low, so growth is measured only over the months already
    # completed this year - same rule GetSalesDepartmentReport.py uses.
    completed_months = max(current_month - 1, 0)

    def make_metric(year_months, round_decimals=None):
        year_rows = []
        for year in years:
            months = year_months.get(year, [0] * 12)
            if round_decimals is not None:
                months = [round(v, round_decimals) for v in months]
            year_rows.append({"year": year, "months": months, "total": round(sum(months), round_decimals) if round_decimals is not None else sum(months)})

        this_year_partial = sum(year_rows[0]["months"][:completed_months])
        last_year_partial = sum(year_rows[1]["months"][:completed_months]) if len(year_rows) > 1 else 0
        growth_pct = (
            round((this_year_partial - last_year_partial) / last_year_partial * 100, 1)
            if last_year_partial > 0
            else None
        )
        return {"yearRows": year_rows, "growthPct": growth_pct}

    return {
        "quantity": make_metric(quantity_year_months),
        "sales": make_metric(sales_year_months, round_decimals=2),
    }


def GetMarketplaceSalesSummary(request):
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

        token = pb_authenticate()
        records = fetch_country_sales(token, min_year=years[-1], atomic_marketplaces=atomic_marketplaces)
        summary = build_summary(records, atomic_marketplaces, years, current_month)

        return json_response({
            "status": "success",
            "years": years,
            "currentMonth": current_month,
            "marketplaces": selected_marketplaces or ALL_UI_MARKETPLACES,
            "quantity": summary["quantity"],
            "sales": summary["sales"],
        })

    except PermissionError as exc:
        return json_response({"error": str(exc)}, 403)
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
