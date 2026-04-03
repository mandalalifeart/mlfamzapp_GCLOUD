import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import requests


SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "SKU SALES")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

# Default CSV location:
# Put the file next to this python file, or override with env var MAPPING_CSV_PATH
MAPPING_CSV_PATH = os.environ.get(
    "MAPPING_CSV_PATH",
    os.path.join(os.path.dirname(__file__), "sku_asin_department.csv"),
)

VALID_DEPARTMENTS = {"PAREO", "P_RUG", "P_BOHO"}
YEARS = [2023, 2024, 2025, 2026]
EU_MARKETPLACES = {"de", "fr", "it", "es", "se", "ie", "pl", "nl", "be"}

LA_TZ = ZoneInfo("America/Los_Angeles")


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, x-admin-key",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def validate_admin_key(request):
    if not ADMIN_KEY:
        return
    incoming = request.headers.get("x-admin-key", "")
    if incoming != ADMIN_KEY:
        raise PermissionError("Unauthorized")


def safe_strip(value):
    return value.strip() if isinstance(value, str) else ""


def normalize_sku(sku):
    sku = safe_strip(sku)
    if sku.startswith("amzn.gr."):
        return sku[len("amzn.gr."):].split("-", 1)[0]
    return sku


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def supabase_table_url():
    return f"{SUPABASE_URL}/rest/v1/{requests.utils.quote(SUPABASE_TABLE, safe='')}"


def load_sku_mapping(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Mapping CSV not found: {csv_path}")

    sku_to_meta = {}
    stats = {
        "totalRows": 0,
        "ignoredRows": 0,
        "keptRows": 0,
        "duplicateSkus": 0,
    }

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required = {"SKU", "ASIN", "Department"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"CSV must contain exactly these columns at minimum: SKU, ASIN, Department. "
                f"Found: {reader.fieldnames}"
            )

        for row in reader:
            stats["totalRows"] += 1

            sku = normalize_sku(row.get("SKU", ""))
            asin = safe_strip(row.get("ASIN", ""))
            department = safe_strip(row.get("Department", "")).upper()

            if not sku or not asin or not department:
                stats["ignoredRows"] += 1
                continue

            if department == "IGNORE":
                stats["ignoredRows"] += 1
                continue

            if department not in VALID_DEPARTMENTS:
                stats["ignoredRows"] += 1
                continue

            if sku in sku_to_meta:
                stats["duplicateSkus"] += 1

            sku_to_meta[sku] = {
                "asin": asin,
                "department": department,
            }
            stats["keptRows"] += 1

    return sku_to_meta, stats


def build_sales_query_params(region, offset, limit):
    params = {
        "select": "SKU,MARKETPLACE,MONTH,YEAR,QUANTITY",
        "YEAR": f"in.({','.join(str(y) for y in YEARS)})",
        "order": "YEAR.asc,MONTH.asc,MARKETPLACE.asc,SKU.asc",
        "limit": str(limit),
        "offset": str(offset),
    }

    region_norm = safe_strip(region).lower() or "all"

    if region_norm == "all":
        # Avoid double-counting the synthetic EU bucket when all marketplaces are requested.
        params["MARKETPLACE"] = "not.eq.eu"
    elif region_norm == "eu":
        params["MARKETPLACE"] = "eq.eu"
    else:
        params["MARKETPLACE"] = f"eq.{region_norm}"

    return params


def fetch_sales_rows(region="all", page_size=5000, timeout=180):
    all_rows = []
    offset = 0

    while True:
        params = build_sales_query_params(region, offset, page_size)
        response = requests.get(
            supabase_table_url(),
            headers=supabase_headers(),
            params=params,
            timeout=timeout,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Supabase select failed: HTTP {response.status_code} - {response.text}"
            )

        try:
            rows = response.json()
        except Exception as exc:
            raise RuntimeError("Invalid JSON from Supabase") from exc

        if not isinstance(rows, list):
            raise RuntimeError("Unexpected Supabase response, expected a list")

        all_rows.extend(rows)

        if len(rows) < page_size:
            break

        offset += page_size

    return all_rows


def empty_year_totals():
    return {str(year): 0 for year in YEARS}


def empty_month_totals():
    data = {}
    for year in YEARS:
        for month in range(1, 13):
            data[f"{year}-{month:02d}"] = 0
    return data


def build_month_columns():
    return [f"{year}-{month:02d}" for year in YEARS for month in range(1, 13)]


def aggregate_sales_rows(sales_rows, sku_to_meta):
    departments = {}

    def ensure_department(department_name):
        if department_name not in departments:
            departments[department_name] = {
                "department": department_name,
                "total": {
                    "skuSet": set(),
                    "asin": "ALL",
                    "yearTotals": empty_year_totals(),
                    "monthTotals": empty_month_totals(),
                },
                "asins": {},
            }
        return departments[department_name]

    for row in sales_rows:
        raw_sku = row.get("SKU", "")
        sku = normalize_sku(raw_sku)
        meta = sku_to_meta.get(sku)

        if not meta:
            continue

        department = meta["department"]
        asin = meta["asin"]

        year = int(row.get("YEAR") or 0)
        month = int(row.get("MONTH") or 0)
        qty = int(float(row.get("QUANTITY") or 0))

        if year not in YEARS or month < 1 or month > 12 or qty == 0:
            continue

        dept_bucket = ensure_department(department)

        if asin not in dept_bucket["asins"]:
            dept_bucket["asins"][asin] = {
                "asin": asin,
                "skuSet": set(),
                "yearTotals": empty_year_totals(),
                "monthTotals": empty_month_totals(),
            }

        asin_bucket = dept_bucket["asins"][asin]
        month_key = f"{year}-{month:02d}"
        year_key = str(year)

        asin_bucket["skuSet"].add(sku)
        asin_bucket["yearTotals"][year_key] += qty
        asin_bucket["monthTotals"][month_key] += qty

        dept_bucket["total"]["skuSet"].add(sku)
        dept_bucket["total"]["yearTotals"][year_key] += qty
        dept_bucket["total"]["monthTotals"][month_key] += qty

    return departments


def build_department_rows(aggregated_departments):
    now_year = datetime.now(LA_TZ).year
    last_year = now_year - 1
    if last_year not in YEARS:
        last_year = max(YEARS)

    result = {}
    month_columns = build_month_columns()

    for department_name in sorted(aggregated_departments.keys()):
        dept_data = aggregated_departments[department_name]
        asin_rows = []

        for asin, asin_data in dept_data["asins"].items():
            row = {
                "SKU": ", ".join(sorted(asin_data["skuSet"])),
                "ASIN": asin,
                "Y2023": asin_data["yearTotals"]["2023"],
                "Y2024": asin_data["yearTotals"]["2024"],
                "Y2025": asin_data["yearTotals"]["2025"],
                "Y2026": asin_data["yearTotals"]["2026"],
                "_sortLastYear": asin_data["yearTotals"][str(last_year)],
            }

            for month_key in month_columns:
                row[month_key] = asin_data["monthTotals"][month_key]

            asin_rows.append(row)

        asin_rows.sort(
            key=lambda item: (
                -item["_sortLastYear"],
                -item["Y2026"],
                -item["Y2025"],
                item["ASIN"],
            )
        )

        total_row = {
            "SKU": "ALL",
            "ASIN": "ALL",
            "Y2023": dept_data["total"]["yearTotals"]["2023"],
            "Y2024": dept_data["total"]["yearTotals"]["2024"],
            "Y2025": dept_data["total"]["yearTotals"]["2025"],
            "Y2026": dept_data["total"]["yearTotals"]["2026"],
            "_sortLastYear": dept_data["total"]["yearTotals"][str(last_year)],
        }

        for month_key in month_columns:
            total_row[month_key] = dept_data["total"]["monthTotals"][month_key]

        rows = [total_row] + asin_rows

        for row in rows:
            row.pop("_sortLastYear", None)

        result[department_name] = rows

    return result


def summarize_departments(department_rows):
    summary = []

    for department_name in ["PAREO", "P_RUG", "P_BOHO"]:
        rows = department_rows.get(department_name, [])
        total_row = rows[0] if rows else None

        summary.append({
            "department": department_name,
            "asinCount": max(len(rows) - 1, 0),
            "Y2023": total_row.get("Y2023", 0) if total_row else 0,
            "Y2024": total_row.get("Y2024", 0) if total_row else 0,
            "Y2025": total_row.get("Y2025", 0) if total_row else 0,
            "Y2026": total_row.get("Y2026", 0) if total_row else 0,
        })

    return summary


def GetSalesDepartmentReport(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        validate_admin_key(request)
        body = request.get_json(silent=True) or {}

        region = safe_strip(body.get("region", "all")).lower() or "all"
        if region not in {"all", "eu", "usa", "ca", "mx", "uk", "de", "fr", "it", "es", "se", "ie", "pl", "nl", "be", "jp"}:
            return json_response({"error": f"Unsupported region: {region}"}, 400)

        sku_to_meta, mapping_stats = load_sku_mapping(MAPPING_CSV_PATH)
        sales_rows = fetch_sales_rows(region=region)
        aggregated = aggregate_sales_rows(sales_rows, sku_to_meta)
        department_rows = build_department_rows(aggregated)

        response_body = {
            "status": "success",
            "region": region,
            "years": YEARS,
            "monthColumns": build_month_columns(),
            "mappingFile": MAPPING_CSV_PATH,
            "mappingStats": mapping_stats,
            "sourceRowCount": len(sales_rows),
            "departmentSummary": summarize_departments(department_rows),
            "departments": {
                "PAREO": department_rows.get("PAREO", []),
                "P_RUG": department_rows.get("P_RUG", []),
                "P_BOHO": department_rows.get("P_BOHO", []),
            },
        }

        return json_response(response_body, 200)

    except PermissionError as exc:
        return json_response({"error": str(exc)}, 403)
    except FileNotFoundError as exc:
        return json_response({"error": str(exc)}, 500)
    except ValueError as exc:
        return json_response({"error": str(exc)}, 400)
    except Exception as exc:
        return json_response(
            {"error": str(exc), "type": exc.__class__.__name__},
            500,
        )