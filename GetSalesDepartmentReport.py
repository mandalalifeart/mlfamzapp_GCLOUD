import csv
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests


SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "SKU SALES")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

MAPPING_CSV_PATH = os.environ.get(
    "MAPPING_CSV_PATH",
    os.path.join(os.path.dirname(__file__), "sku_asin_department.csv"),
)

VALID_DEPARTMENTS = {"PAREO", "P_RUG", "P_BOHO"}
YEARS = [2023, 2024, 2025, 2026]
ALLOWED_REGIONS = {
    "all", "eu", "usa", "ca", "mx", "uk", "de", "fr", "it", "es",
    "se", "ie", "pl", "nl", "be", "jp"
}
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


def normalize_asin(asin):
    return safe_strip(asin).upper()


def normalize_sku_basic(sku):
    sku = safe_strip(sku)
    if sku.startswith("amzn.gr."):
        sku = sku[len("amzn.gr."):]
        sku = sku.split("-", 1)[0]
    return sku.lower()


def normalize_sku_candidates(sku):
    """
    Build several normalized variants so DB rows and CSV rows have a much better chance to match.
    """
    raw = safe_strip(sku)
    if not raw:
        return []

    variants = set()

    # original cleaned
    variants.add(raw)
    variants.add(raw.lower())

    # amzn.gr prefix handling
    if raw.startswith("amzn.gr."):
        stripped = raw[len("amzn.gr."):]
        variants.add(stripped)
        variants.add(stripped.lower())
        stripped_first = stripped.split("-", 1)[0]
        variants.add(stripped_first)
        variants.add(stripped_first.lower())

    # split on first dash
    first_dash = raw.split("-", 1)[0]
    variants.add(first_dash)
    variants.add(first_dash.lower())

    # normalized basic
    variants.add(normalize_sku_basic(raw))

    # remove spaces around and all spaces version
    no_spaces = raw.replace(" ", "")
    variants.add(no_spaces)
    variants.add(no_spaces.lower())

    return [v for v in variants if v]


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def supabase_table_url():
    return f"{SUPABASE_URL}/rest/v1/{requests.utils.quote(SUPABASE_TABLE, safe='')}"


def load_mapping(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Mapping CSV not found: {csv_path}")

    sku_to_meta = {}
    asin_to_skus = {}
    stats = {
        "totalRows": 0,
        "ignoredRows": 0,
        "keptRows": 0,
        "duplicateSkuKeys": 0,
    }

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required = {"SKU", "ASIN", "Department"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"CSV must contain these columns: SKU, ASIN, Department. "
                f"Found: {reader.fieldnames}"
            )

        for row in reader:
            stats["totalRows"] += 1

            raw_sku = row.get("SKU", "")
            asin = normalize_asin(row.get("ASIN", ""))
            department = safe_strip(row.get("Department", "")).upper()

            if not raw_sku or not asin or not department:
                stats["ignoredRows"] += 1
                continue

            if department == "IGNORE":
                stats["ignoredRows"] += 1
                continue

            if department not in VALID_DEPARTMENTS:
                stats["ignoredRows"] += 1
                continue

            raw_sku_clean = safe_strip(raw_sku)
            asin_to_skus.setdefault(asin, set()).add(raw_sku_clean)

            for candidate in normalize_sku_candidates(raw_sku_clean):
                if candidate in sku_to_meta:
                    stats["duplicateSkuKeys"] += 1
                sku_to_meta[candidate] = {
                    "asin": asin,
                    "department": department,
                    "canonicalSku": raw_sku_clean,
                }

            stats["keptRows"] += 1

    return sku_to_meta, asin_to_skus, stats


def csv_quote(value):
    text = str(value).replace('"', '""')
    return f'"{text}"'


def build_sales_query_params(region, offset, limit, sku_filter_list=None):
    params = {
        "select": "SKU,MARKETPLACE,MONTH,YEAR,QUANTITY",
        "YEAR": f"in.({','.join(str(y) for y in YEARS)})",
        "order": "YEAR.asc,MONTH.asc,MARKETPLACE.asc,SKU.asc",
        "limit": str(limit),
        "offset": str(offset),
    }

    region_norm = safe_strip(region).lower() or "all"

    if region_norm == "all":
        params["MARKETPLACE"] = "not.eq.eu"
    elif region_norm == "eu":
        params["MARKETPLACE"] = "eq.eu"
    else:
        params["MARKETPLACE"] = f"eq.{region_norm}"

    if sku_filter_list:
        # exact match against raw DB SKU values
        params["SKU"] = f"in.({','.join(csv_quote(s) for s in sku_filter_list)})"

    return params


def fetch_sales_rows(region="all", sku_filter_list=None, page_size=5000, timeout=180):
    all_rows = []
    offset = 0

    while True:
        params = build_sales_query_params(region, offset, page_size, sku_filter_list=sku_filter_list)
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


def aggregate_sales_rows(sales_rows, sku_to_meta, asin_filter=None):
    departments = {}
    missing_skus = set()
    asin_filter_norm = normalize_asin(asin_filter) if asin_filter else ""

    def ensure_department(department_name):
        if department_name not in departments:
            departments[department_name] = {
                "department": department_name,
                "total": {
                    "skuSet": set(),
                    "yearTotals": empty_year_totals(),
                    "monthTotals": empty_month_totals(),
                },
                "asins": {},
            }
        return departments[department_name]

    matched_rows = 0

    for row in sales_rows:
        raw_sku = safe_strip(row.get("SKU", ""))
        if not raw_sku:
            continue

        meta = None
        for candidate in normalize_sku_candidates(raw_sku):
            meta = sku_to_meta.get(candidate)
            if meta:
                break

        if not meta:
            missing_skus.add(raw_sku)
            continue

        department = meta["department"]
        asin = meta["asin"]

        if asin_filter_norm and asin != asin_filter_norm:
            continue

        year = int(row.get("YEAR") or 0)
        month = int(row.get("MONTH") or 0)
        qty = int(float(row.get("QUANTITY") or 0))

        if year not in YEARS or month < 1 or month > 12 or qty == 0:
            continue

        matched_rows += 1
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

        asin_bucket["skuSet"].add(meta["canonicalSku"])
        asin_bucket["yearTotals"][year_key] += qty
        asin_bucket["monthTotals"][month_key] += qty

        dept_bucket["total"]["skuSet"].add(meta["canonicalSku"])
        dept_bucket["total"]["yearTotals"][year_key] += qty
        dept_bucket["total"]["monthTotals"][month_key] += qty

    return departments, sorted(missing_skus)[:50], matched_rows


def build_department_rows(aggregated_departments):
    now_year = datetime.now(LA_TZ).year
    last_year = now_year - 1
    if last_year not in YEARS:
        last_year = max(YEARS)

    result = {}

    for department_name in ["PAREO", "P_RUG", "P_BOHO"]:
        dept_data = aggregated_departments.get(department_name)
        if not dept_data:
            result[department_name] = []
            continue

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

            for year in YEARS:
                for month in range(1, 13):
                    month_key = f"{year}-{month:02d}"
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
        }

        for year in YEARS:
            for month in range(1, 13):
                month_key = f"{year}-{month:02d}"
                total_row[month_key] = dept_data["total"]["monthTotals"][month_key]

        for row in asin_rows:
            row.pop("_sortLastYear", None)

        result[department_name] = [total_row] + asin_rows

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
        asin = normalize_asin(body.get("asin", "")) if body.get("asin") else ""

        if region not in ALLOWED_REGIONS:
            return json_response({"error": f"Unsupported region: {region}"}, 400)

        sku_to_meta, asin_to_skus, mapping_stats = load_mapping(MAPPING_CSV_PATH)

        sku_filter_list = None
        if asin:
            sku_filter_list = sorted(list(asin_to_skus.get(asin, set())))
            if not sku_filter_list:
                return json_response(
                    {
                        "status": "success",
                        "region": region,
                        "asinFilter": asin,
                        "years": YEARS,
                        "mappingFile": MAPPING_CSV_PATH,
                        "mappingStats": mapping_stats,
                        "mappedSkuCount": 0,
                        "mappedSkus": [],
                        "sourceRowCount": 0,
                        "matchedSalesRows": 0,
                        "missingSkuExamples": [],
                        "departmentSummary": [
                            {"department": "PAREO", "asinCount": 0, "Y2023": 0, "Y2024": 0, "Y2025": 0, "Y2026": 0},
                            {"department": "P_RUG", "asinCount": 0, "Y2023": 0, "Y2024": 0, "Y2025": 0, "Y2026": 0},
                            {"department": "P_BOHO", "asinCount": 0, "Y2023": 0, "Y2024": 0, "Y2025": 0, "Y2026": 0},
                        ],
                        "departments": {
                            "PAREO": [],
                            "P_RUG": [],
                            "P_BOHO": [],
                        },
                    },
                    200,
                )

        sales_rows = fetch_sales_rows(region=region, sku_filter_list=sku_filter_list)
        aggregated, missing_skus, matched_rows = aggregate_sales_rows(
            sales_rows,
            sku_to_meta,
            asin_filter=asin or None,
        )
        department_rows = build_department_rows(aggregated)

        response_body = {
            "status": "success",
            "region": region,
            "asinFilter": asin or None,
            "years": YEARS,
            "mappingFile": MAPPING_CSV_PATH,
            "mappingStats": mapping_stats,
            "mappedSkuCount": len(sku_filter_list) if sku_filter_list else None,
            "mappedSkus": sku_filter_list[:100] if sku_filter_list else None,
            "sourceRowCount": len(sales_rows),
            "matchedSalesRows": matched_rows,
            "missingSkuExamples": missing_skus,
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