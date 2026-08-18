import json
import os
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
from xml.etree import ElementTree as ET

import pymysql
import requests

from MlfReport import DB_MARKETPLACE_MAP, EU_MARKETPLACES

API_BASE = os.environ.get("API_BASE", "https://us-central1-mlfamzapp.cloudfunctions.net")
MARIADB_HOST = os.environ["MARIADB_HOST"]
MARIADB_PORT = int(os.environ.get("MARIADB_PORT", "3306"))
MARIADB_USER = os.environ["MARIADB_USER"]
MARIADB_PASSWORD = os.environ["MARIADB_PASSWORD"]
MARIADB_DATABASE = os.environ.get("MARIADB_DATABASE", "bababot")
MARIADB_TABLE = os.environ.get("MARIADB_TABLE", "SKU_SALES")
MARIADB_COUNTRY_TABLE = os.environ.get("MARIADB_COUNTRY_TABLE", "COUNTRY_SALES")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
ALLOWED_ORIGIN = "https://mlfamzappfire.web.app"
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


def safe_strip(value):
    return value.strip() if isinstance(value, str) else ""


def extract_amzn_gr_value(sku):
    if not isinstance(sku, str):
        return sku
    if sku.startswith("amzn.gr."):
        return sku[len("amzn.gr."):].split("-", 1)[0]
    return sku


def should_ignore_sales_channel(sales_channel):
    if not sales_channel:
        return True
    normalized = sales_channel.strip().lower()
    return normalized.startswith("non-amazon") or ("prod" in normalized)


def parse_input_datetime(value):
    if not value:
        raise ValueError("Missing date")

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=LA_TZ)

    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"Invalid date format: {value}") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))

    return dt.astimezone(LA_TZ)


def get_la_month_year(date_value):
    dt_la = parse_input_datetime(date_value)
    return dt_la.month, dt_la.year


def validate_admin_key(request):
    if not ADMIN_KEY:
        return
    incoming = request.headers.get("x-admin-key", "")
    if incoming != ADMIN_KEY:
        raise PermissionError("Unauthorized")


def fetch_report_payload(marketplace, report_req_id, timeout_sec=180):
    if not report_req_id:
        raise ValueError(f"Missing report request ID for marketplace={marketplace}")

    response = requests.post(
        f"{API_BASE}/MlfReportGet",
        json={"marketplace": marketplace, "report_req_id": report_req_id},
        timeout=timeout_sec,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"MlfReportGet failed for {marketplace}: HTTP {response.status_code} - {response.text}"
        )

    try:
        data = response.json()
    except Exception as exc:
        raise RuntimeError(f"Invalid JSON from MlfReportGet for {marketplace}") from exc

    status = data.get("status")
    payload = ((data.get("data") or {}).get("payload"))

    if status in {"IN_PROCESS", "IN_PROGRESS"} or payload in {"IN_PROCESS", "IN_PROGRESS"}:
        raise RuntimeError(f"Report for {marketplace} is still processing")
    if status != "success":
        raise RuntimeError(f"Unexpected MlfReportGet status for {marketplace}: {status}")
    if not isinstance(payload, str) or not payload.strip():
        raise RuntimeError(f"Empty XML payload for {marketplace}")

    return payload


def parse_orders_from_xml(xml_payload):
    try:
        root = ET.fromstring(xml_payload)
    except ET.ParseError as exc:
        raise RuntimeError("Failed to parse XML payload") from exc

    rows = []
    for order in root.findall(".//Order"):
        sales_channel = safe_strip(order.findtext("SalesChannel"))
        if should_ignore_sales_channel(sales_channel):
            continue

        for order_item in order.findall(".//OrderItem"):
            sku = safe_strip(order_item.findtext("SKU"))
            if not sku:
                continue
            sku = extract_amzn_gr_value(sku)

            qty_raw = safe_strip(order_item.findtext("Quantity"))
            try:
                qty = int(float(qty_raw)) if qty_raw else 0
            except ValueError:
                qty = 0

            if qty <= 0:
                continue

            rows.append({
                "sku": sku,
                "qty": qty,
                "sales_channel": sales_channel.lower(),
            })

    return rows


def build_db_rows(order_rows, month, year):
    totals = {}

    def add_row(sku, marketplace_code, qty):
        key = (sku, marketplace_code, month, year)
        if key not in totals:
            totals[key] = {
                "SKU": sku,
                "MARKETPLACE": marketplace_code,
                "MONTH": month,
                "YEAR": year,
                "QUANTITY": 0,
            }
        totals[key]["QUANTITY"] += qty

    for row in order_rows:
        marketplace_code = DB_MARKETPLACE_MAP.get(row["sales_channel"])
        if not marketplace_code:
            continue

        qty = int(row["qty"])
        add_row(row["sku"], marketplace_code, qty)

        if marketplace_code in EU_MARKETPLACES:
            add_row(row["sku"], "eu", qty)

    return sorted(totals.values(), key=lambda item: (item["MARKETPLACE"], item["SKU"]))


def build_country_rows(db_rows, month, year):
    totals = {}

    for row in db_rows:
        marketplace_code = row["MARKETPLACE"]
        if marketplace_code not in totals:
            totals[marketplace_code] = {
                "MARKETPLACE": marketplace_code,
                "MONTH": month,
                "YEAR": year,
                "SKU_COUNT": 0,
                "QUANTITY": 0,
            }
        totals[marketplace_code]["SKU_COUNT"] += 1
        totals[marketplace_code]["QUANTITY"] += int(row["QUANTITY"])

    return sorted(totals.values(), key=lambda item: item["MARKETPLACE"])


def build_dry_run_summary(db_rows):
    by_marketplace = defaultdict(lambda: {"rows": 0, "units": 0, "unique_skus": set()})

    for row in db_rows:
        mp = row["MARKETPLACE"]
        by_marketplace[mp]["rows"] += 1
        by_marketplace[mp]["units"] += int(row["QUANTITY"])
        by_marketplace[mp]["unique_skus"].add(row["SKU"])

    summary = []
    for mp in sorted(by_marketplace.keys()):
        item = by_marketplace[mp]
        summary.append({
            "marketplace": mp,
            "rows": item["rows"],
            "units": item["units"],
            "uniqueSkus": len(item["unique_skus"]),
        })

    return summary


def collect_report_ids_from_body(body):
    """
    Preferred new format:
      "reportIds": {
        "usa": "677669020544",
        "de": "594242020544",
        "uk": "...",
        "fr": "..."
      }

    Backward-compatible old format:
      "usaReportId": "...",
      "deReportId": "..."
    """
    report_ids = {}

    raw = body.get("reportIds")
    if isinstance(raw, dict):
        for marketplace, report_id in raw.items():
            marketplace_norm = safe_strip(marketplace).lower()
            report_id_norm = safe_strip(report_id)
            if marketplace_norm and report_id_norm:
                report_ids[marketplace_norm] = report_id_norm

    legacy_map = {
        "usa": body.get("usaReportId", ""),
        "de": body.get("deReportId", ""),
        "uk": body.get("ukReportId", ""),
        "fr": body.get("frReportId", ""),
        "it": body.get("itReportId", ""),
        "es": body.get("esReportId", ""),
        "nl": body.get("nlReportId", ""),
        "se": body.get("seReportId", ""),
        "pl": body.get("plReportId", ""),
        "be": body.get("beReportId", ""),
        "jp": body.get("jpReportId", ""),
        "ca": body.get("caReportId", ""),
        "mx": body.get("mxReportId", ""),
    }

    for marketplace, report_id in legacy_map.items():
        report_id_norm = safe_strip(report_id)
        if report_id_norm and marketplace not in report_ids:
            report_ids[marketplace] = report_id_norm

    return report_ids


def get_db_connection():
    return pymysql.connect(
        host=MARIADB_HOST,
        port=MARIADB_PORT,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        database=MARIADB_DATABASE,
        autocommit=False,
    )


def delete_month_rows(cursor, table, month, year):
    cursor.execute(
        f"DELETE FROM `{table}` WHERE MONTH = %s AND YEAR = %s",
        (month, year),
    )
    return cursor.rowcount


def insert_sku_rows(cursor, rows, chunk_size=1000):
    if not rows:
        return 0

    insert_sql = (
        f"INSERT INTO `{MARIADB_TABLE}` (SKU, MARKETPLACE, MONTH, YEAR, QUANTITY) "
        "VALUES (%s, %s, %s, %s, %s)"
    )
    inserted_total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        values = [
            (row["SKU"], row["MARKETPLACE"], row["MONTH"], row["YEAR"], row["QUANTITY"])
            for row in chunk
        ]
        cursor.executemany(insert_sql, values)
        inserted_total += cursor.rowcount
    return inserted_total


def insert_country_rows(cursor, rows):
    if not rows:
        return 0

    insert_sql = (
        f"INSERT INTO `{MARIADB_COUNTRY_TABLE}` (MARKETPLACE, MONTH, YEAR, SKU_COUNT, QUANTITY) "
        "VALUES (%s, %s, %s, %s, %s)"
    )
    values = [
        (row["MARKETPLACE"], row["MONTH"], row["YEAR"], row["SKU_COUNT"], row["QUANTITY"])
        for row in rows
    ]
    cursor.executemany(insert_sql, values)
    return cursor.rowcount


def write_month_data(db_rows, country_rows, month, year):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            deleted_sku = delete_month_rows(cursor, MARIADB_TABLE, month, year)
            inserted_sku = insert_sku_rows(cursor, db_rows)
            deleted_country = delete_month_rows(cursor, MARIADB_COUNTRY_TABLE, month, year)
            inserted_country = insert_country_rows(cursor, country_rows)
        conn.commit()
        return {
            "deletedRows": deleted_sku,
            "insertedRows": inserted_sku,
            "deletedCountryRows": deleted_country,
            "insertedCountryRows": inserted_country,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def UpdateSkuSalesMonth(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        validate_admin_key(request)
        body = request.get_json(silent=True) or {}

        start_date = body.get("startDate")
        end_date = body.get("endDate")
        confirm_month = body.get("confirmMonth")
        confirm_year = body.get("confirmYear")
        dry_run = bool(body.get("dryRun", False))

        if not start_date or not end_date:
            return json_response({"error": "Missing startDate or endDate"}, 400)

        start_month, start_year = get_la_month_year(start_date)
        end_month, end_year = get_la_month_year(end_date)

        if (start_month, start_year) != (end_month, end_year):
            return json_response(
                {
                    "error": "startDate and endDate must belong to the same month and year in America/Los_Angeles"
                },
                400,
            )

        if int(confirm_month or 0) != start_month or int(confirm_year or 0) != start_year:
            return json_response({"error": "Month/year confirmation mismatch"}, 400)

        report_ids = collect_report_ids_from_body(body)
        if not report_ids:
            return json_response(
                {
                    "error": "No report IDs provided. Use reportIds object or legacy fields like usaReportId/deReportId."
                },
                400,
            )

        order_rows = []
        fetched_reports = []

        for marketplace, report_req_id in sorted(report_ids.items()):
            xml_payload = fetch_report_payload(marketplace, report_req_id)
            parsed_rows = parse_orders_from_xml(xml_payload)
            order_rows.extend(parsed_rows)
            fetched_reports.append({
                "marketplace": marketplace,
                "reportId": report_req_id,
                "parsedRows": len(parsed_rows),
            })

        db_rows = build_db_rows(order_rows, start_month, start_year)
        country_rows = build_country_rows(db_rows, start_month, start_year)

        if dry_run:
            return json_response(
                {
                    "status": "dry_run",
                    "month": start_month,
                    "year": start_year,
                    "parsedOrderRows": len(order_rows),
                    "dbRowsCount": len(db_rows),
                    "countryRowsCount": len(country_rows),
                    "reports": fetched_reports,
                    "aggregatedByMarketplace": build_dry_run_summary(db_rows),
                    "preview": db_rows[:100],
                    "countryPreview": country_rows,
                },
                200,
            )

        write_result = write_month_data(db_rows, country_rows, start_month, start_year)

        return json_response(
            {
                "status": "success",
                "month": start_month,
                "year": start_year,
                "parsedOrderRows": len(order_rows),
                "dbRowsCount": len(db_rows),
                "countryRowsCount": len(country_rows),
                "reports": fetched_reports,
                **write_result,
            },
            200,
        )

    except PermissionError as exc:
        return json_response({"error": str(exc)}, 403)
    except ValueError as exc:
        return json_response({"error": str(exc)}, 400)
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500) 