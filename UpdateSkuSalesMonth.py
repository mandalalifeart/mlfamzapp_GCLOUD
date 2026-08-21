import json
import os
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
from xml.etree import ElementTree as ET

import requests

from MlfReport import DB_MARKETPLACE_MAP, EU_MARKETPLACES

API_BASE = os.environ.get("API_BASE", "https://us-central1-mlfamzapp.cloudfunctions.net")
POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_SKU_COLLECTION = os.environ.get("POCKETBASE_SKU_COLLECTION", "sku_sales")
POCKETBASE_COUNTRY_COLLECTION = os.environ.get("POCKETBASE_COUNTRY_COLLECTION", "country_sales")
POCKETBASE_BATCH_SIZE = int(os.environ.get("POCKETBASE_BATCH_SIZE", "50"))
ALLOWED_ORIGIN = "https://mlfamzappfire.web.app"
LA_TZ = ZoneInfo("America/Los_Angeles")


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
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

            asin = safe_strip(order_item.findtext("ASIN"))

            qty_raw = safe_strip(order_item.findtext("Quantity"))
            try:
                qty = int(float(qty_raw)) if qty_raw else 0
            except ValueError:
                qty = 0

            if qty <= 0:
                continue

            amount_raw = safe_strip(order_item.findtext("ItemPrice")) or safe_strip(
                order_item.findtext("ItemPrice/Amount")
            )
            try:
                amount = float(amount_raw) if amount_raw else 0.0
            except ValueError:
                amount = 0.0

            rows.append({
                "sku": sku,
                "asin": asin,
                "qty": qty,
                "amount": amount,
                "sales_channel": sales_channel.lower(),
            })

    return rows


def build_db_rows(order_rows, month, year):
    totals = {}

    def add_row(sku, asin, marketplace_code, qty, amount):
        key = (sku, marketplace_code, month, year)
        if key not in totals:
            totals[key] = {
                "SKU": sku,
                "ASIN": asin,
                "MARKETPLACE": marketplace_code,
                "MONTH": month,
                "YEAR": year,
                "QUANTITY": 0,
                "AMOUNT": 0.0,
            }
        totals[key]["QUANTITY"] += qty
        totals[key]["AMOUNT"] += amount

    for row in order_rows:
        marketplace_code = DB_MARKETPLACE_MAP.get(row["sales_channel"])
        if not marketplace_code:
            continue

        qty = int(row["qty"])
        amount = float(row.get("amount", 0.0))
        asin = row.get("asin", "")
        add_row(row["sku"], asin, marketplace_code, qty, amount)

        if marketplace_code in EU_MARKETPLACES:
            add_row(row["sku"], asin, "eu", qty, amount)

    return sorted(totals.values(), key=lambda item: (item["MARKETPLACE"], item["SKU"]))


def find_missing_asin_skus(db_rows):
    return sorted({row["SKU"] for row in db_rows if not row.get("ASIN")})


def build_country_rows(db_rows, month, year):
    totals = {}

    for row in db_rows:
        marketplace_code = row["MARKETPLACE"]
        if marketplace_code not in totals:
            totals[marketplace_code] = {
                "MARKETPLACE": marketplace_code,
                "MONTH": month,
                "YEAR": year,
                "QUANTITY": 0,
                "AMOUNT": 0.0,
            }
        totals[marketplace_code]["QUANTITY"] += int(row["QUANTITY"])
        totals[marketplace_code]["AMOUNT"] += float(row.get("AMOUNT", 0.0))

    for item in totals.values():
        item["AMOUNT"] = round(item["AMOUNT"], 2)

    return sorted(totals.values(), key=lambda item: item["MARKETPLACE"])


def build_dry_run_summary(db_rows):
    by_marketplace = defaultdict(lambda: {"rows": 0, "units": 0, "unique_skus": set(), "amount": 0.0})

    for row in db_rows:
        mp = row["MARKETPLACE"]
        by_marketplace[mp]["rows"] += 1
        by_marketplace[mp]["units"] += int(row["QUANTITY"])
        by_marketplace[mp]["unique_skus"].add(row["SKU"])
        by_marketplace[mp]["amount"] += float(row.get("AMOUNT", 0.0))

    summary = []
    for mp in sorted(by_marketplace.keys()):
        item = by_marketplace[mp]
        summary.append({
            "marketplace": mp,
            "rows": item["rows"],
            "units": item["units"],
            "uniqueSkus": len(item["unique_skus"]),
            "amount": round(item["amount"], 2),
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


def pb_list_ids(token, collection, month, year):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params={
                "filter": f"(month={month}&&year={year})",
                "fields": "id",
                "perPage": 200,
                "page": page,
            },
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"PocketBase list failed for {collection}: HTTP {response.status_code} - {response.text}"
            )
        data = response.json()
        ids.extend(item["id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


def pb_batch(token, batch_requests):
    if not batch_requests:
        return
    response = requests.post(
        f"{POCKETBASE_URL}/api/batch",
        headers={"Authorization": token},
        json={"requests": batch_requests},
        timeout=60,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase batch request failed: HTTP {response.status_code} - {response.text}")
    for entry, result in zip(batch_requests, response.json()):
        status = result.get("status")
        if status is None or status >= 400:
            raise RuntimeError(
                f"PocketBase batch item failed ({entry['method']} {entry['url']}): {result.get('body')}"
            )


def sku_row_to_body(row):
    return {
        "sku": row["SKU"],
        "ASIN": row.get("ASIN", ""),
        "marketplace": row["MARKETPLACE"],
        "month": row["MONTH"],
        "year": row["YEAR"],
        "quantity": row["QUANTITY"],
    }


def country_row_to_body(row):
    return {
        "marketplace": row["MARKETPLACE"],
        "month": row["MONTH"],
        "year": row["YEAR"],
        "quantity": row["QUANTITY"],
        # PocketBase's country_sales field is named "sales", not "amount" - this
        # previously sent "amount", a field the collection doesn't have, so
        # PocketBase silently dropped it and every row's sales stayed at 0.
        "sales": row["AMOUNT"],
    }


def write_month_data(db_rows, country_rows, month, year):
    token = pb_authenticate()

    existing_sku_ids = pb_list_ids(token, POCKETBASE_SKU_COLLECTION, month, year)
    existing_country_ids = pb_list_ids(token, POCKETBASE_COUNTRY_COLLECTION, month, year)

    ops = (
        [
            {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_SKU_COLLECTION}/records/{rid}"}
            for rid in existing_sku_ids
        ]
        + [
            {
                "method": "POST",
                "url": f"/api/collections/{POCKETBASE_SKU_COLLECTION}/records",
                "body": sku_row_to_body(row),
            }
            for row in db_rows
        ]
        + [
            {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_COUNTRY_COLLECTION}/records/{rid}"}
            for rid in existing_country_ids
        ]
        + [
            {
                "method": "POST",
                "url": f"/api/collections/{POCKETBASE_COUNTRY_COLLECTION}/records",
                "body": country_row_to_body(row),
            }
            for row in country_rows
        ]
    )

    # Each /api/batch call is one PocketBase transaction; chunking keeps requests under
    # the server's max batch size, so atomicity holds within a chunk but not across the
    # whole month once the row count exceeds POCKETBASE_BATCH_SIZE.
    for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
        pb_batch(token, ops[i:i + POCKETBASE_BATCH_SIZE])

    return {
        "deletedRows": len(existing_sku_ids),
        "insertedRows": len(db_rows),
        "deletedCountryRows": len(existing_country_ids),
        "insertedCountryRows": len(country_rows),
    }


def UpdateSkuSalesMonth(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
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
        missing_asin_skus = find_missing_asin_skus(db_rows)
        asin_warning = (
            f"{len(missing_asin_skus)} SKU(s) written with empty ASIN: {', '.join(missing_asin_skus)}"
            if missing_asin_skus
            else ""
        )

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
                    "preview": db_rows,
                    "countryPreview": country_rows,
                    "asinWarning": asin_warning,
                    "missingAsinSkus": missing_asin_skus,
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
                "asinWarning": asin_warning,
                "missingAsinSkus": missing_asin_skus,
                "updatedRows": db_rows,
                "updatedCountryRows": country_rows,
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