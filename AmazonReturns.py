"""Amazon FBA return statistics - USA + all EU marketplaces (see CLAUDE.md).

Pulls the standard Reports API's GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA
report (per-unit FBA return events: order/SKU/ASIN/reason/date/disposition/
fulfillment-center, TSV) - reuses MlfReport.py's create_report/
check_report_status/get_report_document_metadata/download_report_payload
helpers (already used for the Sales report pull) rather than duplicating the
Reports-API request/poll/download plumbing.

Confirmed live 2026-09-10: the real column order is exactly
return-date/order-id/sku/asin/fnsku/product-name/quantity/
fulfillment-center-id/detailed-disposition/reason/status/
license-plate-number/customer-comments. A single "de"-credential pull
returns EU-wide fulfillment centers (UK/DE/FR/IT/ES/PL/CZ/etc all mixed
together in one report), same bundling behavior as the Sales report this
account already relies on (see MlfReport.py's REGION_CONFIGS docstring) -
so only "usa" and "de" pulls are needed, tagged "usa"/"eu" respectively.
Country-level detail isn't broken out (would need a real fulfillment-center
-> country lookup table, not guessed from FC code prefixes) - "eu" is one
bucket for now, same tradeoff GetSalesDepartmentReport already makes
elsewhere in this codebase.

license_plate_number (LPN) is Amazon's own unique ID per physical returned
unit - used as the real upsert key (skip a row if its LPN is already
stored), NOT the delete-then-insert-by-date-range convention used elsewhere
in this codebase (e.g. ads_campaign_stats) - a historical backfill here
pulls many separate date windows one call at a time, and a blanket delete
per call would wipe out every earlier chunk's already-written rows."""
import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from MlfReport import (
    get_region_config,
    create_report,
    check_report_status,
    get_report_document_metadata,
    download_report_payload,
    get_access_token,
)

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_BATCH_SIZE = int(os.environ.get("POCKETBASE_BATCH_SIZE", "50"))
RETURNS_COLLECTION = "amazon_returns"
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
# Not a money/write-capable action, but matches the project's convention of
# gating any real Amazon API pull separately from ungated read-only summaries.
RETURNS_REFRESH_KEY = os.environ.get("RETURNS_REFRESH_KEY", "")

REPORT_TYPE_RETURNS = "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA"
REPORT_POLL_ROUNDS = 20
REPORT_POLL_DELAY_SECONDS = 15
REGIONS = {"usa": "usa", "de": "eu"}


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    import json
    return json.dumps(body), status, cors_headers()


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase auth failed: HTTP {response.status_code} - {response.text}")
    return response.json()["token"]


def pb_batch(token, batch_requests):
    for i in range(0, len(batch_requests), POCKETBASE_BATCH_SIZE):
        chunk = batch_requests[i:i + POCKETBASE_BATCH_SIZE]
        response = requests.post(
            f"{POCKETBASE_URL}/api/batch",
            headers={"Authorization": token},
            json={"requests": chunk},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase batch failed: HTTP {response.status_code} - {response.text}")
        results = response.json()
        failed = [r for r in results if not (200 <= r.get("status", 0) < 300)]
        if failed:
            raise RuntimeError(f"PocketBase batch had {len(failed)}/{len(results)} failed op(s): {failed[:3]}")


def pb_list_ids(token, collection, filter_str):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params={"filter": filter_str, "fields": "id", "perPage": 200, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        ids.extend(item["id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


def parse_returns_tsv(text):
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return []
    header = [h.strip() for h in lines[0].split("\t")]
    rows = []
    for line in lines[1:]:
        cols = line.split("\t")
        rows.append(dict(zip(header, cols)))
    return rows


def fetch_returns_report(region_key, start_iso, end_iso):
    config = get_region_config(region_key)
    report_id = create_report(config, start_iso, end_iso, report_type=REPORT_TYPE_RETURNS)
    for _ in range(REPORT_POLL_ROUNDS):
        status_payload = check_report_status(config, report_id)
        processing_status = status_payload.get("processingStatus")
        if processing_status == "DONE":
            document_id = status_payload.get("reportDocumentId")
            access_token = get_access_token(config)
            doc_meta = get_report_document_metadata(document_id, access_token, config)
            payload = download_report_payload(doc_meta["url"])
            return parse_returns_tsv(payload)
        if processing_status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"Returns report {processing_status} for {region_key}: {status_payload}")
        time.sleep(REPORT_POLL_DELAY_SECONDS)
    raise RuntimeError(f"Returns report timed out for {region_key} (report_id={report_id})")


def pb_list_field(token, collection, filter_str, field):
    values = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params={"filter": filter_str, "fields": field, "perPage": 500, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        values.extend(item.get(field) for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return values


def write_returns(pb_token, marketplace_tag, rows):
    """Upserts by license_plate_number (Amazon's own unique ID per physical
    returned unit) rather than the delete-all-then-insert convention used
    elsewhere in this codebase - deliberately different here, since a
    historical backfill pulls many separate, possibly-overlapping date
    windows one at a time (see UpdateAmazonReturns), and a blanket
    delete-by-marketplace on every call would wipe out every earlier
    chunk's rows each time a new chunk is written. Already-known LPNs are
    skipped (not re-written), which also makes the daily rolling-window
    refresh naturally idempotent against the historical backfill's data."""
    existing_lpns = set(pb_list_field(pb_token, RETURNS_COLLECTION, f'marketplace = "{marketplace_tag}"', "license_plate_number"))
    ops = []
    for row in rows:
        lpn = row.get("license-plate-number", "").strip()
        return_date = row.get("return-date", "").strip()
        if not lpn or not return_date or lpn in existing_lpns:
            continue
        existing_lpns.add(lpn)
        try:
            quantity = int(row.get("quantity") or 1)
        except ValueError:
            quantity = 1
        ops.append({
            "method": "POST",
            "url": f"/api/collections/{RETURNS_COLLECTION}/records",
            "body": {
                "return_date": return_date,
                "order_id": row.get("order-id", ""),
                "sku": row.get("sku", ""),
                "asin": row.get("asin", ""),
                "fnsku": row.get("fnsku", ""),
                "product_name": row.get("product-name", ""),
                "quantity": quantity,
                "fulfillment_center_id": row.get("fulfillment-center-id", ""),
                "detailed_disposition": row.get("detailed-disposition", ""),
                "reason": row.get("reason", ""),
                "status": row.get("status", ""),
                "license_plate_number": lpn,
                "customer_comments": row.get("customer-comments", ""),
                "marketplace": marketplace_tag,
            },
        })
    pb_batch(pb_token, ops)
    return len(ops)


def UpdateAmazonReturns(request):
    """Pulls FBA return events for USA + EU. Default range is a rolling
    30-day lookback (this report type's real max window wasn't pinned down
    beyond 14 days confirmed live - 30 is a conservative single-chunk size);
    pass start_date/end_date (YYYY-MM-DD) for a wider backfill (not
    auto-chunked yet - a backfill beyond ~30 days needs multiple calls with
    different explicit ranges)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if RETURNS_REFRESH_KEY and (not hasattr(request, "args") or request.args.get("key") != RETURNS_REFRESH_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    start_date = request.args.get("start_date") if hasattr(request, "args") else None
    end_date = request.args.get("end_date") if hasattr(request, "args") else None
    now = datetime.now(timezone.utc)
    end_iso = (
        datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if end_date
        else now - timedelta(minutes=5)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    start_iso = (
        datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if start_date
        else now - timedelta(days=30)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    pb_token = pb_authenticate()
    results = {}
    try:
        for region_key, marketplace_tag in REGIONS.items():
            rows = fetch_returns_report(region_key, start_iso, end_iso)
            written = write_returns(pb_token, marketplace_tag, rows)
            results[marketplace_tag] = {"returnsWritten": written}
        return json_response({"startDate": start_iso, "endDate": end_iso, "regions": results})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__, "partialResults": results}, 500)


def GetReturnStats(request):
    """Read-only: aggregates already-stored amazon_returns into a
    frontend-friendly shape - no Amazon API calls, no key needed."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    try:
        pb_token = pb_authenticate()
        rows = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{RETURNS_COLLECTION}/records",
                headers={"Authorization": pb_token},
                params={"sort": "-return_date", "perPage": 500, "page": page},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            rows.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1

        by_sku = {}
        by_reason = {}
        by_marketplace = {}
        by_month = {}
        for r in rows:
            qty = r.get("quantity") or 1
            sku = r.get("sku") or "(unknown)"
            reason = r.get("reason") or "(unspecified)"
            marketplace = r.get("marketplace") or "unknown"
            return_date = r.get("return_date") or ""

            sku_entry = by_sku.setdefault(sku, {"sku": sku, "asin": r.get("asin", ""), "productName": r.get("product_name", ""), "count": 0})
            sku_entry["count"] += qty

            by_reason[reason] = by_reason.get(reason, 0) + qty
            by_marketplace[marketplace] = by_marketplace.get(marketplace, 0) + qty

            if len(return_date) >= 7:
                month_key = return_date[:7]
                by_month[month_key] = by_month.get(month_key, 0) + qty

        top_skus = sorted(by_sku.values(), key=lambda s: -s["count"])
        top_reasons = sorted(by_reason.items(), key=lambda kv: -kv[1])
        month_rows = sorted(by_month.items())

        return json_response({
            "returns": rows[:1000],
            "totalReturns": sum(r.get("quantity") or 1 for r in rows),
            "topSkus": top_skus[:50],
            "topReasons": [{"reason": k, "count": v} for k, v in top_reasons],
            "byMarketplace": by_marketplace,
            "byMonth": [{"month": k, "count": v} for k, v in month_rows],
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


LA_TZ = ZoneInfo("America/Los_Angeles")

# Maps SalesPage.jsx's marketplace dropdown codes to the two buckets this
# report actually distinguishes (see module docstring) - "jp"/"au" and any
# Etsy code have no return data (this pipeline is Amazon FBA USA+EU only)
# and simply contribute nothing, same as GetMarketplaceSalesSummary's
# UI_MARKETPLACE_TO_ATOMIC handles marketplaces it has no data for.
MARKETPLACE_UI_TO_BUCKET = {
    "usa": "usa",
    "eu": "eu", "uk": "eu", "de": "eu", "fr": "eu", "es": "eu", "it": "eu",
    "se": "eu", "nl": "eu", "be": "eu", "ie": "eu", "pl": "eu",
}


def GetReturnStatsByMonth(request):
    """Same shape as GetMarketplaceSalesSummary's per-metric summary
    ({"yearRows": [{year, months, total}], "growthPct"}) so SalesPage.jsx's
    existing YearRows table can render a returns row directly alongside
    sales, gated by the "Show Returns" checkbox (2026-09-10, per the user).
    Takes the same POST {marketplaces: [...]} shape as
    GetMarketplaceSalesSummary for consistency, even though this pipeline
    only ever pulls "usa"/"de" (eu) - see MARKETPLACE_UI_TO_BUCKET."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        body = request.get_json(silent=True) or {}
        selected = body.get("marketplaces")
        if selected is not None and not isinstance(selected, list):
            return json_response({"error": "marketplaces must be an array"}, 400)

        buckets = {MARKETPLACE_UI_TO_BUCKET[code] for code in (selected or []) if code in MARKETPLACE_UI_TO_BUCKET}
        if not buckets:
            buckets = {"usa", "eu"}

        now = datetime.now(LA_TZ)
        years = [now.year, now.year - 1, now.year - 2, now.year - 3]
        current_month = now.month

        pb_token = pb_authenticate()
        rows = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{RETURNS_COLLECTION}/records",
                headers={"Authorization": pb_token},
                params={"perPage": 500, "page": page, "fields": "return_date,quantity,marketplace"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            rows.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1

        year_months = {y: [0] * 12 for y in years}
        for r in rows:
            if r.get("marketplace") not in buckets:
                continue
            return_date = r.get("return_date") or ""
            if len(return_date) < 7:
                continue
            year, month = int(return_date[:4]), int(return_date[5:7])
            if year in year_months and 1 <= month <= 12:
                year_months[year][month - 1] += r.get("quantity") or 1

        year_rows = [{"year": y, "months": year_months[y], "total": sum(year_months[y])} for y in years]

        return json_response({"yearRows": year_rows, "years": years, "currentMonth": current_month})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def GetReturnStatsByAsin(request):
    """Same idea as GetReturnStatsByMonth, but broken out per-ASIN so
    SalesPage.jsx can overlay a returns row on each individual product (and,
    by summing member ASINs client-side, each group) - added 2026-09-10 per
    the user ("returns per product and per group also, not only total").
    Returns {asinYearRows: {asin: [{year, months, total}]}, years,
    currentMonth} - grouping by group is left to the frontend (it already
    knows which ASINs belong to which group from GetSalesDepartmentReport;
    this endpoint doesn't need to know about asin_group_mapping at all)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        body = request.get_json(silent=True) or {}
        selected = body.get("marketplaces")
        if selected is not None and not isinstance(selected, list):
            return json_response({"error": "marketplaces must be an array"}, 400)

        buckets = {MARKETPLACE_UI_TO_BUCKET[code] for code in (selected or []) if code in MARKETPLACE_UI_TO_BUCKET}
        if not buckets:
            buckets = {"usa", "eu"}

        now = datetime.now(LA_TZ)
        years = [now.year, now.year - 1, now.year - 2, now.year - 3]
        current_month = now.month

        pb_token = pb_authenticate()
        rows = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{RETURNS_COLLECTION}/records",
                headers={"Authorization": pb_token},
                params={"perPage": 500, "page": page, "fields": "return_date,quantity,marketplace,asin"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            rows.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1

        by_asin_year_months = {}
        for r in rows:
            if r.get("marketplace") not in buckets:
                continue
            asin = r.get("asin") or ""
            if not asin:
                continue
            return_date = r.get("return_date") or ""
            if len(return_date) < 7:
                continue
            year, month = int(return_date[:4]), int(return_date[5:7])
            if year not in years or not (1 <= month <= 12):
                continue
            year_months = by_asin_year_months.setdefault(asin, {y: [0] * 12 for y in years})
            year_months[year][month - 1] += r.get("quantity") or 1

        asin_year_rows = {
            asin: [{"year": y, "months": ym[y], "total": sum(ym[y])} for y in years]
            for asin, ym in by_asin_year_months.items()
        }

        return json_response({"asinYearRows": asin_year_rows, "years": years, "currentMonth": current_month})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
