"""UK + DE FBA inventory sync into sku_statistics, added 2026-09-20 at the
user's request, mirroring UpdateUsaInventory.py's shape (fetch → resolve by
ASIN → write) but for the EU region.

AWD is NOT called here yet: confirmed live 2026-09-20 that this account's
EU credentials get "Unauthorized" from the AWD API for both UK and DE - the
user is checking Seller Central for whether that's a grantable permission
or a genuine regional unavailability. Once resolved, adding AWD here is a
small increment (same shape as UpdateUsaInventory.fetch_awd_rows), not a
redesign.

DE's balance additionally folds in de_balance_lg - a manually-maintained
3rd-party-warehouse (Lagerpark Meiningen UG) number the user updates
directly from time to time. This sync NEVER writes de_balance_lg itself,
only reads whatever is already stored and adds it into the combined
de_balance total, so a manual LG update always survives the next scheduled
sync.
"""
import csv
import io
import os
import time

from sp_api.api import Reports
from sp_api.base import Marketplaces

from MlfReport import download_report_payload
from UpdateUsaInventory import (
    POCKETBASE_STATS_COLLECTION,
    POCKETBASE_URL,
    cors_headers,
    fetch_all,
    json_response,
    load_mapping,
    pb_authenticate,
    resolve_target_record,
)

import requests

CLIENT_ID_EU = os.environ["CLIENT_ID_EU"]
CLIENT_SECRET_EU = os.environ["CLIENT_SECRET_EU"]
REFRESH_TOKEN_EU = os.environ["REFRESH_TOKEN_EU"]

FBA_REPORT_TYPE = "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA"
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS = 14  # ~140s, safely under the function's timeout

# region -> (sp_api Marketplaces enum, sku_statistics fba field, sku_statistics
# combined balance field). "UK" is an alias for GB in the sp_api enum.
REGIONS = {
    "uk": {"marketplace": Marketplaces.UK, "fba_field": "uk_balance_fba", "balance_field": "uk_balance"},
    "de": {"marketplace": Marketplaces.DE, "fba_field": "de_balance_fba", "balance_field": "de_balance"},
}


def credentials():
    return {
        "refresh_token": REFRESH_TOKEN_EU,
        "lwa_app_id": CLIENT_ID_EU,
        "lwa_client_secret": CLIENT_SECRET_EU,
    }


def parse_tsv(text):
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader)


def fetch_fba_rows(marketplace):
    reports_api = Reports(credentials=credentials(), marketplace=marketplace)

    create_resp = reports_api.create_report(
        reportType=FBA_REPORT_TYPE,
        marketplaceIds=[marketplace.marketplace_id],
    )
    report_id = (create_resp.payload or {}).get("reportId")
    if not report_id:
        raise RuntimeError(f"Amazon did not return a reportId: {create_resp.payload}")

    processing_status = None
    document_id = None
    for _ in range(MAX_POLL_ATTEMPTS):
        time.sleep(POLL_INTERVAL_SECONDS)
        status_resp = reports_api.get_report(reportId=report_id)
        payload = status_resp.payload or {}
        processing_status = payload.get("processingStatus")
        if processing_status == "DONE":
            document_id = payload.get("reportDocumentId")
            break
        if processing_status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"Amazon FBA report failed: {processing_status} (reportId={report_id})")

    if not document_id:
        raise RuntimeError(f"FBA report not ready yet (status={processing_status}, reportId={report_id})")

    doc_resp = reports_api.get_report_document(document_id, download=False)
    url = (doc_resp.payload or {}).get("url")
    if not url:
        raise RuntimeError("Missing FBA report document URL")

    text = download_report_payload(url)
    return parse_tsv(text)


def sync_eu_inventory(region):
    if region not in REGIONS:
        raise ValueError(f"region must be one of {sorted(REGIONS)}")
    cfg = REGIONS[region]
    fba_rows = fetch_fba_rows(cfg["marketplace"])

    token = pb_authenticate()
    sku_to_asin, asin_to_first_sku = load_mapping(token)
    stats_records = fetch_all(token, POCKETBASE_STATS_COLLECTION)
    stats_by_sku = {rec.get("sku"): rec for rec in stats_records if rec.get("sku")}

    fba_by_asin = {}
    unmapped = []
    for row in fba_rows:
        sku = (row.get("sku") or "").strip()
        if not sku or sku.startswith("amzn.gr."):
            continue
        asin = (row.get("asin") or "").strip() or sku_to_asin.get(sku)
        qty = int(float(row.get("afn-fulfillable-quantity") or 0))
        if not asin:
            if qty:
                unmapped.append(sku)
            continue
        fba_by_asin[asin] = fba_by_asin.get(asin, 0) + qty

    written = []
    for asin, fba_qty in fba_by_asin.items():
        record, target_sku = resolve_target_record(asin, stats_by_sku, sku_to_asin, asin_to_first_sku)
        if not target_sku:
            continue

        body = {cfg["fba_field"]: fba_qty}
        if region == "de":
            # Never overwrite the manually-maintained LG number - just fold
            # whatever is already stored into the new combined total.
            existing_lg = (record or {}).get("de_balance_lg") or 0
            body["de_balance"] = fba_qty + existing_lg
        else:
            body[cfg["balance_field"]] = fba_qty

        if record:
            resp = requests.patch(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records/{record['id']}",
                headers={"Authorization": token}, json=body, timeout=30,
            )
        else:
            resp = requests.post(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records",
                headers={"Authorization": token}, json={"sku": target_sku, **body}, timeout=30,
            )
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"PocketBase write failed for {target_sku}: HTTP {resp.status_code} - {resp.text}")
        written.append({"sku": target_sku, "asin": asin, **body})

    return {
        "status": "success",
        "region": region,
        "skusWritten": len(written),
        "unmappedSkus": sorted(set(unmapped)),
        "written": written,
    }


def UpdateEuInventory(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method not in ("GET", "POST"):
        return json_response({"error": "Method not allowed"}, 405)

    region = None
    if hasattr(request, "args"):
        region = request.args.get("region")

    try:
        regions_to_run = [region] if region else ["uk", "de"]
        results = {r: sync_eu_inventory(r) for r in regions_to_run}
        return json_response({"status": "success", "results": results})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
