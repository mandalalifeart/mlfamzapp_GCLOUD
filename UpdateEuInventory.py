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
    # Amazon.com.au (added 2026-10-19): separate Far East app. au_balance =
    # FBA fulfillable, au_on_the_way = FBA inbound (working + shipped +
    # receiving) - no AWD/LG there, and no manual OTW import for AU.
    "au": {"marketplace": Marketplaces.AU, "fba_field": "au_balance", "balance_field": "au_balance"},
    # Amazon.co.jp (added 2026-10-30): the USA app authorized in Seller
    # Central JP (Far East token REFRESH_TOKEN_JP). Same live-API path as AU.
    "jp": {"marketplace": Marketplaces.JP, "fba_field": "jp_balance", "balance_field": "jp_balance"},
}


def credentials(region=None):
    if region == "jp":
        return {
            "refresh_token": os.environ["REFRESH_TOKEN_JP"],
            "lwa_app_id": os.environ["CLIENT_ID_USA"],
            "lwa_client_secret": os.environ["CLIENT_SECRET_USA"],
        }
    if region == "au":
        return {
            "refresh_token": os.environ["REFRESH_TOKEN_AU"],
            "lwa_app_id": os.environ["CLIENT_ID_AU"],
            "lwa_client_secret": os.environ["CLIENT_SECRET_AU"],
        }
    return {
        "refresh_token": REFRESH_TOKEN_EU,
        "lwa_app_id": CLIENT_ID_EU,
        "lwa_client_secret": CLIENT_SECRET_EU,
    }


def parse_tsv(text):
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader)


def fetch_fba_rows(marketplace, region=None):
    reports_api = Reports(credentials=credentials(region), marketplace=marketplace)

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


def fetch_au_rows(region="au"):
    """AU stock from the live FBA Inventory API instead of the MYI report:
    the report comes back FATAL when requested again within ~30 min, the API
    doesn't. Returned in the report's column names so the code below is shared."""
    from sp_api.api import Inventories
    marketplace = REGIONS[region]["marketplace"]
    api = Inventories(credentials=credentials(region), marketplace=marketplace)
    rows, token = [], None
    while True:
        kw = {"details": True, "marketplaceIds": [marketplace.marketplace_id]}
        if token:
            kw["nextToken"] = token
        for attempt in range(6):
            try:
                resp = api.get_inventory_summary_marketplace(**kw)
                break
            except Exception as exc:  # QuotaExceeded: back off and retry
                if "Quota" not in str(exc) and "Throttl" not in str(exc) or attempt == 5:
                    raise
                time.sleep(5 * (attempt + 1))
        for x in (resp.payload or {}).get("inventorySummaries", []):
            d = x.get("inventoryDetails") or {}
            rows.append({"sku": x.get("sellerSku") or "", "asin": x.get("asin") or "",
                         "afn-fulfillable-quantity": d.get("fulfillableQuantity") or 0,
                         "afn-inbound-working-quantity": d.get("inboundWorkingQuantity") or 0,
                         "afn-inbound-shipped-quantity": d.get("inboundShippedQuantity") or 0,
                         "afn-inbound-receiving-quantity": d.get("inboundReceivingQuantity") or 0})
        token = resp.next_token
        if not token:
            return rows
        time.sleep(0.6)


def sync_eu_inventory(region):
    if region not in REGIONS:
        raise ValueError(f"region must be one of {sorted(REGIONS)}")
    cfg = REGIONS[region]
    fba_rows = fetch_au_rows(region) if region in ("au", "jp") else fetch_fba_rows(cfg["marketplace"], region)

    token = pb_authenticate()
    sku_to_asin, asin_to_first_sku = load_mapping(token)
    stats_records = fetch_all(token, POCKETBASE_STATS_COLLECTION)
    stats_by_sku = {rec.get("sku"): rec for rec in stats_records if rec.get("sku")}

    fba_by_asin = {}
    inbound_by_asin = {}
    unmapped = []
    for row in fba_rows:
        sku = (row.get("sku") or "").strip()
        if not sku or sku.startswith("amzn.gr."):
            continue
        asin = (row.get("asin") or "").strip() or sku_to_asin.get(sku)
        qty = int(float(row.get("afn-fulfillable-quantity") or 0))
        inbound = sum(int(float(row.get(c) or 0)) for c in (
            "afn-inbound-working-quantity", "afn-inbound-shipped-quantity", "afn-inbound-receiving-quantity"))
        if not asin:
            if qty:
                unmapped.append(sku)
            continue
        if region in ("au", "jp"):
            # AU has several seller SKUs on one ASIN (e.g. Pareo5Blue + Pareo5Blue502)
            # and Amazon reports the same units on each of them - summing doubled
            # the inbound (checked 2026-10-19 against shipment FBA15GK3TGQD), so
            # take the max per ASIN there.
            fba_by_asin[asin] = max(fba_by_asin.get(asin, 0), qty)
            inbound_by_asin[asin] = max(inbound_by_asin.get(asin, 0), inbound)
        else:
            fba_by_asin[asin] = fba_by_asin.get(asin, 0) + qty
            inbound_by_asin[asin] = inbound_by_asin.get(asin, 0) + inbound

    written = []
    for asin, fba_qty in fba_by_asin.items():
        record, target_sku = resolve_target_record(asin, stats_by_sku, sku_to_asin, asin_to_first_sku)
        if not target_sku:
            continue

        body = {cfg["fba_field"]: fba_qty}
        # Per the user (2026-10-03): DE balance = FBA + AWD + LG, UK balance =
        # FBA + AWD. AWD/LG are never written here - whatever is stored is
        # folded into the new combined total.
        existing_awd = (record or {}).get(f"{region}_balance_awd") or 0
        if region in ("au", "jp"):
            body[f"{region}_on_the_way"] = inbound_by_asin.get(asin, 0)
        elif region == "de":
            existing_lg = (record or {}).get("de_balance_lg") or 0
            body["de_balance"] = fba_qty + existing_awd + existing_lg
        else:
            body[cfg["balance_field"]] = fba_qty + existing_awd

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
