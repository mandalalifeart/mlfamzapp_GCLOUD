import csv
import io
import json
import os
import time

import requests
from sp_api.api import AmazonWarehousingAndDistribution, Reports
from sp_api.base import Marketplaces

from MlfReport import download_report_payload

CLIENT_ID_USA = os.environ["CLIENT_ID_USA"]
CLIENT_SECRET_USA = os.environ["CLIENT_SECRET_USA"]
REFRESH_TOKEN_USA = os.environ["REFRESH_TOKEN_USA"]
POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_STATS_COLLECTION = os.environ.get("POCKETBASE_STATS_COLLECTION", "sku_statistics")
POCKETBASE_MAPPING_COLLECTION = os.environ.get("POCKETBASE_MAPPING_COLLECTION", "asin_group_mapping")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

FBA_REPORT_TYPE = "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA"
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS = 14  # ~140s, safely under the function's timeout


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def credentials():
    return {
        "refresh_token": REFRESH_TOKEN_USA,
        "lwa_app_id": CLIENT_ID_USA,
        "lwa_client_secret": CLIENT_SECRET_USA,
    }


def parse_tsv(text):
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader)


def fetch_fba_rows():
    """Same GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA report already used for
    UK/DE (see the amzbot-fba-inventory-pull memory), just against the US
    marketplace/credentials instead of EU."""
    reports_api = Reports(credentials=credentials(), marketplace=Marketplaces.US)

    create_resp = reports_api.create_report(
        reportType=FBA_REPORT_TYPE,
        marketplaceIds=[Marketplaces.US.marketplace_id],
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


def fetch_awd_rows():
    # maxResults must be passed explicitly, and nextToken is top-level on
    # the response (not nested under "pagination") - see GetAwdInventory.py
    # for the full explanation of the 25-SKU truncation bug this fixes.
    client = AmazonWarehousingAndDistribution(credentials=credentials(), marketplace=Marketplaces.US)
    inventory = []
    next_token = None
    while True:
        kwargs = {"maxResults": 200}
        if next_token:
            kwargs["nextToken"] = next_token
        response = client.list_inventory(**kwargs)
        payload = response.payload or {}
        inventory.extend(payload.get("inventory", []))
        next_token = payload.get("nextToken")
        if not next_token:
            break
    return inventory


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


def fetch_all(token, collection, timeout=60):
    records = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params={"perPage": 500, "page": page},
            timeout=timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        records.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return records


def load_mapping(token):
    """sku_to_asin covers every mapped SKU (including IGNORE'd ones, same as
    GetNextOrderData/UpdateNextOrderField) so a report row resolves correctly
    even if it lands under a retired/ignored SKU spelling."""
    sku_to_asin = {}
    asin_to_first_sku = {}
    for row in fetch_all(token, POCKETBASE_MAPPING_COLLECTION):
        sku = (row.get("sku") or "").strip()
        asin = (row.get("asin") or "").strip()
        if not sku or not asin:
            continue
        sku_to_asin[sku] = asin
        asin_to_first_sku.setdefault(asin, sku)
    return sku_to_asin, asin_to_first_sku


def resolve_target_record(asin, stats_by_sku, sku_to_asin, asin_to_first_sku):
    """Same "resolve by ASIN before creating a new record" rule as
    UpdateNextOrderField.find_record - a value for one ASIN should land on
    whichever sku_statistics record already carries that ASIN, not fork into
    a second record under a different SKU spelling of the same product."""
    for sku, rec in stats_by_sku.items():
        if sku_to_asin.get(sku) == asin:
            return rec, sku
    fallback_sku = asin_to_first_sku.get(asin)
    return None, fallback_sku


def UpdateUsaInventory(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method not in ("GET", "POST"):
        return json_response({"error": "Method not allowed"}, 405)

    try:
        fba_rows = fetch_fba_rows()
        awd_rows = fetch_awd_rows()

        token = pb_authenticate()
        sku_to_asin, asin_to_first_sku = load_mapping(token)
        stats_records = fetch_all(token, POCKETBASE_STATS_COLLECTION)
        stats_by_sku = {rec.get("sku"): rec for rec in stats_records if rec.get("sku")}

        fba_by_asin = {}
        fba_unmapped = []
        for row in fba_rows:
            sku = (row.get("sku") or "").strip()
            if not sku or sku.startswith("amzn.gr."):
                continue
            asin = (row.get("asin") or "").strip() or sku_to_asin.get(sku)
            qty = int(float(row.get("afn-fulfillable-quantity") or 0))
            if not asin:
                if qty:
                    fba_unmapped.append(sku)
                continue
            fba_by_asin[asin] = fba_by_asin.get(asin, 0) + qty

        # AWD's own onhand/inbound split maps onto this app's existing
        # balance/on-the-way convention: usa_balance_awd is available (on
        # hand) stock, and AWD's inbound quantity feeds usa_on_the_way -
        # not usa_balance_awd - since USA's "on the way" is defined as AWD's
        # inbound only (FBA doesn't contribute to it here).
        awd_onhand_by_asin = {}
        awd_inbound_by_asin = {}
        awd_unmapped = []
        for row in awd_rows:
            sku = (row.get("sku") or "").strip()
            onhand = int(row.get("totalOnhandQuantity") or 0)
            inbound = int(row.get("totalInboundQuantity") or 0)
            asin = sku_to_asin.get(sku)
            if not asin:
                if onhand or inbound:
                    awd_unmapped.append(sku)
                continue
            awd_onhand_by_asin[asin] = awd_onhand_by_asin.get(asin, 0) + onhand
            awd_inbound_by_asin[asin] = awd_inbound_by_asin.get(asin, 0) + inbound

        all_asins = set(fba_by_asin) | set(awd_onhand_by_asin) | set(awd_inbound_by_asin)
        written = []
        for asin in all_asins:
            record, target_sku = resolve_target_record(asin, stats_by_sku, sku_to_asin, asin_to_first_sku)
            if not target_sku:
                continue
            fba_qty = fba_by_asin.get(asin, 0)
            awd_qty = awd_onhand_by_asin.get(asin, 0)
            body = {
                "usa_balance_fba": fba_qty,
                "usa_balance_awd": awd_qty,
                "usa_balance": fba_qty + awd_qty,
                "usa_on_the_way": awd_inbound_by_asin.get(asin, 0),
            }
            if record:
                resp = requests.patch(
                    f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records/{record['id']}",
                    headers={"Authorization": token},
                    json=body,
                    timeout=30,
                )
            else:
                resp = requests.post(
                    f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records",
                    headers={"Authorization": token},
                    json={"sku": target_sku, **body},
                    timeout=30,
                )
            if resp.status_code not in (200, 201):
                raise RuntimeError(f"PocketBase write failed for {target_sku}: HTTP {resp.status_code} - {resp.text}")
            written.append({"sku": target_sku, "asin": asin, **body})

        return json_response({
            "status": "success",
            "skusWritten": len(written),
            "fbaUnmappedSkus": sorted(set(fba_unmapped)),
            "awdUnmappedSkus": sorted(set(awd_unmapped)),
            "written": written,
        })

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
