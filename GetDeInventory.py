import csv
import io
import json
import os
import time

from sp_api.api import Reports
from sp_api.base import Marketplaces

from MlfReport import download_report_payload

CLIENT_ID_EU = os.environ["CLIENT_ID_EU"]
CLIENT_SECRET_EU = os.environ["CLIENT_SECRET_EU"]
REFRESH_TOKEN_EU = os.environ["REFRESH_TOKEN_EU"]
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

REPORT_TYPE = "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA"
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS = 14  # ~140s, safely under the function's 180s timeout


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def get_reports_api():
    credentials = {
        "refresh_token": REFRESH_TOKEN_EU,
        "lwa_app_id": CLIENT_ID_EU,
        "lwa_client_secret": CLIENT_SECRET_EU,
    }
    return Reports(credentials=credentials, marketplace=Marketplaces.DE)


def parse_tsv(text):
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader)


def GetDeInventory(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        reports_api = get_reports_api()

        create_resp = reports_api.create_report(
            reportType=REPORT_TYPE,
            marketplaceIds=[Marketplaces.DE.marketplace_id],
        )
        report_id = (create_resp.payload or {}).get("reportId")
        if not report_id:
            return json_response({"error": f"Amazon did not return a reportId: {create_resp.payload}"}, 500)

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
                return json_response({"error": f"Amazon report failed: {processing_status}", "reportId": report_id}, 502)

        if not document_id:
            return json_response(
                {"error": f"Report not ready yet (status={processing_status})", "reportId": report_id},
                202,
            )

        doc_resp = reports_api.get_report_document(document_id, download=False)
        url = (doc_resp.payload or {}).get("url")
        if not url:
            return json_response({"error": "Missing report document URL"}, 500)

        text = download_report_payload(url)
        rows = parse_tsv(text)

        return json_response({"status": "success", "reportId": report_id, "rowCount": len(rows), "rows": rows})

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
