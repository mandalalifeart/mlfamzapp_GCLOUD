import json

from MlfReport import (
    check_report_status,
    create_report,
    download_report_payload,
    get_access_token,
    get_region_config,
    get_report_document_metadata,
)


ALLOWED_ORIGIN = "https://mlfamzappfire.web.app"


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Headers": "Content-Type, x-admin-key",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Content-Type": "application/json",
    }


def json_response(payload, status=200):
    return json.dumps(payload), status, cors_headers()


def _error_response(status_text: str, message: str, http_status: int, extra: dict | None = None):
    body = {"status": status_text, "message": message}
    if extra:
        body.update(extra)
    return json_response(body, http_status)


def MlfReportReq(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    if request.method != "POST":
        return _error_response("error", "Method not allowed", 405)

    request_json = request.get_json(silent=True) or {}
    missing = [field for field in ["start_date", "end_date", "marketplace"] if not request_json.get(field)]
    if missing:
        return _error_response(
            "error",
            f"Missing required field(s): {', '.join(missing)}",
            400,
            {"received": request_json},
        )

    try:
        config = get_region_config(request_json["marketplace"])
        report_id = create_report(
            config=config,
            data_start_time=request_json["start_date"],
            data_end_time=request_json["end_date"],
        )
        return json_response(
            {
                "status": "success",
                "data": {
                    "start_date": request_json["start_date"],
                    "end_date": request_json["end_date"],
                    "report_req_id": report_id,
                },
            },
            200,
        )
    except Exception as exc:
        return _error_response("error", str(exc), 500)


def MlfReportGet(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    if request.method != "POST":
        return _error_response("ERROR", "Method not allowed", 405)

    request_json = request.get_json(silent=True) or {}
    missing = [field for field in ["marketplace", "report_req_id"] if not request_json.get(field)]
    if missing:
        return _error_response(
            "ERROR",
            f"Missing required field(s): {', '.join(missing)}",
            400,
            {"received": request_json},
        )

    marketplace = request_json["marketplace"]
    report_req_id = request_json["report_req_id"]

    try:
        config = get_region_config(marketplace)
        access_token = get_access_token(config)
        status_payload = check_report_status(config, report_req_id)
        processing_status = status_payload.get("processingStatus")

        if processing_status == "DONE":
            document_id = status_payload.get("reportDocumentId")
            if not document_id:
                return _error_response(
                    "ERROR",
                    "Report finished but reportDocumentId is missing",
                    500,
                )

            metadata = get_report_document_metadata(document_id, access_token, config)
            url = metadata.get("url")
            if not url:
                return _error_response("ERROR", "Missing report document URL", 500)

            report_content = download_report_payload(url)
            return json_response(
                {
                    "status": "success",
                    "data": {
                        "marketplace": marketplace,
                        "report_req_id": report_req_id,
                        "payload": report_content,
                    },
                },
                200,
            )

        if processing_status in {"FATAL", "CANCELLED"}:
            return json_response(
                {
                    "status": "ERROR_FATAL",
                    "data": {
                        "marketplace": marketplace,
                        "report_req_id": report_req_id,
                        "payload": processing_status,
                    },
                },
                200,
            )

        return json_response(
            {
                "status": "IN_PROCESS",
                "data": {
                    "marketplace": marketplace,
                    "report_req_id": report_req_id,
                    "payload": processing_status or "IN_PROCESS",
                },
            },
            200,
        )
    except Exception as exc:
        return json_response(
            {
                "status": "ERROR_MlfReportGet",
                "data": {
                    "marketplace": marketplace,
                    "report_req_id": report_req_id,
                    "payload": str(exc),
                },
            },
            500,
        )
