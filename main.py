from Orders import orders_mlf
import json
import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces
import gzip
import os

CLIENT_SECRET_USA = os.environ["CLIENT_SECRET_USA"]
CLIENT_SECRET_EU = os.environ["CLIENT_SECRET_EU"]
REFRESH_TOKEN_USA = os.environ["REFRESH_TOKEN_USA"]
REFRESH_TOKEN_EU = os.environ["REFRESH_TOKEN_EU"]
CLIENT_ID_USA = os.environ["CLIENT_ID_USA"]
CLIENT_ID_EU = os.environ["CLIENT_ID_EU"]


credentials_usa = dict(
    refresh_token=REFRESH_TOKEN_USA,
    lwa_app_id=CLIENT_ID_USA,
    lwa_client_secret=CLIENT_SECRET_USA
)

credentials_eu = dict(
    refresh_token=REFRESH_TOKEN_EU,
    lwa_app_id=CLIENT_ID_EU,
    lwa_client_secret=CLIENT_SECRET_EU
)

MARKETPLACE_ID_USA = 'ATVPDKIKX0DER'
MARKETPLACE_ID_EU = 'A1PA6795UKMFR9'

endpoint_url_usa = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports"
endpoint_url_eu  = "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports"
endpoint_url_document_usa = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents"
endpoint_url_document_eu  = "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/documents"
TOKEN_URL = 'https://api.amazon.com/auth/o2/token'


def json_response(payload, status=200):
    return json.dumps(payload), status, {"Content-Type": "application/json"}


def is_gzip_compressed(data):
    return len(data) > 2 and data[:2] == b'\x1f\x8b'


def get_access_token(mp):
    print("get_access_token MP:", mp)

    if mp == "usa":
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": REFRESH_TOKEN_USA,
            "client_id": CLIENT_ID_USA,
            "client_secret": CLIENT_SECRET_USA,
        }
    else:
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": REFRESH_TOKEN_EU,
            "client_id": CLIENT_ID_EU,
            "client_secret": CLIENT_SECRET_EU,
        }

    response = requests.post(TOKEN_URL, data=payload, timeout=60)
    response_data = response.json()

    if response.status_code == 200:
        return response_data["access_token"]

    raise Exception(f"Failed to get access token: {response_data.get('error_description')}")


def create_report(access_token, dataStartTime, dataEndTime, marketplace):
    if marketplace == "usa":
        mp_id = MARKETPLACE_ID_USA
        endpoint_url = endpoint_url_usa
    else:
        mp_id = MARKETPLACE_ID_EU
        endpoint_url = endpoint_url_eu

    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
    }

    payload = {
        "reportType": "GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
        "marketplaceIds": [mp_id],
        "dataStartTime": dataStartTime,
        "dataEndTime": dataEndTime,
    }

    response = requests.post(endpoint_url, headers=headers, json=payload, timeout=60)
    response_data = response.json()

    if response.ok and response_data.get("reportId"):
        return response_data["reportId"]

    raise Exception(f"Failed to create report: {response_data.get('errors', 'Unknown Error')}")


def check_report_status(credentials, marketplace, report_id):
    print(f"Checking status: marketplace={marketplace} report_id={report_id}")

    if marketplace == "usa":
        mp_id = Marketplaces.US
    else:
        mp_id = Marketplaces.DE

    reports_api = Reports(credentials=credentials, marketplace=mp_id)
    response = reports_api.get_report(reportId=report_id)
    return response.payload


def download_report(document_id, access_token, mp):
    if mp == "usa":
        endpoint_url_docuemnt = endpoint_url_docuemnt_usa
    else:
        endpoint_url_docuemnt = endpoint_url_docuemnt_eu

    url = f"{endpoint_url_docuemnt}/{document_id}"
    print("URL " + str(url))
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-amz-access-token": access_token
    }

    response = requests.get(url, headers=headers)
    print(f"get document details: {response.status_code} {response.text}")
    response.raise_for_status()
    return response.text


def download_report_payload(url):
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    try:
        if is_gzip_compressed(response.content):
            return gzip.decompress(response.content).decode("utf-8", errors="replace")
        return response.content.decode("utf-8", errors="replace")
    except Exception as e:
        raise Exception(f"Failed to decode report payload: {e}")


def wootry(request):
    result = orders_mlf(0)
    return result, 200


def wootry1(request):
    result = orders_mlf(1)
    return result, 200


def MlfReportReq(request):
    if request.method != "POST":
        return "Method not allowed", 405

    request_json = request.get_json(silent=True) or {}

    try:
        start_date = request_json["start_date"]
        end_date = request_json["end_date"]
        marketplace = request_json["marketplace"]

        access_token = get_access_token(marketplace)
        report_id = create_report(access_token, start_date, end_date, marketplace)

        return json_response({
            "status": "success",
            "data": {
                "start_date": start_date,
                "end_date": end_date,
                "marketplace": marketplace,
                "report_req_id": report_id,
            }
        }, 200)

    except KeyError as e:
        return json_response({
            "status": "error",
            "message": f"Missing field: {str(e)}"
        }, 400)

    except Exception as e:
        print("MlfReportReq error:", str(e))
        return json_response({
            "status": "error",
            "message": str(e)
        }, 500)


def MlfReportGet(request):
    print("### MlfReportGet FIXED BUILD 2026-03-28 v8 ###")
    print("RAW BODY:", request.data)

    # Accept only POST if that is your intended API design
    if request.method != 'POST':
        return json.dumps({
            "status": "ERROR",
            "message": "Method not allowed. Use POST."
        }), 405, {'Content-Type': 'application/json'}

    request_json = request.get_json(silent=True) or {}
    print("Incoming JSON:", request_json)

    # For GET-status/fetch, only these are really required
    required_fields = ["marketplace", "report_req_id"]
    missing = [f for f in required_fields if not request_json.get(f)]

    if missing:
        return json.dumps({
            "status": "ERROR",
            "message": f"Missing required field(s): {', '.join(missing)}",
            "received": request_json
        }), 400, {'Content-Type': 'application/json'}

    marketplace = request_json["marketplace"]
    report_req_id = request_json["report_req_id"]

    print(f"MlfReportGet marketplace: {marketplace}, report_req_id: {report_req_id}")

    if marketplace == "usa":
        credentials = credentials_usa
    else:
        credentials = credentials_eu

    try:
        access_token = get_access_token(marketplace)

        print(f"Checking status: {report_req_id} marketplace={marketplace}")
        status = check_report_status(credentials, marketplace, report_req_id)

        if not isinstance(status, dict):
            return json.dumps({
                "status": "ERROR",
                "message": f"Unexpected status response: {status}"
            }), 500, {'Content-Type': 'application/json'}

        processing_status = status.get("processingStatus")
        print(f"processingStatus: {processing_status}")

        if processing_status == "DONE":
            document_id = status.get("reportDocumentId")
            if not document_id:
                return json.dumps({
                    "status": "ERROR",
                    "message": "Report finished but reportDocumentId is missing"
                }), 500, {'Content-Type': 'application/json'}

            print(f"Document ID: {document_id}")
            report_data_in = download_report(document_id, access_token, marketplace)
            print("REPORT_DATA_RAW:", report_data_in)

        elif processing_status in ["FATAL", "CANCELLED"]:
            return json.dumps({
                "status": "ERROR_FATAL",
                "data": {
                    "marketplace": marketplace,
                    "report_req_id": report_req_id,
                    "payload": processing_status,
                    "input": request_json   # 👈 add here too
                }
            }), 200, {'Content-Type': 'application/json'}

        else:
            return json.dumps({
                "status": "IN_PROCESS",
                "data": {
                    "marketplace": marketplace,
                    "report_req_id": report_req_id,
                    "payload": processing_status or "IN_PROCESS"
                }
            }), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        print("ERROR in status/download stage:", str(e))
        return json.dumps({
            "status": "ERROR_MlfReportGet",
            "data": {
                "marketplace": marketplace,
                "report_req_id": report_req_id,
                "payload": str(e),
                "input": request_json   # 👈 add here too
            }
        }), 500, {'Content-Type': 'application/json'}

    try:
        data = json.loads(report_data_in)
        url = data["url"]
        print("Download URL:", url)

        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to download the report file: {response.status_code} {response.text}")

        try:
            if is_gzip_compressed(response.content):
                decompressed_data = gzip.decompress(response.content)
                report_content_str = decompressed_data.decode("utf-8")
            else:
                report_content_str = response.content.decode("utf-8")
        except (UnicodeDecodeError, gzip.BadGzipFile) as e:
            raise Exception(f"Failed to decode report content: {str(e)}")

        return json.dumps({
            "status": "success",
            "data": {
                "marketplace": marketplace,
                "report_req_id": report_req_id,
                "payload": report_content_str
            }
        }), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        print("ERROR in report file processing:", str(e))
        return json.dumps({
            "status": "ERROR",
            "data": {
                "marketplace": marketplace,
                "report_req_id": report_req_id,
                "payload": "ERRORx5 " + str(e)
            }
        }), 500, {'Content-Type': 'application/json'}