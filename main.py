from Orders import orders_mlf
import json
import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces
import gzip
import os
from MlfReport import get_endpoint
from MlfReport import MARKETPLACE_MAP

def cors_headers():
    return {
        "Access-Control-Allow-Origin": "https://mlfamzappfire.web.app",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
    }

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


#Marketplace(country_code='GB', marketplace_id='A1F83G8C2ARO7P', region='EU')


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
    print("get_access_token MP done:", mp)

    if response.status_code == 200:
        print("get_access_token MP 200:", response_data["access_token"])
        return response_data["access_token"]
        

    raise Exception(f"Failed to get access token: {response_data.get('error_description')}")


def create_report(access_token, dataStartTime, dataEndTime, marketplace):
    
    mp = MARKETPLACE_MAP[marketplace]
    mp_id = mp.marketplace_id
    endpoint_url = get_endpoint(marketplace,"reports")
    print("URL " + str(endpoint_url))

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
    mp = MARKETPLACE_MAP[marketplace]
    mp_id = mp.marketplace_id

    reports_api = Reports(credentials=credentials, marketplace=mp)
    response = reports_api.get_report(reportId=report_id)
    return response.payload


def download_report(document_id, access_token, marketplace):
    mp = MARKETPLACE_MAP[marketplace]
    endpoint_url_docuemnt = get_endpoint(mp,"documents")
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


def MlfReportGet(request):
    if request.method == "OPTIONS":
        return ("", 204, cors_headers())

    if request.method != "POST":
        return (
            json.dumps({
                "status": "ERROR",
                "message": "Method not allowed"
            }),
            405,
            cors_headers(),
        )

    request_json = request.get_json(silent=True) or {}

    missing = [f for f in ["marketplace", "report_req_id"] if not request_json.get(f)]
    if missing:
        return (
            json.dumps({
                "status": "ERROR",
                "message": f"Missing required field(s): {', '.join(missing)}",
                "received": request_json
            }),
            400,
            cors_headers(),
        )

    marketplace = request_json["marketplace"]
    report_req_id = request_json["report_req_id"]

    if marketplace == "usa":
        credentials = credentials_usa
    else:
        credentials = credentials_eu

    try:
        access_token = get_access_token(marketplace)
        status = check_report_status(credentials, marketplace, report_req_id)

        if not isinstance(status, dict):
            return (
                json.dumps({
                    "status": "ERROR",
                    "message": f"Unexpected status response: {status}"
                }),
                500,
                cors_headers(),
            )

        processing_status = status.get("processingStatus")

        if processing_status == "DONE":
            document_id = status.get("reportDocumentId")
            if not document_id:
                return (
                    json.dumps({
                        "status": "ERROR",
                        "message": "Report finished but reportDocumentId is missing"
                    }),
                    500,
                    cors_headers(),
                )

            report_data_in = download_report(document_id, access_token, marketplace)
            data = json.loads(report_data_in)
            url = data["url"]

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

            return (
                json.dumps({
                    "status": "success",
                    "data": {
                        "marketplace": marketplace,
                        "report_req_id": report_req_id,
                        "payload": report_content_str
                    }
                }),
                200,
                cors_headers(),
            )

        elif processing_status in ["FATAL", "CANCELLED"]:
            return (
                json.dumps({
                    "status": "ERROR_FATAL",
                    "data": {
                        "marketplace": marketplace,
                        "report_req_id": report_req_id,
                        "payload": processing_status
                    }
                }),
                200,
                cors_headers(),
            )

        else:
            return (
                json.dumps({
                    "status": "IN_PROCESS",
                    "data": {
                        "marketplace": marketplace,
                        "report_req_id": report_req_id,
                        "payload": processing_status or "IN_PROCESS"
                    }
                }),
                200,
                cors_headers(),
            )

    except Exception as e:
        return (
            json.dumps({
                "status": "ERROR_MlfReportGet",
                "data": {
                    "marketplace": marketplace,
                    "report_req_id": report_req_id,
                    "payload": str(e)
                }
            }),
            500,
            cors_headers(),
        )
def MlfReportReq(request):
    if request.method == "OPTIONS":
        return ("", 204, cors_headers())

    if request.method != "POST":
        return (
            json.dumps({
                "status": "error",
                "message": "Method not allowed"
            }),
            405,
            cors_headers(),
        )

    request_json = request.get_json(silent=True) or {}

    try:
        start_date = request_json["start_date"]
        end_date = request_json["end_date"]
        marketplace = request_json["marketplace"]

        access_token = get_access_token(marketplace)
        report_id = create_report(access_token, start_date, end_date, marketplace)

        response_body = json.dumps({
            "status": "success",
            "data": {
                "start_date": start_date,
                "end_date": end_date,
                "report_req_id": report_id
            }
        })
        return (response_body, 200, cors_headers())

    except Exception as e:
        return (
            json.dumps({
                "status": "error",
                "message": str(e)
            }),
            500,
            cors_headers(),
        )        