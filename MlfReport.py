import os
import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

TOKEN_URL = "https://api.amazon.com/auth/o2/token"

def get_endpoint(mp,url_type):
    if mp.region == "NA":
        return= "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/".url_type
    elif mp.region == "EU":
        return= "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/".url_type
    else:
        return= "https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/".url_type


REGIONS = {
    "USA": {
        "client_id_env": "CLIENT_ID_USA",
        "client_secret_env": "CLIENT_SECRET_USA",
        "refresh_token_env": "REFRESH_TOKEN_USA"
    },
    "EU": { 
        "client_id_env": "CLIENT_ID_EU",
        "client_secret_env": "CLIENT_SECRET_EU",
        "refresh_token_env": "REFRESH_TOKEN_EU"
   },
}

MARKETPLACE_MAP = {
    "usa": Marketplaces.US,
    "uk": Marketplaces.GB,
    "de": Marketplaces.DE,
    "fr": Marketplaces.FR,
    "it": Marketplaces.IT,
    "es": Marketplaces.ES,
    "nl": Marketplaces.NL,
    "pl": Marketplaces.PL,
    "jp": Marketplaces.JP,
}
def get_region_config(region_name: str) -> dict:
    region_name = region_name.upper()
    if region_name not in REGIONS:
        raise ValueError(f"Unsupported region: {region_name}")

    cfg = REGIONS[region_name]
    marketplace = MARKETPLACE_MAP[region_name]
    marketplace_id = marketplace.marketplace_id
    return {
        "client_id": os.environ[cfg["client_id_env"]],
        "client_secret": os.environ[cfg["client_secret_env"]],
        "refresh_token": os.environ[cfg["refresh_token_env"]],
        "marketplace_id": marketplace_id,
        "marketplace": marketplace,
        "reports_endpoint": get_endpoint(region_name,"reports"),
        "documents_endpoint": get_endpoint(region_name,"documents")
    }


def build_sp_api_credentials(config: dict) -> dict:
    return {
        "refresh_token": config["refresh_token"],
        "lwa_app_id": config["client_id"],
        "lwa_client_secret": config["client_secret"],
    }


def get_access_token(config: dict) -> str:
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": config["refresh_token"],
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
    }

    response = requests.post(TOKEN_URL, data=payload, timeout=30)
    response_data = response.json()

    if response.status_code == 200 and "access_token" in response_data:
        return response_data["access_token"]

    raise Exception(
        f"Failed to get access token: {response_data.get('error_description') or response.text}"
    )


def create_report(
    access_token: str,
    config: dict,
    data_start_time: str,
    data_end_time: str,
    report_type: str = "GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
) -> str | None:
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
    }
    ZZZ
    payload = {
        "reportType": report_type,
        "marketplaceIds": [config["marketplace_id"]],
        "dataStartTime": data_start_time,
        "dataEndTime": data_end_time,
    }

    response = requests.post(
        config["reports_endpoint"],
        headers=headers,
        json=payload,
        timeout=60,
    )
    response_data = response.json()

    if response.ok and "reportId" in response_data:
        return response_data["reportId"]

    print("Failed to create report:", response_data)
    return None


def check_report_status(config: dict, report_id: str) -> dict | None:
    credentials = build_sp_api_credentials(config)
    reports_api = Reports(credentials=credentials, marketplace=config["marketplace"])

    try:
        response = reports_api.get_report(reportId=report_id)
        return response.payload
    except Exception as e:
        print(f"Error checking report status: {e}")
        return None


def get_report_document_metadata(
    document_id: str,
    access_token: str,
    config: dict,
) -> dict:
    url = f"{config['documents_endpoint']}/{document_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-amz-access-token": access_token,
    }

    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def request_report(region_name: str, data_start_time: str, data_end_time: str) -> dict:
    config = get_region_config(region_name)
    access_token = get_access_token(config)

    report_id = create_report(
        access_token=access_token,
        config=config,
        data_start_time=data_start_time,
        data_end_time=data_end_time,
    )

    if not report_id:
        raise Exception("Failed to create report")

    return {
        "region": region_name.upper(),
        "report_id": report_id,
    }