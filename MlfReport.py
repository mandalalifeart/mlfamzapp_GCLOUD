import os
import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

TOKEN_URL = "https://api.amazon.com/auth/o2/token"


REGIONS = {
    "USA": {
        "client_id_env": "CLIENT_ID_USA",
        "client_secret_env": "CLIENT_SECRET_USA",
        "refresh_token_env": "REFRESH_TOKEN_USA",
        "marketplace_id": "ATVPDKIKX0DER",
        "marketplace": Marketplaces.US,
        "reports_endpoint": "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports",
        "documents_endpoint": "https://sellingpartnerapi-na.amazon.com/reports/2020-09-04/documents",
    },
    "EU": { 
        "client_id_env": "CLIENT_ID_EU",
        "client_secret_env": "CLIENT_SECRET_EU",
        "refresh_token_env": "REFRESH_TOKEN_EU",
        "marketplace_id": "A1PA6795UKMFR9",
        "marketplace": Marketplaces.DE,
        "reports_endpoint": "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports",
        "documents_endpoint": "https://sellingpartnerapi-eu.amazon.com/reports/2020-09-04/documents",
    },
}


def get_region_config(region_name: str) -> dict:
    region_name = region_name.upper()
    if region_name not in REGIONS:
        raise ValueError(f"Unsupported region: {region_name}")

    cfg = REGIONS[region_name]

    return {
        "client_id": os.environ[cfg["client_id_env"]],
        "client_secret": os.environ[cfg["client_secret_env"]],
        "refresh_token": os.environ[cfg["refresh_token_env"]],
        "marketplace_id": cfg["marketplace_id"],
        "marketplace": cfg["marketplace"],
        "reports_endpoint": cfg["reports_endpoint"],
        "documents_endpoint": cfg["documents_endpoint"],
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