import gzip
import os
from typing import Any

import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

TOKEN_URL = "https://api.amazon.com/auth/o2/token"
REPORT_TYPE_ALL_ORDERS_BY_DATE = "GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"


REGION_CONFIGS = {
    "usa": {
        "client_id_env": "CLIENT_ID_USA",
        "client_secret_env": "CLIENT_SECRET_USA",
        "refresh_token_env": "REFRESH_TOKEN_USA",
        "marketplace": Marketplaces.US,
    },
    "de": {
        "client_id_env": "CLIENT_ID_EU",
        "client_secret_env": "CLIENT_SECRET_EU",
        "refresh_token_env": "REFRESH_TOKEN_EU",
        "marketplace": Marketplaces.DE,
    },
}


REGION_ENDPOINT_BY_SP_REGION = {
    "us-east-1": "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30",
    "eu-west-1": "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30",
    "us-west-2": "https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30",
    "fe": "https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30",
}


DB_MARKETPLACE_MAP = {
    "amazon.com": "usa",
    "amazon.ca": "ca",
    "amazon.com.mx": "mex",
    "amazon.co.uk": "gb",
    "amazon.de": "de",
    "amazon.fr": "fr",
    "amazon.it": "it",
    "amazon.es": "es",
    "amazon.se": "se",
    "amazon.com.be": "be",
    "amazon.co.jp": "jp",
    "amazon.pl": "pl",
    "amazon.nl": "nl",
    "amazon.ie": "ie",
}

EU_MARKETPLACES = {"gb", "de", "fr", "it", "es", "se", "be", "pl", "nl", "ie"}


def get_region_config(region_name: str) -> dict[str, Any]:
    key = (region_name or "").strip().lower()
    if key not in REGION_CONFIGS:
        raise ValueError(f"Unsupported marketplace region: {region_name}")

    cfg = REGION_CONFIGS[key]
    marketplace = cfg["marketplace"]
    marketplace_id = marketplace.marketplace_id
    marketplace_region = getattr(marketplace, "region", "")
    base_endpoint = REGION_ENDPOINT_BY_SP_REGION.get(
        marketplace_region,
        "https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30",
    )

    return {
        "name": key,
        "client_id": os.environ[cfg["client_id_env"]],
        "client_secret": os.environ[cfg["client_secret_env"]],
        "refresh_token": os.environ[cfg["refresh_token_env"]],
        "marketplace": marketplace,
        "marketplace_id": marketplace_id,
        "reports_endpoint": f"{base_endpoint}/reports",
        "documents_endpoint": f"{base_endpoint}/documents",
    }


def build_sp_api_credentials(config: dict[str, Any]) -> dict[str, str]:
    return {
        "refresh_token": config["refresh_token"],
        "lwa_app_id": config["client_id"],
        "lwa_client_secret": config["client_secret"],
    }


def get_access_token(config: dict[str, Any]) -> str:
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": config["refresh_token"],
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
    }

    response = requests.post(TOKEN_URL, data=payload, timeout=60)
    try:
        response_data = response.json()
    except Exception:
        response_data = {}

    if response.status_code == 200 and response_data.get("access_token"):
        return response_data["access_token"]

    raise RuntimeError(
        f"Failed to get access token: {response_data.get('error_description') or response.text}"
    )


def create_report(
    config: dict[str, Any],
    data_start_time: str,
    data_end_time: str,
    report_type: str = REPORT_TYPE_ALL_ORDERS_BY_DATE,
) -> str:
    credentials = build_sp_api_credentials(config)
    reports_api = Reports(credentials=credentials, marketplace=config["marketplace"])
    response = reports_api.create_report(
        reportType=report_type,
        dataStartTime=data_start_time,
        dataEndTime=data_end_time,
    )
    report_id = (response.payload or {}).get("reportId")
    if not report_id:
        raise RuntimeError(f"Amazon did not return reportId. Payload: {response.payload}")
    return report_id


def check_report_status(config: dict[str, Any], report_id: str) -> dict[str, Any]:
    credentials = build_sp_api_credentials(config)
    reports_api = Reports(credentials=credentials, marketplace=config["marketplace"])
    response = reports_api.get_report(reportId=report_id)
    payload = response.payload or {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected Amazon report status payload: {payload}")
    return payload


def get_report_document_metadata(
    document_id: str,
    access_token: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    url = f"{config['documents_endpoint']}/{document_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-amz-access-token": access_token,
    }
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def is_gzip_compressed(data: bytes) -> bool:
    return len(data) > 2 and data[:2] == b"\x1f\x8b"


def download_report_payload(url: str) -> str:
    response = requests.get(url, timeout=180)
    response.raise_for_status()
    content = response.content

    try:
        if is_gzip_compressed(content):
            content = gzip.decompress(content)
        return content.decode("utf-8", errors="replace")
    except Exception as exc:
        raise RuntimeError(f"Failed to decode report payload: {exc}") from exc