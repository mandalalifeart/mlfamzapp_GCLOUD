import os

import requests

from AdsAuth import AD_PROFILES, ADS_REGION_ENDPOINTS, cors_headers, json_response
from AdsReporting import ADMIN_KEY, pb_authenticate, pb_list_connected, refresh_access_token


def ProbeAdsReportColumns(request):
    """One-off diagnostic (not part of any pipeline): submits a report
    request with a candidate column added, to see whether Amazon accepts or
    rejects it - Amazon validates columns at submission time and returns the
    full allowed-values list in its 400 body, so this is fast (no waiting
    for report generation) and safe (no report is ever actually generated
    for an invalid column)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    report_type_id = request.args.get("report_type_id") if hasattr(request, "args") else None
    ad_product = request.args.get("ad_product") if hasattr(request, "args") else None
    columns_param = request.args.get("columns") if hasattr(request, "args") else None
    if not report_type_id or not ad_product or not columns_param:
        return json_response({"error": "report_type_id, ad_product, and columns (comma-separated) are required"}, 400)
    columns = columns_param.split(",")

    try:
        pb_token = pb_authenticate()
        connections = pb_list_connected(pb_token)
        if not connections:
            return json_response({"error": "No Ads connections"}, 400)

        connection = connections[0]
        profile_key = connection.get("region")
        access_token = refresh_access_token(profile_key, connection.get("refresh_token"))
        client_id = AD_PROFILES[profile_key]["client_id"]

        profiles = [p for p in connection.get("profiles", []) or [] if p.get("accountType") != "agency"]
        if not profiles:
            return json_response({"error": "No usable profiles on this connection"}, 400)
        ads_profile = profiles[0]
        region = ads_profile.get("region")
        base_url = ADS_REGION_ENDPOINTS.get(region)
        ads_profile_id = ads_profile.get("profileId")

        response = requests.post(
            f"{base_url}/reporting/reports",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Amazon-Advertising-API-ClientId": client_id,
                "Amazon-Advertising-API-Scope": str(ads_profile_id),
                "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            },
            json={
                "name": f"column probe {report_type_id}",
                "startDate": "2026-08-25",
                "endDate": "2026-08-25",
                "configuration": {
                    "adProduct": ad_product,
                    "groupBy": ["targeting"],
                    "columns": columns,
                    "reportTypeId": report_type_id,
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            },
            timeout=30,
        )
        return json_response({
            "statusCode": response.status_code,
            "accepted": response.status_code in (200, 202),
            "body": response.text,
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
