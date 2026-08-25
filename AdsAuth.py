import json
import os

import requests

AD_CLIENT_ID_USA = os.environ["AD_CLIENT_ID_USA"]
AD_CLIENT_SECRET_USA = os.environ["AD_CLIENT_SECRET_USA"]
POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_ADS_COLLECTION = os.environ.get("POCKETBASE_ADS_COLLECTION", "ads_connections")

ALLOWED_ORIGIN = "https://mlfamzappfire.web.app"
FRONTEND_ADS_URL = f"{ALLOWED_ORIGIN}/ads"
# Must exactly match an "Allowed Return URL" registered on the Amazon Ads
# Security Profile (Login with Amazon) - this is the Gen2 Cloud Function's
# default cloudfunctions.net URL, same pattern as API_BASE elsewhere.
REDIRECT_URI = os.environ.get(
    "ADS_REDIRECT_URI",
    "https://us-central1-mlfamzapp.cloudfunctions.net/AdsOAuthCallback",
)

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
# One LWA grant covers all regions the Ads account has profiles in - each
# region has its own Advertising API host, so profile discovery has to hit
# all three rather than picking one.
ADS_REGION_ENDPOINTS = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=15,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase auth failed: HTTP {response.status_code} - {response.text}")
    token = response.json().get("token")
    if not token:
        raise RuntimeError("PocketBase auth response missing token")
    return token


def get_connection_record(token):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_COLLECTION}/records",
        headers={"Authorization": token},
        params={"perPage": 1},
        timeout=15,
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    return items[0] if items else None


def save_connection(token, fields):
    existing = get_connection_record(token)
    headers = {"Authorization": token, "Content-Type": "application/json"}
    if existing:
        response = requests.patch(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_COLLECTION}/records/{existing['id']}",
            headers=headers,
            json=fields,
            timeout=15,
        )
    else:
        response = requests.post(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ADS_COLLECTION}/records",
            headers=headers,
            json=fields,
            timeout=15,
        )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"PocketBase write failed: HTTP {response.status_code} - {response.text}")


def exchange_code_for_tokens(code):
    response = requests.post(
        LWA_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AD_CLIENT_ID_USA,
            "client_secret": AD_CLIENT_SECRET_USA,
            "redirect_uri": REDIRECT_URI,
        },
        timeout=15,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Token exchange failed: HTTP {response.status_code} - {response.text}")
    return response.json()


def discover_profiles(access_token):
    profiles = []
    errors = []
    for region, base_url in ADS_REGION_ENDPOINTS.items():
        try:
            response = requests.get(
                f"{base_url}/v2/profiles",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Amazon-Advertising-API-ClientId": AD_CLIENT_ID_USA,
                },
                timeout=15,
            )
            if response.status_code != 200:
                errors.append(f"{region}: HTTP {response.status_code}")
                continue
            for profile in response.json():
                profiles.append({
                    "region": region,
                    "profileId": profile.get("profileId"),
                    "countryCode": profile.get("countryCode"),
                    "currencyCode": profile.get("currencyCode"),
                    "accountName": (profile.get("accountInfo") or {}).get("name"),
                    "accountType": (profile.get("accountInfo") or {}).get("type"),
                })
        except requests.RequestException as exc:
            errors.append(f"{region}: {exc}")
    return profiles, errors


def AdsOAuthCallback(request):
    error = request.args.get("error")
    code = request.args.get("code")

    if error:
        return "", 302, {"Location": f"{FRONTEND_ADS_URL}?error={error}"}
    if not code:
        return "", 302, {"Location": f"{FRONTEND_ADS_URL}?error=missing_code"}

    try:
        tokens = exchange_code_for_tokens(code)
        refresh_token = tokens.get("refresh_token")
        access_token = tokens.get("access_token")
        if not refresh_token or not access_token:
            raise RuntimeError("Token response missing refresh_token/access_token")

        profiles, profile_errors = discover_profiles(access_token)

        pb_token = pb_authenticate()
        save_connection(pb_token, {
            "region": "ALL",
            "refresh_token": refresh_token,
            "status": "connected",
            "profiles": profiles,
            "last_error": "; ".join(profile_errors) if profile_errors else "",
        })

        return "", 302, {"Location": f"{FRONTEND_ADS_URL}?connected=1"}
    except Exception as exc:
        try:
            pb_token = pb_authenticate()
            save_connection(pb_token, {
                "region": "ALL",
                "refresh_token": "",
                "status": "error",
                "profiles": [],
                "last_error": str(exc),
            })
        except Exception:
            pass
        return "", 302, {"Location": f"{FRONTEND_ADS_URL}?error={requests.utils.quote(str(exc))}"}


def GetAdsConnectionStatus(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    try:
        token = pb_authenticate()
        record = get_connection_record(token)
        connected = bool(record) and record.get("status") == "connected"
        return json_response({
            "connected": connected,
            "status": (record.get("status") if record else "not_connected"),
            "lastError": (record.get("last_error") if record else "") or "",
            "profiles": (record.get("profiles") if connected else []) or [],
            "authorizeUrl": build_authorize_url(),
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def build_authorize_url():
    from urllib.parse import urlencode

    params = {
        "client_id": AD_CLIENT_ID_USA,
        "scope": "advertising::campaign_management",
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
    }
    return f"https://www.amazon.com/ap/oa?{urlencode(params)}"
