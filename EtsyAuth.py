import base64
import hashlib
import json
import os
import secrets

import requests

ETSY_KEYSTRING = os.environ["ETSY_Keystring"]
ETSY_SHARED_SECRET = os.environ["ETSY_SHARED_SECRET"]
POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_ETSY_STATE_COLLECTION = os.environ.get("POCKETBASE_ETSY_STATE_COLLECTION", "etsy_oauth_state")
POCKETBASE_ETSY_CONNECTIONS_COLLECTION = os.environ.get("POCKETBASE_ETSY_CONNECTIONS_COLLECTION", "etsy_connections")

ALLOWED_ORIGIN = "https://mlfamzappfire.web.app"
FRONTEND_ETSY_URL = f"{ALLOWED_ORIGIN}/etsy"
# Must exactly match a registered "Redirect URI" on the Etsy app (developer
# portal) - this is the Gen2 Cloud Function's default cloudfunctions.net URL,
# same pattern as the Amazon Ads callback.
REDIRECT_URI = os.environ.get(
    "ETSY_REDIRECT_URI",
    "https://us-central1-mlfamzapp.cloudfunctions.net/EtsyOAuthCallback",
)

ETSY_AUTHORIZE_URL = "https://www.etsy.com/oauth/connect"
ETSY_TOKEN_URL = "https://api.etsy.com/v3/public/oauth/token"
ETSY_API_BASE = "https://api.etsy.com/v3/application"
# transactions_w added 2026-08-26 so UpdateEtsyTrackingFromAmazon can push
# tracking numbers back to Etsy (createReceiptShipment) - everything else
# here is still read-only. Existing connections authorized before this change
# only carry the old read-only scopes; the refresh token doesn't gain the new
# scope automatically, so the user must reconnect via the /etsy page once for
# tracking pushes to start working (a 403 with an insufficient_scope-style
# error from Etsy is the symptom if this step is skipped).
ETSY_SCOPES = "listings_r shops_r transactions_r transactions_w"


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def api_key_header():
    # Etsy's x-api-key combines the app's keystring and shared secret with a
    # colon, distinct from the OAuth bearer token used for the acting user.
    return f"{ETSY_KEYSTRING}:{ETSY_SHARED_SECRET}"


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


def pb_get_connection(token):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ETSY_CONNECTIONS_COLLECTION}/records",
        headers={"Authorization": token},
        params={"perPage": 1},
        timeout=15,
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    return items[0] if items else None


def pb_save_connection(token, fields):
    existing = pb_get_connection(token)
    headers = {"Authorization": token, "Content-Type": "application/json"}
    if existing:
        response = requests.patch(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ETSY_CONNECTIONS_COLLECTION}/records/{existing['id']}",
            headers=headers, json=fields, timeout=15,
        )
    else:
        response = requests.post(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ETSY_CONNECTIONS_COLLECTION}/records",
            headers=headers, json=fields, timeout=15,
        )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"PocketBase write failed: HTTP {response.status_code} - {response.text}")


def generate_pkce_pair():
    code_verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).decode("utf-8").rstrip("=")
    code_verifier = code_verifier[:128]
    digest = hashlib.sha256(code_verifier.encode("utf-8")).digest()
    code_challenge = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
    return code_verifier, code_challenge


def refresh_access_token(refresh_token):
    response = requests.post(
        ETSY_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": ETSY_KEYSTRING,
            "refresh_token": refresh_token,
        },
        timeout=15,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Etsy token refresh failed: HTTP {response.status_code} - {response.text}")
    body = response.json()
    access_token = body.get("access_token")
    new_refresh_token = body.get("refresh_token")
    if not access_token:
        raise RuntimeError(f"Etsy token refresh response missing access_token: {body}")
    return access_token, new_refresh_token or refresh_token


def EtsyOAuthStart(request):
    code_verifier, code_challenge = generate_pkce_pair()
    state = secrets.token_urlsafe(24)

    pb_token = pb_authenticate()
    requests.post(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ETSY_STATE_COLLECTION}/records",
        headers={"Authorization": pb_token, "Content-Type": "application/json"},
        json={"state": state, "code_verifier": code_verifier},
        timeout=15,
    )

    from urllib.parse import urlencode
    params = {
        "response_type": "code",
        "client_id": ETSY_KEYSTRING,
        "redirect_uri": REDIRECT_URI,
        "scope": ETSY_SCOPES,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return "", 302, {"Location": f"{ETSY_AUTHORIZE_URL}?{urlencode(params)}"}


def EtsyOAuthCallback(request):
    error = request.args.get("error")
    code = request.args.get("code")
    state = request.args.get("state")

    if error:
        return "", 302, {"Location": f"{FRONTEND_ETSY_URL}?error={error}"}
    if not code or not state:
        return "", 302, {"Location": f"{FRONTEND_ETSY_URL}?error=missing_code_or_state"}

    try:
        pb_token = pb_authenticate()

        state_response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ETSY_STATE_COLLECTION}/records",
            headers={"Authorization": pb_token},
            params={"filter": f'state = "{state}"', "perPage": 1},
            timeout=15,
        )
        state_response.raise_for_status()
        state_items = state_response.json().get("items", [])
        if not state_items:
            raise RuntimeError("Unknown or expired OAuth state")
        code_verifier = state_items[0]["code_verifier"]
        requests.delete(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ETSY_STATE_COLLECTION}/records/{state_items[0]['id']}",
            headers={"Authorization": pb_token},
            timeout=15,
        )

        token_response = requests.post(
            ETSY_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "client_id": ETSY_KEYSTRING,
                "redirect_uri": REDIRECT_URI,
                "code": code,
                "code_verifier": code_verifier,
            },
            timeout=15,
        )
        if token_response.status_code != 200:
            raise RuntimeError(f"Token exchange failed: HTTP {token_response.status_code} - {token_response.text}")
        token_body = token_response.json()
        access_token = token_body.get("access_token")
        refresh_token = token_body.get("refresh_token")
        if not access_token or not refresh_token:
            raise RuntimeError(f"Token response missing access_token/refresh_token: {token_body}")

        user_id = access_token.split(".")[0]

        shops_response = requests.get(
            f"{ETSY_API_BASE}/users/{user_id}/shops",
            headers={"x-api-key": api_key_header(), "Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        if shops_response.status_code != 200:
            raise RuntimeError(f"Shop lookup failed: HTTP {shops_response.status_code} - {shops_response.text}")
        shop = shops_response.json()
        shop_id = str(shop.get("shop_id", ""))
        shop_name = shop.get("shop_name", "")

        pb_save_connection(pb_token, {
            "shop_id": shop_id,
            "shop_name": shop_name,
            "user_id": user_id,
            "refresh_token": refresh_token,
            "status": "connected",
            "last_error": "",
        })

        return "", 302, {"Location": f"{FRONTEND_ETSY_URL}?connected=1"}
    except Exception as exc:
        try:
            pb_token = pb_authenticate()
            pb_save_connection(pb_token, {"status": "error", "last_error": str(exc)})
        except Exception:
            pass
        return "", 302, {"Location": f"{FRONTEND_ETSY_URL}?error={requests.utils.quote(str(exc))}"}


def GetEtsyConnectionStatus(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    try:
        token = pb_authenticate()
        record = pb_get_connection(token)
        connected = bool(record) and record.get("status") == "connected"
        return json_response({
            "connected": connected,
            "status": (record.get("status") if record else "not_connected"),
            "lastError": (record.get("last_error") if record else "") or "",
            "shopId": (record.get("shop_id") if connected else "") or "",
            "shopName": (record.get("shop_name") if connected else "") or "",
            "authorizeUrl": "https://us-central1-mlfamzapp.cloudfunctions.net/EtsyOAuthStart",
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
