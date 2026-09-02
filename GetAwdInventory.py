import json
import os

CLIENT_ID_USA = os.environ["CLIENT_ID_USA"]
CLIENT_SECRET_USA = os.environ["CLIENT_SECRET_USA"]
REFRESH_TOKEN_USA = os.environ["REFRESH_TOKEN_USA"]
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def GetAwdInventory(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        from sp_api.api import AmazonWarehousingAndDistribution
        from sp_api.base import Marketplaces

        credentials = {
            "refresh_token": REFRESH_TOKEN_USA,
            "lwa_app_id": CLIENT_ID_USA,
            "lwa_client_secret": CLIENT_SECRET_USA,
        }
        client = AmazonWarehousingAndDistribution(credentials=credentials, marketplace=Marketplaces.US)

        # maxResults must be passed explicitly - Amazon silently defaults to
        # 25 with no nextToken at all when omitted (confirmed 2026-08-31:
        # this account has 91 real AWD SKUs, matching Seller Central's own
        # AWD inventory list, but omitting maxResults returned exactly 25
        # with the response's "inventory" key as the only top-level key -
        # no pagination signal that more existed). nextToken (top-level on
        # the response, not nested under "pagination") is still honored
        # below for any future page beyond one maxResults=200 batch.
        inventory = []
        next_token = None
        while True:
            kwargs = {"maxResults": 200}
            if next_token:
                kwargs["nextToken"] = next_token
            response = client.list_inventory(**kwargs)
            payload = response.payload or {}
            inventory.extend(payload.get("inventory", []))
            next_token = payload.get("nextToken")
            if not next_token:
                break

        return json_response({"status": "success", "rowCount": len(inventory), "inventory": inventory})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
