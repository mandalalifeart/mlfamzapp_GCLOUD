import json
import os

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_STATS_COLLECTION = os.environ.get("POCKETBASE_STATS_COLLECTION", "sku_statistics")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

# Only the columns the NextOrder page lets a user edit - everything else in
# sku_statistics is written by the import pipelines, not from the browser.
EDITABLE_FIELDS = {"uk_next_shipment", "de_next_shipment", "usa_next_shipment", "next_order"}


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase auth failed: HTTP {response.status_code} - {response.text}")
    token = response.json().get("token")
    if not token:
        raise RuntimeError("PocketBase auth response missing token")
    return token


def find_record(token, sku):
    escaped = sku.replace("\\", "\\\\").replace('"', '\\"')
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records",
        headers={"Authorization": token},
        params={"filter": f'sku="{escaped}"', "perPage": 1},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
    items = response.json().get("items", [])
    return items[0] if items else None


def UpdateNextOrderField(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        body = request.get_json(silent=True) or {}
        sku = (body.get("sku") or "").strip()
        field = (body.get("field") or "").strip()
        value = body.get("value")

        if not sku:
            return json_response({"error": "sku is required"}, 400)
        if field not in EDITABLE_FIELDS:
            return json_response({"error": f"field must be one of {sorted(EDITABLE_FIELDS)}"}, 400)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            return json_response({"error": "value must be a non-negative number"}, 400)

        token = pb_authenticate()
        record = find_record(token, sku)

        if record:
            response = requests.patch(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records/{record['id']}",
                headers={"Authorization": token},
                json={field: value},
                timeout=30,
            )
        else:
            response = requests.post(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records",
                headers={"Authorization": token},
                json={"sku": sku, field: value},
                timeout=30,
            )

        if response.status_code not in (200, 201):
            raise RuntimeError(f"PocketBase write failed: HTTP {response.status_code} - {response.text}")

        return json_response({"status": "success", "record": response.json()})

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
