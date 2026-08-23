import json
import os

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_MAPPING_COLLECTION = os.environ.get("POCKETBASE_MAPPING_COLLECTION", "asin_group_mapping")
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


def pb_escape(value):
    return value.replace("\\", "\\\\").replace('"', '\\"')


def find_by_sku(token, sku):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_MAPPING_COLLECTION}/records",
        headers={"Authorization": token},
        params={"filter": f'sku="{pb_escape(sku)}"', "perPage": 1},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
    items = response.json().get("items", [])
    return items[0] if items else None


# Assigns (or reassigns) a SKU to a product GROUP - used by the Sales page's
# "Move to Group" button on Unmapped SKUs. asin_group_mapping lives in
# PocketBase (not the old CSV), so this takes effect immediately - no redeploy.
def AssignSkuGroup(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        body = request.get_json(silent=True) or {}
        sku = (body.get("sku") or "").strip()
        asin = (body.get("asin") or "").strip()
        group = (body.get("group") or "").strip()

        if not sku:
            return json_response({"error": "sku is required"}, 400)
        if not group:
            return json_response({"error": "group is required"}, 400)

        token = pb_authenticate()
        record = find_by_sku(token, sku)

        if record:
            update_body = {"group": group}
            if asin:
                update_body["asin"] = asin
            response = requests.patch(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_MAPPING_COLLECTION}/records/{record['id']}",
                headers={"Authorization": token},
                json=update_body,
                timeout=30,
            )
        else:
            response = requests.post(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_MAPPING_COLLECTION}/records",
                headers={"Authorization": token},
                json={"sku": sku, "asin": asin, "ean": "", "group": group},
                timeout=30,
            )

        if response.status_code not in (200, 201):
            raise RuntimeError(f"PocketBase write failed: HTTP {response.status_code} - {response.text}")

        return json_response({"status": "success", "record": response.json()})

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
