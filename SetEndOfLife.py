"""Toggles a SKU's end_of_life flag on asin_group_mapping, added 2026-09-22
at the user's request - a product page button to mark a product as
discontinued/end-of-life, which GetNextOrderData.py then excludes from the
Next Order page entirely (same "skip at the mapping level" pattern already
used for group == "IGNORE", but end_of_life is independent of group - a
product keeps its real group, it's just flagged separately as no longer
worth reordering).
"""
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


def find_all_by_asin(token, asin):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_MAPPING_COLLECTION}/records",
        headers={"Authorization": token},
        params={"filter": f'asin="{pb_escape(asin)}"', "perPage": 200},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
    return response.json().get("items", [])


def SetEndOfLife(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        body = request.get_json(silent=True) or {}
        sku = (body.get("sku") or "").strip()
        end_of_life = bool(body.get("endOfLife"))

        if not sku:
            return json_response({"error": "sku is required"}, 400)

        token = pb_authenticate()
        record = find_by_sku(token, sku)
        if not record:
            return json_response({"error": f"No asin_group_mapping row found for sku {sku}"}, 404)

        # Apply to every mapping row sharing this ASIN, not just the one SKU
        # spelling the button was clicked from - end_of_life is a property
        # of the product, and a product can have multiple mapping rows
        # sharing one ASIN (old/new spellings). Confirmed live 2026-09-22:
        # flagging only one spelling left a sibling spelling still showing
        # on Next Order, and un-flagging from a different spelling than the
        # one originally flagged would silently not take effect otherwise.
        asin = (record.get("asin") or "").strip()
        target_records = find_all_by_asin(token, asin) if asin else [record]

        updated_skus = []
        for rec in target_records:
            response = requests.patch(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_MAPPING_COLLECTION}/records/{rec['id']}",
                headers={"Authorization": token},
                json={"end_of_life": end_of_life},
                timeout=30,
            )
            if response.status_code != 200:
                raise RuntimeError(f"PocketBase write failed for {rec.get('sku')}: HTTP {response.status_code} - {response.text}")
            updated_skus.append(rec.get("sku"))

        return json_response({"status": "success", "asin": asin, "endOfLife": end_of_life, "updatedSkus": updated_skus})

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
