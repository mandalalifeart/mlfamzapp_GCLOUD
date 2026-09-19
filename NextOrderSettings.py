"""Small key/value settings for the Next Order page, added 2026-09-19 at the
user's request for an adjustable, persisted "next shipment date" shown at
the top of the page (used by GetNextOrderData.py to compute the USA
Recommendation to Order column - see that file for the actual formula).

Backed by the `next_order_settings` collection (key/value text rows, admin-
only rules, no schema beyond that - a deliberately generic single-row-per-
key store rather than a one-off column bolted onto sku_statistics, since a
"next shipment date" is a page-level setting, not a per-SKU one).
"""
import json
import os
from datetime import date, timedelta

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_SETTINGS_COLLECTION = os.environ.get("POCKETBASE_SETTINGS_COLLECTION", "next_order_settings")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

NEXT_SHIPMENT_DATE_KEY = "next_shipment_date"
# Used only the very first time this is read, before the user has ever set a
# real date - not a business rule, just a harmless placeholder so the page
# has something to show instead of blank/null on first load.
DEFAULT_LEAD_DAYS = 90


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


def fetch_setting_record(token, key):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SETTINGS_COLLECTION}/records",
        headers={"Authorization": token},
        params={"filter": f'key = "{key}"', "perPage": 1},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
    items = response.json().get("items", [])
    return items[0] if items else None


def get_or_create_next_shipment_date(token):
    """Returns the stored next-shipment-date as an ISO "YYYY-MM-DD" string,
    creating a default (today + DEFAULT_LEAD_DAYS) the first time it's ever
    read so GetNextOrderData always has a real date to compute against."""
    record = fetch_setting_record(token, NEXT_SHIPMENT_DATE_KEY)
    if record and record.get("value"):
        return record["value"]

    default_date = (date.today() + timedelta(days=DEFAULT_LEAD_DAYS)).isoformat()
    if record:
        requests.patch(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SETTINGS_COLLECTION}/records/{record['id']}",
            headers={"Authorization": token},
            json={"value": default_date},
            timeout=30,
        )
    else:
        requests.post(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SETTINGS_COLLECTION}/records",
            headers={"Authorization": token},
            json={"key": NEXT_SHIPMENT_DATE_KEY, "value": default_date},
            timeout=30,
        )
    return default_date


def set_next_shipment_date(token, date_str):
    record = fetch_setting_record(token, NEXT_SHIPMENT_DATE_KEY)
    if record:
        response = requests.patch(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SETTINGS_COLLECTION}/records/{record['id']}",
            headers={"Authorization": token},
            json={"value": date_str},
            timeout=30,
        )
    else:
        response = requests.post(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SETTINGS_COLLECTION}/records",
            headers={"Authorization": token},
            json={"key": NEXT_SHIPMENT_DATE_KEY, "value": date_str},
            timeout=30,
        )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"PocketBase write failed: HTTP {response.status_code} - {response.text}")


def UpdateNextShipmentDate(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        body = request.get_json(silent=True) or {}
        date_str = (body.get("date") or "").strip()
        try:
            date.fromisoformat(date_str)
        except ValueError:
            return json_response({"error": "date must be an ISO YYYY-MM-DD string"}, 400)

        token = pb_authenticate()
        set_next_shipment_date(token, date_str)
        return json_response({"status": "success", "nextShipmentDate": date_str})

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
