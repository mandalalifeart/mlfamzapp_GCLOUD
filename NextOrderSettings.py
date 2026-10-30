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


SHIPMENT_REGIONS = ("usa", "de", "uk", "au", "jp")


def get_next_shipment_dates(token):
    """Per-region next shipment arriving dates (2026-10-18, per the user
    "split next shipment arriving date for usa/de/uk"): keys
    next_shipment_date_<region>; a region never set falls back to the old
    single date. Returns {"usa": "YYYY-MM-DD", "de": ..., "uk": ...}."""
    shared = get_or_create_next_shipment_date(token)
    dates = {}
    for region in SHIPMENT_REGIONS:
        record = fetch_setting_record(token, f"{NEXT_SHIPMENT_DATE_KEY}_{region}")
        dates[region] = (record or {}).get("value") or shared
    return dates


COVER_UNTIL_KEY = "cover_until"
DEFAULT_COVER_MONTHS = 3


def get_cover_until_dates(token):
    """Per-region "shipment covers until" date (2026-10-18, per the user "de
    shipment is till 1.9.2027"): keys cover_until_<region>. Missing = the old
    3 months after arrival. Returns {region: "YYYY-MM-DD" or None}."""
    return {region: (fetch_setting_record(token, f"{COVER_UNTIL_KEY}_{region}") or {}).get("value") or None
            for region in SHIPMENT_REGIONS}


def get_cover_months(token):
    """Per-region "goods should last N months after arrival" (2026-10-18, per
    the user) - keys cover_months_<region>; wins over cover_until.
    Returns {region: int or None}."""
    out = {}
    for region in SHIPMENT_REGIONS:
        value = (fetch_setting_record(token, f"cover_months_{region}") or {}).get("value")
        out[region] = int(value) if value and str(value).isdigit() and int(value) > 0 else None
    return out


def region_cover_months(token, region_dates):
    """{region: months the next shipment must cover}: cover_months, else the
    cover_until date, else 3."""
    months = get_cover_months(token)
    until = get_cover_until_dates(token)
    return {r: months.get(r) or cover_months_count(d, date.fromisoformat(until[r]) if until.get(r) else None)
            for r, d in region_dates.items()}


def cover_months_count(arrival, cover_until):
    """Calendar months the shipment must cover: arrival month up to the
    cover-until month (exclusive when cover-until is the 1st, e.g. arrival
    1 Apr, until 1 Sep -> Apr..Aug = 5). No cover-until -> 3."""
    if not cover_until:
        return DEFAULT_COVER_MONTHS
    n = (cover_until.year - arrival.year) * 12 + (cover_until.month - arrival.month) + (1 if cover_until.day > 1 else 0)
    return max(1, n)


def set_setting(token, key, value):
    record = fetch_setting_record(token, key)
    if record:
        response = requests.patch(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SETTINGS_COLLECTION}/records/{record['id']}",
            headers={"Authorization": token}, json={"value": value}, timeout=30)
    else:
        response = requests.post(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SETTINGS_COLLECTION}/records",
            headers={"Authorization": token}, json={"key": key, "value": value}, timeout=30)
    if response.status_code not in (200, 201):
        raise RuntimeError(f"PocketBase write failed: HTTP {response.status_code} - {response.text}")


def set_next_shipment_date(token, date_str, region=None):
    key = f"{NEXT_SHIPMENT_DATE_KEY}_{region}" if region else NEXT_SHIPMENT_DATE_KEY
    record = fetch_setting_record(token, key)
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
            json={"key": key, "value": date_str},
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
        if "coverMonths" in body:  # per-region months the goods must last; "" clears it
            region = (body.get("region") or "").strip().lower()
            value = str(body.get("coverMonths") or "").strip()
            if region not in SHIPMENT_REGIONS:
                return json_response({"error": f"region must be one of {SHIPMENT_REGIONS}"}, 400)
            if value and not (value.isdigit() and 1 <= int(value) <= 24):
                return json_response({"error": "coverMonths must be a whole number 1-24"}, 400)
            token = pb_authenticate()
            set_setting(token, f"cover_months_{region}", value)
            return json_response({"status": "success", "coverMonths": get_cover_months(token)})
        if "coverUntil" in body:  # per-region "covers until" date; "" clears it
            region = (body.get("region") or "").strip().lower()
            value = (body.get("coverUntil") or "").strip()
            if region not in SHIPMENT_REGIONS:
                return json_response({"error": f"region must be one of {SHIPMENT_REGIONS}"}, 400)
            if value:
                try:
                    date.fromisoformat(value)
                except ValueError:
                    return json_response({"error": "coverUntil must be an ISO YYYY-MM-DD string"}, 400)
            token = pb_authenticate()
            set_setting(token, f"{COVER_UNTIL_KEY}_{region}", value)
            return json_response({"status": "success", "coverUntil": get_cover_until_dates(token)})
        date_str = (body.get("date") or "").strip()
        try:
            date.fromisoformat(date_str)
        except ValueError:
            return json_response({"error": "date must be an ISO YYYY-MM-DD string"}, 400)

        region = (body.get("region") or "").strip().lower() or None
        if region and region not in SHIPMENT_REGIONS:
            return json_response({"error": f"region must be one of {SHIPMENT_REGIONS}"}, 400)

        token = pb_authenticate()
        set_next_shipment_date(token, date_str, region)
        return json_response({"status": "success", "nextShipmentDate": date_str, "region": region,
                              "nextShipmentDates": get_next_shipment_dates(token)})

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
