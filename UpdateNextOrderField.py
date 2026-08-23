import csv
import json
import os
from pathlib import Path

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_STATS_COLLECTION = os.environ.get("POCKETBASE_STATS_COLLECTION", "sku_statistics")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

MAPPING_CSV_PATH = Path(__file__).with_name("asin_group_mapping.csv")

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


def load_sku_to_asin():
    sku_to_asin = {}
    with open(MAPPING_CSV_PATH, newline="") as f:
        for row in csv.DictReader(f):
            sku = (row.get("SKU") or "").strip()
            asin = (row.get("ASIN") or "").strip()
            if sku and asin:
                sku_to_asin[sku] = asin
    return sku_to_asin


def fetch_all_stats(token):
    records = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records",
            headers={"Authorization": token},
            params={"perPage": 500, "page": page},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        records.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return records


# sku_statistics is still keyed by sku (no schema change), but a value edited
# on the NextOrder page for one SKU spelling should land on the SAME record as
# any other SKU spelling sharing that product's ASIN - otherwise editing
# PAREO_ACA_6B_NEW vs PAREO_ACA_6B_OLD would silently fork into two rows for
# one physical product. Resolve by ASIN (via the mapping) before falling back
# to creating a brand-new record under this exact SKU string.
def find_record(token, sku, sku_to_asin):
    stats_records = fetch_all_stats(token)
    by_sku = {rec.get("sku"): rec for rec in stats_records if rec.get("sku")}

    if sku in by_sku:
        return by_sku[sku]

    asin = sku_to_asin.get(sku)
    if not asin:
        return None

    for candidate_sku, rec in by_sku.items():
        if sku_to_asin.get(candidate_sku) == asin:
            return rec
    return None


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

        sku_to_asin = load_sku_to_asin()
        token = pb_authenticate()
        record = find_record(token, sku, sku_to_asin)

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
