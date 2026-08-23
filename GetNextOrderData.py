import json
import os
from collections import defaultdict

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_STATS_COLLECTION = os.environ.get("POCKETBASE_STATS_COLLECTION", "sku_statistics")
POCKETBASE_MAPPING_COLLECTION = os.environ.get("POCKETBASE_MAPPING_COLLECTION", "asin_group_mapping")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

STATS_FIELDS = [
    "uk_balance", "uk_on_the_way", "uk_next_shipment",
    "de_balance", "de_on_the_way", "de_next_shipment",
    "usa_balance", "usa_on_the_way", "usa_next_shipment",
    "malani_balance", "malani_order", "next_order",
]


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def fetch_mapping_records(token):
    records = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_MAPPING_COLLECTION}/records",
            headers={"Authorization": token},
            params={"perPage": 500, "page": page},
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        records.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return records


def load_mapping(token):
    # Same GROUP/IGNORE handling as GetSalesDepartmentReport.load_mapping -
    # IGNORE-group SKUs are deliberately excluded products, not candidates to reorder.
    # sku_to_asin covers every row (IGNORE included) so an sku_statistics record
    # filed under any SKU spelling - even one belonging to an ignored/retired
    # SKU string - can still be resolved to its ASIN for the join below.
    rows = []
    sku_to_asin = {}
    for row in fetch_mapping_records(token):
        sku = (row.get("sku") or "").strip()
        asin = (row.get("asin") or "").strip()
        group = (row.get("group") or "").strip() or "UNGROUPED"
        if not sku:
            continue
        if asin:
            sku_to_asin[sku] = asin
        if group == "IGNORE":
            continue
        rows.append({"sku": sku, "asin": asin, "group": group})
    return rows, sku_to_asin


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


def fetch_sku_statistics(token):
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


def GetNextOrderData(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    try:
        token = pb_authenticate()
        mapping_rows, sku_to_asin = load_mapping(token)
        stats_records = fetch_sku_statistics(token)
        stats_by_sku = {rec.get("sku"): rec for rec in stats_records if rec.get("sku")}

        # Index sku_statistics by ASIN (resolved via the mapping, not PocketBase
        # itself - the collection is still keyed by sku) so a record filed under
        # any SKU spelling sharing that ASIN is found, not just an exact SKU match.
        stats_by_asin = {}
        for rec in stats_records:
            asin = sku_to_asin.get(rec.get("sku") or "")
            if asin and asin not in stats_by_asin:
                stats_by_asin[asin] = rec

        groups = defaultdict(list)
        for row in mapping_rows:
            stats = stats_by_sku.get(row["sku"]) or stats_by_asin.get(row["asin"]) or {}
            item = {"sku": row["sku"], "asin": row["asin"]}
            for field in STATS_FIELDS:
                item[field] = stats.get(field) or 0
            groups[row["group"]].append(item)

        group_list = []
        for group, items in groups.items():
            items.sort(key=lambda i: i["sku"])
            group_list.append({"group": group, "items": items})
        group_list.sort(key=lambda g: g["group"])

        return json_response({"status": "success", "groups": group_list})

    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
