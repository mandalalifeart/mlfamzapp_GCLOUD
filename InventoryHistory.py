"""Shared helper for writing weekly per-SKU inventory snapshots into the
region-generic `sku_inventory_history` collection (sku, asin, region,
balance, week_date) - added 2026-09-20 so the Reco formula can later exclude
weeks where a SKU had zero stock from its sales-velocity denominator (a
stockout week can't produce real sales no matter the true demand, so
counting it as a normal sales day would understate velocity). Used by both
WeeklyUsaInventorySync.py (region="usa") and WeeklyEuInventorySync.py
(region="uk"/"de"). This only starts building real history from whenever
each region's weekly sync first runs it - there's no way to know past
weeks' stock levels retroactively.
"""
import os

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_INVENTORY_HISTORY_COLLECTION = os.environ.get("POCKETBASE_INVENTORY_HISTORY_COLLECTION", "sku_inventory_history")
POCKETBASE_BATCH_SIZE = 50


def pb_batch(token, batch_requests):
    for i in range(0, len(batch_requests), POCKETBASE_BATCH_SIZE):
        chunk = batch_requests[i:i + POCKETBASE_BATCH_SIZE]
        response = requests.post(
            f"{POCKETBASE_URL}/api/batch",
            headers={"Authorization": token},
            json={"requests": chunk},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase batch failed: HTTP {response.status_code} - {response.text}")
        results = response.json()
        failed = [r for r in results if not (200 <= r.get("status", 0) < 300)]
        if failed:
            raise RuntimeError(f"PocketBase batch had {len(failed)}/{len(results)} failed op(s): {failed[:3]}")


def write_inventory_history_snapshot(token, written_records, region, week_date, balance_field):
    """written_records: the `written` list from a sync_*_inventory() call -
    each entry must carry sku+asin+<balance_field> from the real pull.
    balance_field is the sku_statistics field name that call wrote (e.g.
    "usa_balance", "uk_balance", "de_balance") - its value becomes this
    snapshot's `balance`."""
    requests_body = [
        {
            "method": "POST",
            "url": f"/api/collections/{POCKETBASE_INVENTORY_HISTORY_COLLECTION}/records",
            "body": {
                "sku": rec.get("sku"),
                "asin": rec.get("asin", ""),
                "region": region,
                "balance": rec.get(balance_field) or 0,
                "week_date": week_date,
            },
        }
        for rec in written_records
        if rec.get("sku")
    ]
    pb_batch(token, requests_body)
