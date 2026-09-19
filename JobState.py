"""Tiny reusable key/value store for a scheduled job's own "where did I
leave off" bookkeeping, added 2026-09-19 for AmazonRecentSalesDigest.py's
catch-up-safe window tracking (see the catch-up-safe-scheduled-jobs
convention already used by AdsReporting.py's last_recorded_date, etc.) - but
kept generic (not named after that one job) so any future live/non-stored
digest can reuse it the same way, rather than each one growing its own
one-off settings collection like next_order_settings did.

Backed by the `job_state` collection (key/value text rows, admin-only
rules, same shape as `next_order_settings`).
"""
import os

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_JOB_STATE_COLLECTION = os.environ.get("POCKETBASE_JOB_STATE_COLLECTION", "job_state")


def get_job_state(token, key):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_JOB_STATE_COLLECTION}/records",
        headers={"Authorization": token},
        params={"filter": f'key = "{key}"', "perPage": 1},
        timeout=30,
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    return items[0]["value"] if items and items[0].get("value") else None


def set_job_state(token, key, value):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_JOB_STATE_COLLECTION}/records",
        headers={"Authorization": token},
        params={"filter": f'key = "{key}"', "perPage": 1},
        timeout=30,
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    if items:
        resp = requests.patch(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_JOB_STATE_COLLECTION}/records/{items[0]['id']}",
            headers={"Authorization": token},
            json={"value": value},
            timeout=30,
        )
    else:
        resp = requests.post(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_JOB_STATE_COLLECTION}/records",
            headers={"Authorization": token},
            json={"key": key, "value": value},
            timeout=30,
        )
    resp.raise_for_status()
