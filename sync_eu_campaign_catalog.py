"""One-off catalog sync: populates `ads_campaigns` (and `ads_portfolios`) with
the real EU campaign catalog from the manually-imported EU Campaign report
CSVs. Added 2026-08-29 after confirming a real gap: GetAdsCampaignStats seeds
its response from `ads_campaigns` (a live snapshot collection), but that
collection only had 3 stale DE rows (different campaign_ids than the real
ones with actual spend, left over from an earlier broken EU API sync) - so
every real EU campaign was silently missing from /ads-campaigns even though
ads_campaign_stats had real data for them. Unlike the historical stats
collections, `ads_campaigns` is a snapshot ("what does the account currently
look like"), so this uses the union of all 6 months' Campaign CSVs to build
the most complete/current picture, with a later month's data overwriting an
earlier month's for the same campaign_id.

Usage: python3 sync_eu_campaign_catalog.py <country-map.csv> <campaign1.csv> [<campaign2.csv> ...]
"""
import csv
import os
import sys

import requests

from import_manual_ads_report import (
    AD_PRODUCT_MAP,
    COUNTRY_PROFILE_MAP,
    load_country_mapping,
    resolve_country,
    strip_id,
)

POCKETBASE_URL = os.environ["POCKETBASE_URL"]
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]

EU_COUNTRIES = list(COUNTRY_PROFILE_MAP.keys())  # IT, ES, UK, DE, FR, PL, SE, NL


def pb_authenticate():
    r = requests.post(f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
                       json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD}, timeout=30)
    r.raise_for_status()
    return r.json()["token"]


def pb_batch(token, batch):
    if not batch:
        return
    r = requests.post(f"{POCKETBASE_URL}/api/batch", headers={"Authorization": token}, json={"requests": batch}, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"batch failed: HTTP {r.status_code} - {r.text}")
    for entry, result in zip(batch, r.json()):
        if result.get("status", 500) >= 400:
            raise RuntimeError(f"batch item failed ({entry['method']} {entry['url']}): {result.get('body')}")


def pb_delete_all(token, collection, filter_str):
    deleted = 0
    while True:
        r = requests.get(f"{POCKETBASE_URL}/api/collections/{collection}/records",
                          headers={"Authorization": token},
                          params={"filter": filter_str, "fields": "id", "perPage": 200}, timeout=30)
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            break
        for i in range(0, len(items), 50):
            chunk = items[i:i + 50]
            pb_batch(token, [{"method": "DELETE", "url": f"/api/collections/{collection}/records/{it['id']}"} for it in chunk])
            deleted += len(chunk)
    return deleted


def main():
    country_map_path = sys.argv[1]
    campaign_csvs = sys.argv[2:]

    by_np, by_n = load_country_mapping(country_map_path)

    campaigns = {}  # campaign_id -> row dict
    portfolios = {}  # portfolio_id -> {"name":..., "country_code":..., "profile_id":...}

    for path in campaign_csvs:
        with open(path, newline="", encoding="utf-8-sig", errors="replace") as fh:
            for row in csv.DictReader(fh):
                country_code = resolve_country(row, by_np, by_n)
                if country_code is None:
                    continue  # USD/MXN (USA-covered) or unresolved
                campaign_id = strip_id(row.get("Campaign ID"))
                profile_id = COUNTRY_PROFILE_MAP.get(country_code, "")
                portfolio_id = strip_id(row.get("Portfolio ID"))
                portfolio_name = row.get("Portfolio name", "")

                campaigns[campaign_id] = {
                    "profile_id": profile_id,
                    "campaign_id": campaign_id,
                    "campaign_name": row.get("Campaign name", ""),
                    "campaign_status": "ENABLED",
                    "ad_product": AD_PRODUCT_MAP.get((row.get("Ad product") or "").strip(), row.get("Ad product") or ""),
                    "country_code": country_code,
                    "currency_code": row.get("Budget currency", ""),
                    "portfolio_id": portfolio_id if portfolio_id and portfolio_id != "-1" else "",
                }
                if portfolio_id and portfolio_id != "-1" and portfolio_name and portfolio_name != "No Portfolio":
                    portfolios[portfolio_id] = {"name": portfolio_name, "country_code": country_code, "profile_id": profile_id}

    print(f"Resolved {len(campaigns)} unique EU campaigns, {len(portfolios)} unique EU portfolios across {len(campaign_csvs)} files")

    token = pb_authenticate()

    country_filter = " || ".join(f'country_code = "{c}"' for c in EU_COUNTRIES)
    deleted_campaigns = pb_delete_all(token, "ads_campaigns", country_filter)
    print(f"deleted {deleted_campaigns} stale ads_campaigns rows for EU countries")

    batch = []
    for c in campaigns.values():
        batch.append({"method": "POST", "url": "/api/collections/ads_campaigns/records", "body": c})
        if len(batch) >= 50:
            pb_batch(token, batch)
            batch = []
    pb_batch(token, batch)
    print(f"wrote {len(campaigns)} ads_campaigns rows")

    deleted_portfolios = pb_delete_all(token, "ads_portfolios", country_filter)
    print(f"deleted {deleted_portfolios} stale ads_portfolios rows for EU countries")

    batch = []
    for pid, p in portfolios.items():
        batch.append({"method": "POST", "url": "/api/collections/ads_portfolios/records",
                       "body": {"portfolio_id": pid, "name": p["name"], "state": "ENABLED",
                                "country_code": p["country_code"], "profile_id": p["profile_id"]}})
        if len(batch) >= 50:
            pb_batch(token, batch)
            batch = []
    pb_batch(token, batch)
    print(f"wrote {len(portfolios)} ads_portfolios rows")


if __name__ == "__main__":
    main()
