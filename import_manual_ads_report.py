"""One-off/reusable importer for Amazon Ads console CSV exports (Search Term,
Campaign, Targeting, Advertised Product report types) into the matching
PocketBase collection, for months outside Amazon's ~60-day Reporting API
retention window. Added 2026-08-29 per the user's standing rule: automated
pipelines never touch AmzBot ads data for months before July 2026 - only a
manual report upload (this script) may write/replace that history.

Each console-exported row is a per-entity MONTHLY total (Amazon's console
does not break this export out daily), not a daily row - confirmed by
checking for duplicate (campaign, ad group, target/search-term) keys within
a single file (none found at meaningful scale). So this writes ONE row per
entity per month, with `date` set to the 1st of that month as a clear
"this is a monthly aggregate, not a specific day" marker.

Usage:
    python3 import_manual_ads_report.py --file <path> --type search_term|campaign|targeting|advertised_product \
        --month M --year Y [--replace] [--dry-run]

--replace deletes any existing rows for (collection, country_code=US,
month, year) before inserting - use only when the console total is known to
be the complete, authoritative replacement for whatever partial data (if
any) already exists for that month.
"""
import argparse
import csv
import os
import sys

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"]
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]

PROFILE_ID = "1649312585287580"  # USA seller ads profile, confirmed elsewhere in this project
COUNTRY_CODE = "US"

# These two campaign IDs showed up inside every "USA" console export (all 4
# report types, all months) despite being genuinely non-US campaigns - "MX
# PAREO" (currency MXN, campaign_id 82317817918693) and "Golden Naggets"
# (currency CAD, campaign_id 281420756110570). The console export apparently
# isn't marketplace-filtered the way the filename implies. Flagged by the
# user 2026-08-29 after reviewing the imported totals - excluded here so any
# future manual import of this account's "USA" reports doesn't reintroduce
# them.
EXCLUDED_CAMPAIGN_IDS = {"82317817918693", "281420756110570"}

COLLECTIONS = {
    "search_term": "ads_search_term_stats",
    "campaign": "ads_campaign_stats",
    "targeting": "ads_keyword_stats",
    "advertised_product": "ads_advertised_product_stats",
}

AD_PRODUCT_MAP = {
    "Sponsored Products": "SPONSORED_PRODUCTS",
    "Sponsored Brands": "SPONSORED_BRANDS",
    "Sponsored Display": "SPONSORED_DISPLAY",
}


def strip_id(v):
    v = (v or "").strip()
    if v.startswith('="') and v.endswith('"'):
        return v[2:-1]
    return v


def to_float(v):
    v = (v or "").strip()
    return float(v) if v else 0.0


def to_int(v):
    v = (v or "").strip()
    return int(float(v)) if v else 0


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["token"]


def pb_batch(token, batch_requests):
    if not batch_requests:
        return
    response = requests.post(
        f"{POCKETBASE_URL}/api/batch",
        headers={"Authorization": token},
        json={"requests": batch_requests},
        timeout=120,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase batch request failed: HTTP {response.status_code} - {response.text}")
    for entry, result in zip(batch_requests, response.json()):
        status = result.get("status")
        if status is None or status >= 400:
            raise RuntimeError(f"PocketBase batch item failed ({entry['method']} {entry['url']}): {result.get('body')}")


def pb_delete_month(token, collection, month, year):
    deleted = 0
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params={"filter": f'country_code = "{COUNTRY_CODE}" && month = {month} && year = {year}',
                    "fields": "id", "perPage": 200},
            timeout=30,
        )
        response.raise_for_status()
        items = response.json().get("items", [])
        if not items:
            break
        # PocketBase's /api/batch caps at 50 requests per call - chunk deletes
        # the same way inserts already are.
        for i in range(0, len(items), 50):
            chunk = items[i:i + 50]
            batch = [{"method": "DELETE", "url": f"/api/collections/{collection}/records/{it['id']}"} for it in chunk]
            pb_batch(token, batch)
            deleted += len(chunk)
    return deleted


def build_campaign_ad_product_map(token, campaign_csv_files):
    """Prefer the union of all Campaign-report CSVs (has an explicit 'Ad
    product' column and covers the exact historical campaigns in play for
    these months) over the live ads_campaigns snapshot, which only reflects
    currently ENABLED/PAUSED campaigns and can miss older ones."""
    mapping = {}
    for path in campaign_csv_files:
        with open(path, newline="", encoding="utf-8-sig", errors="replace") as fh:
            for row in csv.DictReader(fh):
                cid = strip_id(row.get("Campaign ID"))
                ad_product = AD_PRODUCT_MAP.get((row.get("Ad product") or "").strip(), row.get("Ad product") or "")
                if cid:
                    mapping[cid] = ad_product

    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/ads_campaigns/records",
            headers={"Authorization": token},
            params={"filter": f'profile_id = "{PROFILE_ID}"', "fields": "campaign_id,ad_product", "perPage": 200, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            mapping.setdefault(item["campaign_id"], item.get("ad_product", ""))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return mapping


def parse_search_term(row, ad_product_map):
    campaign_id = strip_id(row.get("Campaign ID"))
    return {
        "profile_id": PROFILE_ID,
        "campaign_id": campaign_id,
        "campaign_name": row.get("Campaign name", ""),
        "ad_group_id": strip_id(row.get("Ad group ID")),
        "ad_group_name": row.get("Ad group name", ""),
        "target_id": "",
        "target_text": "",
        "match_type": "",
        "search_term": row.get("Search term") or "(no search term - product/audience targeted)",
        "country_code": COUNTRY_CODE,
        "currency_code": row.get("Budget currency", "USD"),
        "ad_product": ad_product_map.get(campaign_id, ""),
        "impressions": to_int(row.get("Impressions")),
        "clicks": to_int(row.get("Clicks")),
        "spend": to_float(row.get("Total cost")),
        "sales": to_float(row.get("Sales")),
        "orders": to_int(row.get("Purchases")),
    }


def parse_campaign(row, ad_product_map):
    return {
        "profile_id": PROFILE_ID,
        "campaign_id": strip_id(row.get("Campaign ID")),
        "campaign_name": row.get("Campaign name", ""),
        "campaign_status": "",
        "country_code": COUNTRY_CODE,
        "currency_code": row.get("Budget currency", "USD"),
        "ad_product": AD_PRODUCT_MAP.get((row.get("Ad product") or "").strip(), row.get("Ad product") or ""),
        "impressions": to_int(row.get("Impressions")),
        "clicks": to_int(row.get("Clicks")),
        "spend": to_float(row.get("Total cost")),
        "sales": to_float(row.get("Sales")),
        "orders": to_int(row.get("Purchases")),
    }


def parse_targeting(row, ad_product_map):
    campaign_id = strip_id(row.get("Campaign ID"))
    return {
        "profile_id": PROFILE_ID,
        "campaign_id": campaign_id,
        "campaign_name": row.get("Campaign name", ""),
        "campaign_status": "",
        "ad_group_id": strip_id(row.get("Ad group ID")),
        "ad_group_name": row.get("Ad group name", ""),
        "target_id": strip_id(row.get("Target ID")) or f"unknown-{campaign_id}-{row.get('Ad group ID', '')}",
        "target_text": row.get("Targeting", ""),
        "target_type": row.get("Target type", ""),
        "match_type": row.get("Targeting match type", ""),
        "country_code": COUNTRY_CODE,
        "currency_code": row.get("Budget currency", "USD"),
        "ad_product": ad_product_map.get(campaign_id, ""),
        "impressions": to_int(row.get("Impressions")),
        "clicks": to_int(row.get("Clicks")),
        "spend": to_float(row.get("Total cost")),
        "sales": to_float(row.get("Sales")),
        "orders": to_int(row.get("Purchases")),
        "bid": to_float(row.get("Target bid")),
    }


def parse_advertised_product(row, ad_product_map):
    campaign_id = strip_id(row.get("Campaign ID"))
    return {
        "profile_id": PROFILE_ID,
        "campaign_id": campaign_id,
        "campaign_name": row.get("Campaign name", ""),
        "ad_group_id": strip_id(row.get("Ad group ID")),
        "ad_group_name": row.get("Ad group name", ""),
        "asin": row.get("Advertised product ID", ""),
        "sku": row.get("Advertised product SKU", ""),
        "country_code": COUNTRY_CODE,
        "currency_code": row.get("Budget currency", "USD"),
        "ad_product": ad_product_map.get(campaign_id, ""),
        "impressions": to_int(row.get("Impressions")),
        "clicks": to_int(row.get("Clicks")),
        "spend": to_float(row.get("Total cost")),
        "sales": to_float(row.get("Sales")),
        "orders": to_int(row.get("Purchases")),
    }


PARSERS = {
    "search_term": parse_search_term,
    "campaign": parse_campaign,
    "targeting": parse_targeting,
    "advertised_product": parse_advertised_product,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--type", required=True, choices=list(COLLECTIONS))
    ap.add_argument("--month", required=True, type=int)
    ap.add_argument("--year", required=True, type=int)
    ap.add_argument("--replace", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--campaign-csv", action="append", default=[],
                     help="Campaign-report CSV(s) to build the campaign_id->ad_product map from (repeatable)")
    args = ap.parse_args()

    collection = COLLECTIONS[args.type]
    date_str = f"{args.year:04d}-{args.month:02d}-01"

    token = pb_authenticate()
    ad_product_map = build_campaign_ad_product_map(token, args.campaign_csv)
    parser = PARSERS[args.type]

    with open(args.file, newline="", encoding="utf-8-sig", errors="replace") as fh:
        rows = [parser(row, ad_product_map) for row in csv.DictReader(fh)]
    rows = [r for r in rows if r.get("campaign_id") not in EXCLUDED_CAMPAIGN_IDS]

    for r in rows:
        r["date"] = date_str
        r["month"] = args.month
        r["year"] = args.year

    total_spend = sum(r["spend"] for r in rows)
    total_sales = sum(r["sales"] for r in rows)
    print(f"{args.file}: {len(rows)} rows parsed | spend={total_spend:.2f} sales={total_sales:.2f}")

    if args.dry_run:
        print("(dry run - not writing)")
        return

    if args.replace:
        deleted = pb_delete_month(token, collection, args.month, args.year)
        print(f"  deleted {deleted} existing rows for {collection} {args.year}-{args.month:02d}")

    batch = []
    written = 0
    for r in rows:
        batch.append({"method": "POST", "url": f"/api/collections/{collection}/records", "body": r})
        if len(batch) >= 50:
            pb_batch(token, batch)
            written += len(batch)
            batch = []
    if batch:
        pb_batch(token, batch)
        written += len(batch)

    print(f"  wrote {written} rows to {collection}")


if __name__ == "__main__":
    sys.exit(main())
