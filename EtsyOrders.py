import os
import time
from datetime import datetime, timedelta, timezone

import requests

from EtsyAuth import (
    ETSY_API_BASE,
    api_key_header,
    cors_headers,
    json_response,
    pb_authenticate,
    pb_get_connection,
    pb_save_connection,
    refresh_access_token,
)

RECEIPTS_PAGE_LIMIT = 100
# Small pace between pages - receipts pulls are infrequent/on-demand right
# now (diagnostic only, no scheduler yet), so this is just politeness rather
# than a measured throttling response like the Amazon Ads pipeline needed.
PAGE_DELAY_SECONDS = 0.2

# Etsy country_iso values are ISO-3166 alpha-2. "GB" is the UK; the 27 EU
# member states map to a combined etsy_eu bucket per the user's 3-marketplace
# scheme (etsy_usa/etsy_eu/etsy_uk) - anything outside these three is left
# out of the bucket totals (reported separately as "other") rather than
# guessed into one of them.
EU_COUNTRY_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR",
    "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK",
    "SI", "ES", "SE",
}


def map_marketplace(country_iso):
    if country_iso == "US":
        return "etsy_usa"
    if country_iso == "GB":
        return "etsy_uk"
    if country_iso in EU_COUNTRY_CODES:
        return "etsy_eu"
    return None


def fetch_receipts(shop_id, access_token, min_created, max_created, errors):
    headers = {"x-api-key": api_key_header(), "Authorization": f"Bearer {access_token}"}
    receipts = []
    offset = 0
    while True:
        try:
            response = requests.get(
                f"{ETSY_API_BASE}/shops/{shop_id}/receipts",
                headers=headers,
                params={
                    "min_created": min_created,
                    "max_created": max_created,
                    "limit": RECEIPTS_PAGE_LIMIT,
                    "offset": offset,
                    "sort_on": "created",
                    "sort_order": "asc",
                },
                timeout=30,
            )
        except requests.RequestException as exc:
            errors.append(f"receipts fetch (offset {offset}): {exc}")
            break
        if response.status_code != 200:
            errors.append(f"receipts fetch (offset {offset}): HTTP {response.status_code} - {response.text}")
            break
        body = response.json()
        results = body.get("results", [])
        receipts.extend(results)
        offset += len(results)
        if offset >= body.get("count", 0) or not results:
            break
        time.sleep(PAGE_DELAY_SECONDS)
    return receipts


def DiagnoseEtsyOrders(request):
    """Read-only: pulls receipts/transactions for a date range and reports
    marketplace breakdown + how many transactions are missing a SKU, without
    writing anything to PocketBase. A first look before deciding how
    missing-SKU rows should be handled in country_sales/sku_sales."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    now = datetime.now(timezone.utc)
    default_min = int((now - timedelta(days=365)).timestamp())
    default_max = int(now.timestamp())
    min_created = request.args.get("min_created", type=int) if hasattr(request, "args") else None
    max_created = request.args.get("max_created", type=int) if hasattr(request, "args") else None
    min_created = min_created or default_min
    max_created = max_created or default_max

    try:
        pb_token = pb_authenticate()
        connection = pb_get_connection(pb_token)
        if not connection or connection.get("status") != "connected":
            return json_response({"error": "Etsy is not connected"}, 400)

        shop_id = connection["shop_id"]
        access_token, new_refresh_token = refresh_access_token(connection["refresh_token"])
        if new_refresh_token != connection.get("refresh_token"):
            pb_save_connection(pb_token, {"refresh_token": new_refresh_token})

        errors = []
        receipts = fetch_receipts(shop_id, access_token, min_created, max_created, errors)

        marketplace_totals = {}
        other_countries = {}
        total_transactions = 0
        missing_sku_transactions = 0
        missing_sku_samples = []

        for receipt in receipts:
            country_iso = receipt.get("country_iso", "")
            marketplace = map_marketplace(country_iso)
            transactions = receipt.get("transactions", []) or []

            bucket = marketplace_totals.setdefault(marketplace or "other", {
                "marketplace": marketplace or "other",
                "receipts": 0,
                "transactions": 0,
                "quantity": 0,
                "missingSku": 0,
            })
            bucket["receipts"] += 1

            if not marketplace:
                other_countries[country_iso] = other_countries.get(country_iso, 0) + 1

            for txn in transactions:
                total_transactions += 1
                bucket["transactions"] += 1
                bucket["quantity"] += txn.get("quantity", 0) or 0
                sku = txn.get("sku")
                if not sku:
                    missing_sku_transactions += 1
                    bucket["missingSku"] += 1
                    if len(missing_sku_samples) < 20:
                        missing_sku_samples.append({
                            "receiptId": receipt.get("receipt_id"),
                            "listingId": txn.get("listing_id"),
                            "title": txn.get("title"),
                            "quantity": txn.get("quantity"),
                            "countryIso": country_iso,
                        })

        return json_response({
            "minCreated": min_created,
            "maxCreated": max_created,
            "totalReceipts": len(receipts),
            "totalTransactions": total_transactions,
            "missingSkuTransactions": missing_sku_transactions,
            "marketplaceBreakdown": list(marketplace_totals.values()),
            "otherCountries": other_countries,
            "missingSkuSamples": missing_sku_samples,
            "errors": errors,
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
