import os
import time

import requests

from EtsyAuth import (
    ETSY_API_BASE,
    POCKETBASE_URL,
    api_key_header,
    cors_headers,
    json_response,
    pb_authenticate,
    pb_get_connection,
    pb_save_connection,
    refresh_access_token,
)

POCKETBASE_ETSY_LISTINGS_COLLECTION = os.environ.get("POCKETBASE_ETSY_LISTINGS_COLLECTION", "etsy_listings")
POCKETBASE_BATCH_SIZE = int(os.environ.get("POCKETBASE_BATCH_SIZE", "50"))

LISTINGS_PAGE_LIMIT = 100
# Etsy's rate limit is generous (10 req/sec, 10k/day for a standard app), but
# fetching each listing's inventory is an extra request per listing - a small
# pace avoids bursting into the per-second limit on shops with many listings.
INVENTORY_FETCH_DELAY_SECONDS = 0.15


def fetch_active_listings(shop_id, access_token):
    headers = {"x-api-key": api_key_header(), "Authorization": f"Bearer {access_token}"}
    listings = []
    offset = 0
    while True:
        response = requests.get(
            f"{ETSY_API_BASE}/shops/{shop_id}/listings/active",
            headers=headers,
            params={"limit": LISTINGS_PAGE_LIMIT, "offset": offset},
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Listings fetch failed: HTTP {response.status_code} - {response.text}")
        body = response.json()
        results = body.get("results", [])
        listings.extend(results)
        offset += len(results)
        if offset >= body.get("count", 0) or not results:
            break
    return listings


def fetch_listing_skus(listing_id, access_token, errors):
    """Returns one entry per inventory product/variation on the listing, as
    "variation:sku" (e.g. "Blue:CoverPoufRectBlue93"), or the bare sku when
    the product has no variation properties. Each variation gets its own row
    on the listings table rather than being packed into one comma-joined
    cell - a single listing can genuinely carry many distinct SKUs."""
    headers = {"x-api-key": api_key_header(), "Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(
            f"{ETSY_API_BASE}/listings/{listing_id}/inventory",
            headers=headers,
            timeout=15,
        )
        if response.status_code != 200:
            errors.append(f"listing {listing_id} inventory: HTTP {response.status_code}")
            return []
        products = response.json().get("products", []) or []
        seen = set()
        entries = []
        for p in products:
            sku = p.get("sku")
            if not sku or p.get("is_deleted"):
                continue
            variation_name = " / ".join(
                ", ".join(pv.get("values") or []) for pv in (p.get("property_values") or []) if pv.get("values")
            )
            entry = f"{variation_name}:{sku}" if variation_name else sku
            if entry not in seen:
                seen.add(entry)
                entries.append(entry)
        return entries
    except requests.RequestException as exc:
        errors.append(f"listing {listing_id} inventory: {exc}")
        return []


def listing_to_body(shop_id, listing, sku):
    price = listing.get("price") or {}
    amount = price.get("amount", 0)
    divisor = price.get("divisor", 1) or 1
    return {
        "shop_id": str(shop_id),
        "listing_id": str(listing.get("listing_id")),
        "title": listing.get("title", ""),
        "state": listing.get("state", ""),
        "sku": sku,
        "quantity": listing.get("quantity", 0),
        "price_amount": amount / divisor,
        "price_currency": price.get("currency_code", ""),
        "url": listing.get("url", ""),
        "updated_at": str(listing.get("last_modified_timestamp", "")),
        "views": listing.get("views", 0) or 0,
        "num_favorers": listing.get("num_favorers", 0) or 0,
    }


def pb_batch(token, batch_requests):
    if not batch_requests:
        return
    response = requests.post(
        f"{POCKETBASE_URL}/api/batch",
        headers={"Authorization": token},
        json={"requests": batch_requests},
        timeout=60,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase batch request failed: HTTP {response.status_code} - {response.text}")
    for entry, result in zip(batch_requests, response.json()):
        status = result.get("status")
        if status is None or status >= 400:
            raise RuntimeError(
                f"PocketBase batch item failed ({entry['method']} {entry['url']}): {result.get('body')}"
            )


def pb_list_listing_ids(token, shop_id):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ETSY_LISTINGS_COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": f'shop_id = "{shop_id}"', "fields": "id", "perPage": 200, "page": page},
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed: HTTP {response.status_code} - {response.text}")
        data = response.json()
        ids.extend(item["id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


def pull_and_store_listings():
    pb_token = pb_authenticate()
    connection = pb_get_connection(pb_token)
    if not connection or connection.get("status") != "connected":
        raise RuntimeError("Etsy is not connected")

    shop_id = connection["shop_id"]
    access_token, new_refresh_token = refresh_access_token(connection["refresh_token"])
    if new_refresh_token != connection.get("refresh_token"):
        pb_save_connection(pb_token, {"refresh_token": new_refresh_token})

    errors = []
    listings = fetch_active_listings(shop_id, access_token)

    bodies = []
    for listing in listings:
        skus = fetch_listing_skus(listing.get("listing_id"), access_token, errors)
        # One row per listing - multiple variations are newline-joined into
        # the one sku field (frontend renders them stacked via
        # white-space: pre-line) rather than each getting its own row.
        bodies.append(listing_to_body(shop_id, listing, "\n".join(skus)))
        time.sleep(INVENTORY_FETCH_DELAY_SECONDS)

    existing_ids = pb_list_listing_ids(pb_token, shop_id)
    ops = [
        {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ETSY_LISTINGS_COLLECTION}/records/{rid}"}
        for rid in existing_ids
    ]
    ops.extend(
        {"method": "POST", "url": f"/api/collections/{POCKETBASE_ETSY_LISTINGS_COLLECTION}/records", "body": b}
        for b in bodies
    )
    for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
        pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])

    return {"listingsWritten": len(bodies), "errors": errors}


def UpdateEtsyListings(request):
    # Unlike the Amazon Ads report pulls, this has no per-call cost/rate-limit
    # risk worth gating behind ADMIN_KEY - it's also triggered directly by a
    # "Pull Listings Now" button on the frontend, which can't hold a secret.
    try:
        result = pull_and_store_listings()
        return json_response(result)
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetEtsyListings(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    state_filter = request.args.get("state") if hasattr(request, "args") else None
    search = request.args.get("search") if hasattr(request, "args") else None

    try:
        token = pb_authenticate()
        filters = []
        if state_filter:
            filters.append(f'state = "{state_filter}"')
        if search:
            escaped = search.replace('"', '\\"')
            filters.append(f'(title ~ "{escaped}" || sku ~ "{escaped}")')
        filter_str = " && ".join(filters)

        listings = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ETSY_LISTINGS_COLLECTION}/records",
                headers={"Authorization": token},
                params={k: v for k, v in {"filter": filter_str, "perPage": 200, "page": page}.items() if v},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                listings.append({
                    "listingId": item.get("listing_id"),
                    "title": item.get("title", ""),
                    "state": item.get("state", ""),
                    "sku": item.get("sku", ""),
                    "quantity": item.get("quantity", 0),
                    "priceAmount": item.get("price_amount", 0),
                    "priceCurrency": item.get("price_currency", ""),
                    "url": item.get("url", ""),
                    "views": item.get("views", 0),
                    "numFavorers": item.get("num_favorers", 0),
                })
            if page >= data.get("totalPages", 1):
                break
            page += 1

        listings.sort(key=lambda l: l["title"])
        return json_response({"listings": listings})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
