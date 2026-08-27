import os
import time
from datetime import datetime, timedelta, timezone

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

POCKETBASE_SKU_COLLECTION = os.environ.get("POCKETBASE_SKU_COLLECTION", "sku_sales")
POCKETBASE_COUNTRY_COLLECTION = os.environ.get("POCKETBASE_COUNTRY_COLLECTION", "country_sales")
POCKETBASE_ORDERS_COLLECTION = os.environ.get("POCKETBASE_ETSY_ORDERS_COLLECTION", "etsy_orders")
POCKETBASE_BATCH_SIZE = int(os.environ.get("POCKETBASE_BATCH_SIZE", "50"))

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


def txn_sku_label(txn):
    """"variation:sku" (e.g. "Blue:PAREO_ACA_6B") using the transaction's own
    variations list, or the bare sku when it carries no variation. Empty when
    the transaction has no SKU at all (~36% don't, per DiagnoseEtsyOrders)."""
    sku = txn.get("sku")
    if not sku:
        return ""
    variations = txn.get("variations") or []
    variation_name = " / ".join(v.get("formatted_value", "") for v in variations if v.get("formatted_value"))
    return f"{variation_name}:{sku}" if variation_name else sku


def receipt_to_order_body(shop_id, receipt, marketplace):
    transactions = receipt.get("transactions", []) or []
    total = receipt.get("grandtotal") or {}
    amount = total.get("amount", 0)
    divisor = total.get("divisor", 1) or 1

    created = receipt.get("create_timestamp") or receipt.get("created_timestamp") or 0
    dt = datetime.fromtimestamp(created, tz=timezone.utc) if created else None

    item_count = sum(txn.get("quantity", 0) or 0 for txn in transactions)
    # Readable one-line stand-in for the full transaction list, kept for any
    # older consumer of this field - the orders table itself now renders one
    # row per transaction from line_items instead of parsing this string.
    items_summary = ", ".join(
        f"{txn.get('quantity', 0)}x {txn.get('title', '')}" for txn in transactions
    )
    # One entry per transaction (line item) so the frontend can give each
    # SKU its own table row instead of cramming them into items_summary.
    line_items = [
        {
            "title": txn.get("title", ""),
            "quantity": txn.get("quantity", 0) or 0,
            "sku": txn_sku_label(txn),
            "listingId": str(txn.get("listing_id", "")),
        }
        for txn in transactions
    ]

    return {
        "shop_id": str(shop_id),
        "receipt_id": str(receipt.get("receipt_id")),
        "created": created,
        "month": dt.month if dt else 0,
        "year": dt.year if dt else 0,
        "marketplace": marketplace or "other",
        "country_iso": receipt.get("country_iso", ""),
        "buyer_name": receipt.get("name", ""),
        "total_amount": amount / divisor,
        "currency": total.get("currency_code", ""),
        "item_count": item_count,
        "items_summary": items_summary,
        "line_items": line_items,
        "is_shipped": bool(receipt.get("is_shipped")),
        "status": (
            "Cancelled" if receipt.get("status") == "Canceled"
            else "Shipped" if receipt.get("is_shipped")
            else "Pending"
        ),
        "etsy_status": receipt.get("status", ""),
    }


def pb_list_orders(token, receipt_ids):
    """Existing etsy_orders rows (id + receipt_id + mcf_status) for the given
    receipt_ids - mcf_status is set by hand or by CreateMcfOrderForReceipt
    and must survive UpdateEtsyOrders' delete-then-reinsert upsert, since
    Etsy's own receipt data has no idea an MCF order was ever created."""
    rows = []
    for i in range(0, len(receipt_ids), 50):
        chunk = receipt_ids[i:i + 50]
        filter_str = " || ".join(f'receipt_id = "{rid}"' for rid in chunk)
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ORDERS_COLLECTION}/records",
                headers={"Authorization": token},
                params={"filter": filter_str, "fields": "id,receipt_id,mcf_status", "perPage": 200, "page": page},
                timeout=30,
            )
            if response.status_code != 200:
                raise RuntimeError(f"PocketBase list failed for orders: HTTP {response.status_code} - {response.text}")
            data = response.json()
            rows.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1
    return rows


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


def pb_list_ids(token, collection, filter_str):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params={"filter": filter_str, "fields": "id", "perPage": 200, "page": page},
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase list failed for {collection}: HTTP {response.status_code} - {response.text}")
        data = response.json()
        ids.extend(item["id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


def UpdateEtsyOrders(request):
    """Writes Etsy order data into the shared country_sales/sku_sales
    collections under three marketplaces (etsy_usa/etsy_eu/etsy_uk), same
    shape Amazon orders already use. Every transaction counts toward
    country_sales regardless of SKU; sku_sales only gets transactions that
    have a real SKU set (per the 36%-missing-SKU finding from
    DiagnoseEtsyOrders - skipped rather than guessed at).

    Default window is a rolling 35 days (for the daily scheduled run - wide
    enough to catch late-arriving/updated orders without reprocessing the
    whole year every day); pass min_created/max_created explicitly for a
    wider one-off backfill, as was done for the initial 365-day pull."""
    now = datetime.now(timezone.utc)
    default_min = int((now - timedelta(days=35)).timestamp())
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

        country_totals = {}  # (marketplace, month, year) -> {quantity, sales}
        sku_totals = {}      # (sku, marketplace, month, year) -> quantity
        order_bodies = []    # one row per receipt, for the orders table on /etsy
        skipped_no_marketplace = 0
        skipped_no_sku = 0

        for receipt in receipts:
            country_iso = receipt.get("country_iso", "")
            marketplace = map_marketplace(country_iso)
            order_bodies.append(receipt_to_order_body(shop_id, receipt, marketplace))
            if not marketplace:
                skipped_no_marketplace += len(receipt.get("transactions", []) or [])
                continue

            created = receipt.get("create_timestamp") or receipt.get("created_timestamp")
            dt = datetime.fromtimestamp(created, tz=timezone.utc) if created else now
            month, year = dt.month, dt.year

            for txn in receipt.get("transactions", []) or []:
                quantity = txn.get("quantity", 0) or 0
                price = txn.get("price") or {}
                amount = price.get("amount", 0)
                divisor = price.get("divisor", 1) or 1
                line_total = (amount / divisor) * quantity

                country_key = (marketplace, month, year)
                country_bucket = country_totals.setdefault(country_key, {"quantity": 0, "sales": 0.0})
                country_bucket["quantity"] += quantity
                country_bucket["sales"] += line_total

                sku = txn.get("sku")
                if sku:
                    sku_key = (sku, marketplace, month, year)
                    sku_totals[sku_key] = sku_totals.get(sku_key, 0) + quantity
                else:
                    skipped_no_sku += 1

        ops = []

        # country_sales: delete-then-reinsert each (marketplace, month, year)
        # this pull actually touched, so a re-run over the same range is safe
        # to repeat without duplicating rows.
        for (marketplace, month, year), totals in country_totals.items():
            existing_ids = pb_list_ids(
                pb_token, POCKETBASE_COUNTRY_COLLECTION,
                f'(marketplace = "{marketplace}" && month = {month} && year = {year})',
            )
            ops.extend(
                {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_COUNTRY_COLLECTION}/records/{rid}"}
                for rid in existing_ids
            )
            ops.append({
                "method": "POST",
                "url": f"/api/collections/{POCKETBASE_COUNTRY_COLLECTION}/records",
                "body": {
                    "marketplace": marketplace,
                    "month": month,
                    "year": year,
                    "quantity": totals["quantity"],
                    "sales": round(totals["sales"], 2),
                },
            })

        for (sku, marketplace, month, year), quantity in sku_totals.items():
            existing_ids = pb_list_ids(
                pb_token, POCKETBASE_SKU_COLLECTION,
                f'(sku = "{sku}" && marketplace = "{marketplace}" && month = {month} && year = {year})',
            )
            ops.extend(
                {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_SKU_COLLECTION}/records/{rid}"}
                for rid in existing_ids
            )
            ops.append({
                "method": "POST",
                "url": f"/api/collections/{POCKETBASE_SKU_COLLECTION}/records",
                "body": {
                    "sku": sku,
                    "ASIN": "",
                    "marketplace": marketplace,
                    "month": month,
                    "year": year,
                    "quantity": quantity,
                },
            })

        # etsy_orders: upsert by receipt_id (delete any existing row for a
        # receipt this pull touched, then reinsert) so a re-run over the same
        # range updates rows whose status changed (e.g. is_shipped flipping)
        # without duplicating them. mcf_status is carried forward from the
        # existing row since Etsy's own data has no concept of it - it's set
        # by hand or by CreateMcfOrderForReceipt, and must survive this
        # delete+reinsert instead of getting silently wiped on the next pull.
        if order_bodies:
            existing_orders = pb_list_orders(pb_token, [b["receipt_id"] for b in order_bodies])
            existing_mcf_status = {row["receipt_id"]: row.get("mcf_status", "") for row in existing_orders}
            for body in order_bodies:
                body["mcf_status"] = existing_mcf_status.get(body["receipt_id"], "")
            ops.extend(
                {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ORDERS_COLLECTION}/records/{row['id']}"}
                for row in existing_orders
            )
            ops.extend(
                {"method": "POST", "url": f"/api/collections/{POCKETBASE_ORDERS_COLLECTION}/records", "body": b}
                for b in order_bodies
            )

        for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
            pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])

        return json_response({
            "minCreated": min_created,
            "maxCreated": max_created,
            "totalReceipts": len(receipts),
            "countryRowsWritten": len(country_totals),
            "skuRowsWritten": len(sku_totals),
            "ordersWritten": len(order_bodies),
            "transactionsSkippedNoMarketplace": skipped_no_marketplace,
            "transactionsSkippedNoSku": skipped_no_sku,
            "errors": errors,
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetEtsyOrders(request):
    """Reads stored order rows from etsy_orders for display on /etsy - no
    Amazon-style ADMIN_KEY gate needed, this is read-only aggregation of
    already-pulled data, same as GetEtsyListings."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    min_created = request.args.get("min_created", type=int) if hasattr(request, "args") else None
    max_created = request.args.get("max_created", type=int) if hasattr(request, "args") else None
    marketplace = request.args.get("marketplace") if hasattr(request, "args") else None
    search = request.args.get("search") if hasattr(request, "args") else None
    limit = request.args.get("limit", type=int) if hasattr(request, "args") else None
    limit = limit or 200

    try:
        token = pb_authenticate()
        filters = []
        if min_created:
            filters.append(f"created >= {min_created}")
        if max_created:
            filters.append(f"created <= {max_created}")
        if marketplace:
            filters.append(f'marketplace = "{marketplace}"')
        if search:
            escaped = search.replace('"', '\\"')
            filters.append(f'(buyer_name ~ "{escaped}" || items_summary ~ "{escaped}")')
        filter_str = " && ".join(filters)

        orders = []
        page = 1
        per_page = min(limit, 200)
        while len(orders) < limit:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ORDERS_COLLECTION}/records",
                headers={"Authorization": token},
                params={k: v for k, v in {
                    "filter": filter_str,
                    "perPage": per_page,
                    "page": page,
                    "sort": "-created",
                }.items() if v},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                orders.append({
                    "receiptId": item.get("receipt_id"),
                    "created": item.get("created", 0),
                    "marketplace": item.get("marketplace", ""),
                    "countryIso": item.get("country_iso", ""),
                    "buyerName": item.get("buyer_name", ""),
                    "totalAmount": item.get("total_amount", 0),
                    "currency": item.get("currency", ""),
                    "itemCount": item.get("item_count", 0),
                    "itemsSummary": item.get("items_summary", ""),
                    "lineItems": item.get("line_items") or [],
                    "status": item.get("status", ""),
                    "etsyStatus": item.get("etsy_status", ""),
                    "mcfStatus": item.get("mcf_status", ""),
                })
            if len(orders) >= limit or page >= data.get("totalPages", 1):
                break
            page += 1

        return json_response({"orders": orders[:limit]})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
