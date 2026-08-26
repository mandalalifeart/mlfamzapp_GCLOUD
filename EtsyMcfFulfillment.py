import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

import requests

from EtsyAuth import (
    POCKETBASE_URL,
    cors_headers,
    json_response,
    pb_authenticate,
    pb_get_connection,
    pb_save_connection,
    refresh_access_token,
)
from EtsyOrders import fetch_receipts

POCKETBASE_MAPPING_COLLECTION = os.environ.get("POCKETBASE_MAPPING_COLLECTION", "asin_group_mapping")
POCKETBASE_ORDERS_COLLECTION = os.environ.get("POCKETBASE_ETSY_ORDERS_COLLECTION", "etsy_orders")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
REPORT_EMAIL_TO = os.environ.get("REPORT_EMAIL_TO", "")
TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")

# Wide enough that an order stuck in "Pending" for a while still shows up -
# Etsy receipts don't expose a "still unshipped" filter server-side, so this
# pulls everything in the window and filters client-side.
PENDING_WINDOW_DAYS = 90


def load_sku_asin_map(pb_token):
    """asin_group_mapping.sku is the same internal SKU string used on Etsy
    listings (confirmed empirically 2026-08-26 - Etsy SKUs like "Stool259"
    resolve directly), so no extra translation step is needed: a hit here
    both confirms the item is a real Amazon catalog product and gives the
    SellerSKU/ASIN needed to eventually place an MCF order for it."""
    mapping = {}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_MAPPING_COLLECTION}/records",
            headers={"Authorization": pb_token},
            params={"perPage": 200, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            sku = item.get("sku")
            if sku:
                mapping[sku] = {"asin": item.get("asin", ""), "group": item.get("group", "")}
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return mapping


def load_mcf_status_map(pb_token):
    """receipt_id -> mcf_status ("in_progress" once an MCF order has been
    created for it, by hand or via CreateMcfOrderForReceipt) - used to drop
    already-handled orders out of the report entirely rather than
    re-reporting them as pending every day."""
    status = {}
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ORDERS_COLLECTION}/records",
            headers={"Authorization": pb_token},
            params={"perPage": 200, "page": page, "fields": "receipt_id,mcf_status"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            if item.get("mcf_status"):
                status[item["receipt_id"]] = item["mcf_status"]
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return status


def set_order_mcf_status(pb_token, receipt_id, mcf_status):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ORDERS_COLLECTION}/records",
        headers={"Authorization": pb_token},
        params={"filter": f'receipt_id = "{receipt_id}"', "fields": "id", "perPage": 1},
        timeout=30,
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    if not items:
        raise RuntimeError(f"No stored etsy_orders row for receipt {receipt_id} to update mcf_status on")
    patch_resp = requests.patch(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ORDERS_COLLECTION}/records/{items[0]['id']}",
        headers={"Authorization": pb_token},
        json={"mcf_status": mcf_status},
        timeout=15,
    )
    patch_resp.raise_for_status()


def build_fulfillment_plan(receipts, sku_map, mcf_status_map):
    """One entry per pending order (receipt), each carrying its own line
    items (sku/quantity) and a single fulfillable/remark verdict for the
    whole order - fulfillable only if every line item on it resolved to a
    known ASIN."""
    orders = []
    for receipt in receipts:
        if receipt.get("is_shipped"):
            continue
        # Etsy's own receipt "status" field (e.g. "Paid", "Canceled") -
        # a cancelled order isn't pending, so it's dropped entirely rather
        # than showing up as "not fulfillable".
        if receipt.get("status") == "Canceled":
            continue
        # Already has a real MCF order created for it (by hand or by
        # CreateMcfOrderForReceipt) - already being handled, so drop it
        # rather than reporting it as pending again every day.
        if mcf_status_map.get(str(receipt.get("receipt_id"))) == "in_progress":
            continue
        transactions = receipt.get("transactions", []) or []
        line_items = []
        reasons = []
        for txn in transactions:
            sku = txn.get("sku")
            match = sku_map.get(sku) if sku else None
            item = {
                "sku": sku or "",
                "quantity": txn.get("quantity", 0) or 0,
                "title": txn.get("title", ""),
            }
            if match and match.get("asin"):
                item["asin"] = match["asin"]
            else:
                reason = "no SKU on this order line" if not sku else f"SKU '{sku}' not found in asin_group_mapping"
                item["reason"] = reason
                reasons.append(reason)
            line_items.append(item)

        if not transactions:
            reasons.append("order has no line items")
        fulfillable = not reasons

        orders.append({
            "receiptId": receipt.get("receipt_id"),
            "buyer": receipt.get("name", ""),
            "lineItems": line_items,
            "fulfillable": fulfillable,
            "remark": "Fulfillable" if fulfillable else "Not fulfillable: " + "; ".join(reasons),
        })
    return orders


def format_report_text(orders):
    fulfillable_count = sum(1 for o in orders if o["fulfillable"])
    lines = [
        f"Etsy -> Amazon MCF fulfillment report - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "*** DRY RUN - matching/reporting only, no MCF orders are being placed yet ***",
        "",
        f"{len(orders)} pending order(s), {fulfillable_count} fulfillable, {len(orders) - fulfillable_count} not.",
        "",
    ]
    if not orders:
        lines.append("(no pending orders)")
        return "\n".join(lines)

    for o in orders:
        lines.append(f"Order {o['receiptId']} — {o['buyer']}")
        for li in o["lineItems"]:
            sku_part = li["sku"] or "(no sku)"
            asin_part = f" -> ASIN {li['asin']}" if li.get("asin") else ""
            lines.append(f"  {li['quantity']}x SKU {sku_part}{asin_part} — {li['title']}")
        lines.append(f"  Remark: {o['remark']}")
        lines.append("")

    return "\n".join(lines)


def send_email_report(text):
    if not GMAIL_USER or not GMAIL_APP_PASSWORD or not REPORT_EMAIL_TO:
        return
    msg = MIMEText(text)
    msg["Subject"] = f"Etsy MCF fulfillment report (DRY RUN) - {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
    msg["From"] = GMAIL_USER
    msg["To"] = REPORT_EMAIL_TO
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, [REPORT_EMAIL_TO], msg.as_string())


def send_telegram_report(orders, report_text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    # Telegram messages cap around 4096 chars - send the same order-by-order
    # text as the email, but truncate with a pointer to email for the rest.
    if len(report_text) > 3900:
        report_text = report_text[:3900] + "\n...(truncated, see email for full report)"
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": report_text},
        timeout=15,
    )


def RunEtsyMcfFulfillment(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    try:
        pb_token = pb_authenticate()
        connection = pb_get_connection(pb_token)
        if not connection or connection.get("status") != "connected":
            return json_response({"error": "Etsy is not connected"}, 400)

        shop_id = connection["shop_id"]
        access_token, new_refresh_token = refresh_access_token(connection["refresh_token"])
        if new_refresh_token != connection.get("refresh_token"):
            pb_save_connection(pb_token, {"refresh_token": new_refresh_token})

        now = datetime.now(timezone.utc)
        min_created = int((now - timedelta(days=PENDING_WINDOW_DAYS)).timestamp())
        max_created = int(now.timestamp())
        errors = []
        receipts = fetch_receipts(shop_id, access_token, min_created, max_created, errors)

        sku_map = load_sku_asin_map(pb_token)
        mcf_status_map = load_mcf_status_map(pb_token)
        orders = build_fulfillment_plan(receipts, sku_map, mcf_status_map)

        report_text = format_report_text(orders)
        send_email_report(report_text)
        send_telegram_report(orders, report_text)

        return json_response({
            "dryRun": True,
            "pendingOrders": len(orders),
            "fulfillableOrders": sum(1 for o in orders if o["fulfillable"]),
            "notFulfillableOrders": sum(1 for o in orders if not o["fulfillable"]),
            "errors": errors,
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def find_receipt(shop_id, access_token, receipt_id):
    """Fetches one specific receipt fresh from Etsy by scanning a wide
    window - Etsy's receipts endpoint has no get-by-id-alone route that
    also returns transactions, so this reuses fetch_receipts and filters."""
    now = datetime.now(timezone.utc)
    min_created = int((now - timedelta(days=PENDING_WINDOW_DAYS)).timestamp())
    max_created = int(now.timestamp())
    errors = []
    receipts = fetch_receipts(shop_id, access_token, min_created, max_created, errors)
    for r in receipts:
        if str(r.get("receipt_id")) == str(receipt_id):
            return r, errors
    return None, errors


def create_mcf_order(receipt, sku_map):
    """Builds and submits one real Amazon MCF fulfillment order for a single
    Etsy receipt. Raises on any unmapped SKU rather than partially fulfilling
    an order. This is the one function in this file with a real, hard-to-
    reverse side effect - callers must gate it deliberately."""
    from sp_api.api import FulfillmentOutbound
    from sp_api.base import Marketplaces

    transactions = receipt.get("transactions", []) or []
    items = []
    for idx, txn in enumerate(transactions):
        sku = txn.get("sku")
        match = sku_map.get(sku) if sku else None
        if not match or not match.get("asin"):
            raise RuntimeError(f"Cannot create MCF order: SKU '{sku}' on receipt {receipt.get('receipt_id')} "
                                f"is not in asin_group_mapping")
        items.append({
            "sellerSku": sku,
            "sellerFulfillmentOrderItemId": f"{receipt.get('receipt_id')}-{idx}",
            "quantity": txn.get("quantity", 0) or 0,
        })
    if not items:
        raise RuntimeError(f"Receipt {receipt.get('receipt_id')} has no line items")

    address = {
        "name": receipt.get("name", ""),
        "addressLine1": receipt.get("first_line", ""),
        "city": receipt.get("city", ""),
        "stateOrRegion": receipt.get("state", ""),
        "postalCode": receipt.get("zip", ""),
        "countryCode": receipt.get("country_iso", ""),
    }
    if receipt.get("second_line"):
        address["addressLine2"] = receipt["second_line"]

    credentials = {
        "refresh_token": os.environ["REFRESH_TOKEN_USA"],
        "lwa_app_id": os.environ["CLIENT_ID_USA"],
        "lwa_client_secret": os.environ["CLIENT_SECRET_USA"],
    }
    client = FulfillmentOutbound(credentials=credentials, marketplace=Marketplaces.US)
    created = datetime.fromtimestamp(
        receipt.get("create_timestamp") or receipt.get("created_timestamp") or 0, tz=timezone.utc
    )
    resp = client.create_fulfillment_order(
        sellerFulfillmentOrderId=f"ETSY-{receipt.get('receipt_id')}",
        displayableOrderId=str(receipt.get("receipt_id")),
        displayableOrderDate=created.isoformat(),
        displayableOrderComment=f"Etsy order {receipt.get('receipt_id')}",
        shippingSpeedCategory="Standard",
        destinationAddress=address,
        items=items,
    )
    return resp.payload


def CreateMcfOrderForReceipt(request):
    """Places one REAL Amazon MCF fulfillment order for one Etsy receipt.
    Deliberately requires both ADMIN_KEY and an explicit confirm=yes flag,
    on top of a specific receipt_id - there is no bulk/automatic path to
    this function, it is only ever meant to be triggered by hand for a
    single, reviewed order."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    receipt_id = request.args.get("receipt_id") if hasattr(request, "args") else None
    confirm = request.args.get("confirm") if hasattr(request, "args") else None
    if not receipt_id:
        return json_response({"error": "receipt_id is required"}, 400)
    if confirm != "yes":
        return json_response({"error": "Pass confirm=yes to actually place this real order"}, 400)

    try:
        pb_token = pb_authenticate()
        connection = pb_get_connection(pb_token)
        if not connection or connection.get("status") != "connected":
            return json_response({"error": "Etsy is not connected"}, 400)

        shop_id = connection["shop_id"]
        access_token, new_refresh_token = refresh_access_token(connection["refresh_token"])
        if new_refresh_token != connection.get("refresh_token"):
            pb_save_connection(pb_token, {"refresh_token": new_refresh_token})

        receipt, fetch_errors = find_receipt(shop_id, access_token, receipt_id)
        if not receipt:
            return json_response({"error": f"Receipt {receipt_id} not found in the last {PENDING_WINDOW_DAYS} days",
                                   "errors": fetch_errors}, 404)

        sku_map = load_sku_asin_map(pb_token)
        payload = create_mcf_order(receipt, sku_map)
        set_order_mcf_status(pb_token, receipt_id, "in_progress")
        return json_response({"created": True, "receiptId": receipt_id, "amazonResponse": payload})
    except Exception as exc:
        return json_response({"created": False, "error": str(exc), "type": exc.__class__.__name__}, 500)


def CheckMcfAccess(request):
    """Read-only, no-op probe: confirms whether this SP-API app's USA
    credentials actually have the Fulfillment Outbound (MCF) role granted in
    Seller Central, before any real order-placement code gets built on top
    of it. Calls list_all_fulfillment_orders (returns real order history,
    creates/changes nothing) rather than create_fulfillment_order."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    try:
        from sp_api.api import FulfillmentOutbound
        from sp_api.base import Marketplaces

        credentials = {
            "refresh_token": os.environ["REFRESH_TOKEN_USA"],
            "lwa_app_id": os.environ["CLIENT_ID_USA"],
            "lwa_client_secret": os.environ["CLIENT_SECRET_USA"],
        }
        client = FulfillmentOutbound(credentials=credentials, marketplace=Marketplaces.US)
        resp = client.list_all_fulfillment_orders(queryStartDate="2026-01-01T00:00:00Z")
        orders = (resp.payload or {}).get("FulfillmentOrders", [])
        return json_response({
            "mcfAccessGranted": True,
            "fulfillmentOrdersFound": len(orders),
        })
    except Exception as exc:
        return json_response({
            "mcfAccessGranted": False,
            "error": str(exc),
            "type": exc.__class__.__name__,
        }, 200)
