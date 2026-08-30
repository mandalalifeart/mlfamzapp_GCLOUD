import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

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


def MarkEtsyOrderInProgress(request):
    """Lets the /etsy page mark an order's mcf_status "in_progress" by hand
    - for orders fulfilled outside this system (e.g. created directly in
    Amazon Seller Central) rather than via CreateMcfOrderForReceipt. No
    ADMIN_KEY gate, same reasoning as UpdateEtsyListings/UpdateEtsyOrders:
    triggered directly by a frontend button that can't hold a secret, and
    this only flips our own tracking field - no real Amazon/Etsy write."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    body = request.get_json(silent=True) or {}
    receipt_id = body.get("receipt_id") or (request.args.get("receipt_id") if hasattr(request, "args") else None)
    if not receipt_id:
        return json_response({"error": "receipt_id is required"}, 400)

    try:
        pb_token = pb_authenticate()
        set_order_mcf_status(pb_token, receipt_id, "in_progress")
        return json_response({"updated": True, "receiptId": receipt_id, "mcfStatus": "in_progress"})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


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

        not_fulfillable = sum(1 for o in orders if not o["fulfillable"])
        report_text = format_report_text(orders)
        from NotificationRouting import notify
        notify(
            "etsy-daily-mcf-fulfillment", "amzbot", report_text,
            is_error=bool(not_fulfillable or errors),
            subject=f"Etsy MCF fulfillment report (DRY RUN) - {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        )

        return json_response({
            "dryRun": True,
            "pendingOrders": len(orders),
            "fulfillableOrders": sum(1 for o in orders if o["fulfillable"]),
            "notFulfillableOrders": not_fulfillable,
            "errors": errors,
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def RunEtsyMcfFulfillmentWet(request):
    """WET RUN - places a real Amazon MCF fulfillment order for every
    currently-pending, fulfillable Etsy receipt. Built 2026-08-29 at the
    user's explicit, repeated request ("make this a wet run, really create
    mcf orders, and run it twice a day") to turn the dry-run report
    (RunEtsyMcfFulfillment, kept as-is/unchanged) into the real thing.
    Deliberately a separate function/endpoint from the dry-run report rather
    than a mode flag on it - real order placement stays its own clearly-
    labeled path, same as CreateMcfOrderForReceipt's single-receipt version.
    One order's failure doesn't block the rest; each outcome is reported."""
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
        fetch_errors = []
        receipts = fetch_receipts(shop_id, access_token, min_created, max_created, fetch_errors)
        receipts_by_id = {str(r.get("receipt_id")): r for r in receipts}

        sku_map = load_sku_asin_map(pb_token)
        mcf_status_map = load_mcf_status_map(pb_token)
        plan = build_fulfillment_plan(receipts, sku_map, mcf_status_map)

        created = []
        failed = []
        for order in plan:
            if not order["fulfillable"]:
                continue
            receipt = receipts_by_id.get(str(order["receiptId"]))
            if not receipt:
                failed.append({"receiptId": order["receiptId"], "error": "receipt vanished between plan and creation"})
                continue
            try:
                payload = create_mcf_order(receipt, sku_map)
            except Exception as exc:
                failed.append({"receiptId": order["receiptId"], "buyer": order["buyer"], "error": str(exc)})
                continue
            # The real Amazon order is placed at this point - any failure past
            # here must NOT land in `failed` (build_fulfillment_plan only
            # skips a receipt on the next run if mcf_status is already
            # "in_progress", so reporting this as failed would leave it
            # eligible to be re-submitted and double-fulfilled for real).
            entry = {"receiptId": order["receiptId"], "buyer": order["buyer"], "amazonOrderId": payload.get("fulfillmentOrderId", "")}
            try:
                set_order_mcf_status(pb_token, order["receiptId"], "in_progress")
            except Exception as exc:
                entry["statusUpdateFailed"] = str(exc)
            created.append(entry)

        not_fulfillable = sum(1 for o in plan if not o["fulfillable"])

        lines = [
            f"Etsy -> Amazon MCF fulfillment (WET RUN) - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            f"{len(plan)} pending order(s): {len(created)} MCF order(s) created, {len(failed)} failed, {not_fulfillable} not fulfillable (unmapped SKU).",
            "",
        ]
        for c in created:
            lines.append(f"  CREATED: Order {c['receiptId']} — {c['buyer']}")
        for f in failed:
            lines.append(f"  FAILED: Order {f['receiptId']} — {f.get('buyer', '')} — {f['error']}")
        report_text = "\n".join(lines)

        from NotificationRouting import notify
        notify(
            "etsy-daily-mcf-fulfillment-wet", "amzbot", report_text,
            is_error=bool(failed or fetch_errors),
            subject=f"Etsy MCF fulfillment (WET RUN) - {len(created)} created, {len(failed)} failed",
        )

        return json_response({
            "wetRun": True,
            "pendingOrders": len(plan),
            "created": created,
            "failed": failed,
            "notFulfillableOrders": not_fulfillable,
            "fetchErrors": fetch_errors,
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


def fulfillment_outbound_client():
    from sp_api.api import FulfillmentOutbound
    from sp_api.base import Marketplaces

    credentials = {
        "refresh_token": os.environ["REFRESH_TOKEN_USA"],
        "lwa_app_id": os.environ["CLIENT_ID_USA"],
        "lwa_client_secret": os.environ["CLIENT_SECRET_USA"],
    }
    return FulfillmentOutbound(credentials=credentials, marketplace=Marketplaces.US)


def create_mcf_order(receipt, sku_map):
    """Builds and submits one real Amazon MCF fulfillment order for a single
    Etsy receipt. Raises on any unmapped SKU rather than partially fulfilling
    an order. This is the one function in this file with a real, hard-to-
    reverse side effect - callers must gate it deliberately."""
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

    client = fulfillment_outbound_client()
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
        # Explicit rather than relying on Amazon's default: "NotRequired"
        # means the constraint is NOT enforced, i.e. Amazon Logistics stays
        # allowed as a carrier - matches "Block Amazon Logistics" unchecked
        # in Seller Central's manual create-order UI.
        # Real valid values (confirmed live via Amazon's own rejection
        # message): BLANK_BOX, OVERBOX, BLOCK_AMZL, PRIME_ELIGIBILITY,
        # DELIVER_TOGETHER, SIGNATURE_CONFIRMATION,
        # ADULT_SIGNATURE_CONFIRMATION, PACKING_SLIP - "BLOCK_AMAZON_LOGISTICS"
        # (used originally) isn't one of them and broke every order creation
        # until caught here.
        featureConstraints=[{"featureName": "BLOCK_AMZL", "featureFulfillmentPolicy": "NotRequired"}],
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
        # The real Amazon order is placed at this point - a failure updating
        # our own mcf_status must not be reported as "created: False", or a
        # real order could look like it never happened and get re-submitted.
        try:
            set_order_mcf_status(pb_token, receipt_id, "in_progress")
        except Exception as status_exc:
            return json_response({"created": True, "receiptId": receipt_id, "amazonResponse": payload,
                                   "statusUpdateFailed": str(status_exc)})
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
        client = fulfillment_outbound_client()
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


def load_in_progress_orders(pb_token):
    """receipt_id list for every etsy_orders row still mcf_status="in_progress"
    - these are the ones a real Amazon MCF order was created for (by hand or
    via CreateMcfOrderForReceipt) but that haven't had a tracking number
    pushed back to Etsy yet."""
    receipt_ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_ORDERS_COLLECTION}/records",
            headers={"Authorization": pb_token},
            params={"filter": 'mcf_status = "in_progress"', "fields": "receipt_id", "perPage": 200, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        receipt_ids.extend(item["receipt_id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return receipt_ids


def fetch_etsy_carrier_names(access_token, origin_country_iso="US"):
    """Etsy's createReceiptShipment carrier_name must match one of the names
    this endpoint returns (confirmed via Etsy's own OpenAPI spec) - there is
    no fixed enum to hardcode against, so the valid set is fetched live."""
    response = requests.get(
        f"{ETSY_API_BASE}/shipping-carriers",
        headers={"x-api-key": api_key_header(), "Authorization": f"Bearer {access_token}"},
        params={"origin_country_iso": origin_country_iso},
        timeout=30,
    )
    response.raise_for_status()
    return [c["name"] for c in response.json().get("results", []) if c.get("name")]


# Amazon's package carrierCode values (e.g. "FEDEX", "AMZN_US") don't always
# spell the carrier the same way Etsy's shipping-carriers list does (e.g.
# "FedEx") - these are the mappings confirmed to not line up with a plain
# case-insensitive equality check. "AMZN_US" (Amazon Logistics) deliberately
# has no entry: Etsy has no equivalent carrier, so it's left to fall through
# to "no confident match" rather than mapped to something wrong.
AMAZON_TO_ETSY_CARRIER_HINTS = {
    "FEDEX": "fedex",
    "FEDEX_SMARTPOST": "fedex",
    "UPS_MI": "ups",
    "DHL_GLOBAL_MAIL": "dhl",
    "DHL_ECOMMERCE": "dhl",
}


def map_carrier_name(amazon_carrier_code, etsy_carrier_names):
    """Maps one Amazon package carrierCode to one of Etsy's valid carrier
    names, or None if there's no confident match - callers must skip pushing
    tracking for a package rather than guess, since a wrong carrier_name on
    Etsy sends the buyer a shipment notification pointing at the wrong
    tracking system."""
    if not amazon_carrier_code:
        return None
    code_lower = amazon_carrier_code.strip().lower()
    for name in etsy_carrier_names:
        if name.lower() == code_lower:
            return name
    hint = AMAZON_TO_ETSY_CARRIER_HINTS.get(amazon_carrier_code.strip().upper(), code_lower)
    for name in etsy_carrier_names:
        if hint in name.lower():
            return name
    return None


def extract_shipped_packages(payload):
    """All packages that already have a real tracking number, from ANY
    fulfillment shipment - not gated on fulfillmentShipmentStatus=="SHIPPED".
    Amazon assigns a real carrier tracking number as soon as a shipping
    label is generated, which can happen while the shipment is still
    "PENDING" (confirmed live, 2026-08-28, per the user's own correction -
    a real USPS tracking number was already present on a PENDING shipment).
    Gating on SHIPPED specifically was needlessly conservative and delayed
    pushing real, usable tracking to Etsy/the buyer."""
    packages = []
    for shipment in payload.get("fulfillmentShipments") or []:
        for pkg in shipment.get("fulfillmentShipmentPackage") or []:
            tracking = pkg.get("trackingNumber") or pkg.get("amazonFulfillmentTrackingNumber")
            if tracking:
                packages.append({"trackingNumber": tracking, "carrierCode": pkg.get("carrierCode", "")})
    return packages


def push_tracking_to_etsy(access_token, shop_id, receipt_id, tracking_code, carrier_name):
    response = requests.post(
        f"{ETSY_API_BASE}/shops/{shop_id}/receipts/{receipt_id}/tracking",
        headers={"x-api-key": api_key_header(), "Authorization": f"Bearer {access_token}"},
        json={"tracking_code": tracking_code, "carrier_name": carrier_name, "send_bcc": False},
        timeout=30,
    )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Etsy tracking update failed: HTTP {response.status_code} - {response.text}")
    return response.json()


def format_tracking_report(shipped, still_processing, skipped_no_carrier_match, cancelled, errors):
    lines = [
        f"Etsy tracking sync (from Amazon MCF) - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"{len(shipped)} order(s) marked shipped on Etsy with tracking, "
        f"{still_processing} still processing on Amazon, "
        f"{len(skipped_no_carrier_match)} shipped but skipped (no carrier match), "
        f"{len(cancelled)} cancelled on Amazon, "
        f"{len(errors)} error(s).",
        "",
    ]
    for entry in shipped:
        lines.append(f"  Receipt {entry['receiptId']}: {entry['carrier']} {entry['tracking']}")
    if cancelled:
        lines.append("")
        lines.append("Cancelled on Amazon (mcf_status set to 'cancelled', needs manual review/refund on Etsy):")
        for entry in cancelled:
            lines.append(f"  Receipt {entry['receiptId']}: {entry['status']}")
    if skipped_no_carrier_match:
        lines.append("")
        lines.append("Skipped (Amazon carrier code didn't match any Etsy carrier - needs a manual update):")
        for entry in skipped_no_carrier_match:
            lines.append(f"  Receipt {entry['receiptId']}: Amazon carrierCode '{entry['carrierCode']}', tracking {entry['tracking']}")
    if errors:
        lines.append("")
        lines.append("Errors:")
        for err in errors:
            lines.append(f"  {err}")
    return "\n".join(lines)


def send_tracking_email(text):
    if not GMAIL_USER or not GMAIL_APP_PASSWORD or not REPORT_EMAIL_TO:
        return
    msg = MIMEText(text)
    msg["Subject"] = f"Etsy tracking sync - {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
    msg["From"] = GMAIL_USER
    msg["To"] = REPORT_EMAIL_TO
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, [REPORT_EMAIL_TO], msg.as_string())


def send_tracking_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if len(text) > 3900:
        text = text[:3900] + "\n...(truncated, see email for full report)"
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=15,
    )


def UpdateEtsyTrackingFromAmazon(request):
    """Daily job: for every Etsy order with mcf_status="in_progress" (a real
    Amazon MCF order was placed for it), checks whether that Amazon
    fulfillment order has shipped, and if so pushes the real tracking
    number(s) back to Etsy via createReceiptShipment - this is what actually
    marks the Etsy order shipped and notifies the buyer. Requires the Etsy
    connection to carry the transactions_w scope (added 2026-08-26); an
    old read-only-only connection needs reconnecting via /etsy first."""
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

        receipt_ids = load_in_progress_orders(pb_token)
        if not receipt_ids:
            return json_response({"inProgress": 0, "shipped": 0, "stillProcessing": 0, "errors": []})

        etsy_carrier_names = fetch_etsy_carrier_names(access_token)
        client = fulfillment_outbound_client()

        shipped = []
        skipped_no_carrier_match = []
        cancelled = []
        still_processing = 0
        errors = []

        for receipt_id in receipt_ids:
            try:
                resp = client.get_fulfillment_order(sellerFulfillmentOrderId=f"ETSY-{receipt_id}")
                payload = resp.payload or {}
                # A Cancelled/Invalid MCF order will never produce a shipment -
                # counting it as "still processing" forever (the original
                # behavior here) hides a real cancellation indefinitely
                # instead of surfacing it. Found live 2026-08-30 via receipt
                # 4158189487, whose Amazon fulfillment order had already been
                # Cancelled but mcf_status stayed stuck at "in_progress".
                order_status = (payload.get("fulfillmentOrder") or {}).get("fulfillmentOrderStatus")
                if order_status in ("Cancelled", "Invalid"):
                    set_order_mcf_status(pb_token, receipt_id, "cancelled")
                    cancelled.append({"receiptId": receipt_id, "status": order_status})
                    continue

                packages = extract_shipped_packages(payload)
                if not packages:
                    still_processing += 1
                    continue

                pushed_any = False
                for pkg in packages:
                    carrier_name = map_carrier_name(pkg["carrierCode"], etsy_carrier_names)
                    if not carrier_name:
                        skipped_no_carrier_match.append({
                            "receiptId": receipt_id,
                            "carrierCode": pkg["carrierCode"],
                            "tracking": pkg["trackingNumber"],
                        })
                        continue
                    push_tracking_to_etsy(access_token, shop_id, receipt_id, pkg["trackingNumber"], carrier_name)
                    shipped.append({"receiptId": receipt_id, "carrier": carrier_name, "tracking": pkg["trackingNumber"]})
                    pushed_any = True

                if pushed_any:
                    set_order_mcf_status(pb_token, receipt_id, "shipped")
            except Exception as exc:
                errors.append(f"Receipt {receipt_id}: {exc}")

        report_text = format_tracking_report(shipped, still_processing, skipped_no_carrier_match, cancelled, errors)
        from NotificationRouting import notify
        notify(
            "etsy-daily-tracking-update", "amzbot", report_text,
            is_error=bool(skipped_no_carrier_match or cancelled or errors),
            subject=f"Etsy tracking sync - {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        )

        return json_response({
            "inProgress": len(receipt_ids),
            "shipped": len(shipped),
            "stillProcessing": still_processing,
            "skippedNoCarrierMatch": skipped_no_carrier_match,
            "cancelled": cancelled,
            "errors": errors,
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetMcfFulfillmentPreview(request):
    """Read-only: SP-API's getFulfillmentPreview returns the REAL shipping
    cost Amazon would charge to MCF-fulfill a given SKU/quantity/destination
    - no order is created, nothing is committed. Used to get real numbers
    for a launch-viability question (e.g. "would a $27 item with $5 cost
    work via MCF on Etsy") instead of guessing at fee estimates."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    sku = request.args.get("sku") if hasattr(request, "args") else None
    if not sku:
        return json_response({"error": "sku is required"}, 400)

    try:
        client = fulfillment_outbound_client()
        resp = client.get_fulfillment_preview(
            address={
                "name": "Test Buyer",
                "addressLine1": "410 Terry Ave N",
                "city": "Seattle",
                "stateOrRegion": "WA",
                "postalCode": "98109",
                "countryCode": "US",
            },
            items=[{"sellerSku": sku, "sellerFulfillmentOrderItemId": "preview-1", "quantity": 1}],
            shippingSpeedCategories=["Standard"],
            includeCODFulfillmentPreview=False,
            includeDeliveryWindows=False,
        )
        return json_response({"sku": sku, "preview": resp.payload})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def GetMcfOrderById(request):
    """Read-only: looks up one specific Amazon MCF fulfillment order by its
    exact sellerFulfillmentOrderId - used to check whether a manually
    created Seller Central order actually exists under the id the user
    thinks they used, without waiting for the daily tracking-sync job."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    order_id = request.args.get("order_id") if hasattr(request, "args") else None
    if not order_id:
        return json_response({"error": "order_id is required"}, 400)

    try:
        client = fulfillment_outbound_client()
        resp = client.get_fulfillment_order(sellerFulfillmentOrderId=order_id)
        return json_response({"found": True, "orderId": order_id, "data": resp.payload})
    except Exception as exc:
        return json_response({"found": False, "orderId": order_id, "error": str(exc), "type": exc.__class__.__name__}, 200)
