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


def build_fulfillment_plan(receipts, sku_map):
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
        orders = build_fulfillment_plan(receipts, sku_map)

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
