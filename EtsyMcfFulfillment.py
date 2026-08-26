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
    matched = []
    unmatched = []
    for receipt in receipts:
        if receipt.get("is_shipped"):
            continue
        buyer = receipt.get("name", "")
        receipt_id = receipt.get("receipt_id")
        for txn in receipt.get("transactions", []) or []:
            sku = txn.get("sku")
            entry = {
                "receiptId": receipt_id,
                "buyer": buyer,
                "title": txn.get("title", ""),
                "sku": sku or "",
                "quantity": txn.get("quantity", 0) or 0,
            }
            match = sku_map.get(sku) if sku else None
            if match and match.get("asin"):
                entry["asin"] = match["asin"]
                entry["group"] = match.get("group", "")
                matched.append(entry)
            else:
                entry["reason"] = "no SKU on this order line" if not sku else "SKU not found in asin_group_mapping"
                unmatched.append(entry)
    return matched, unmatched


def format_report_text(matched, unmatched):
    lines = [
        f"Etsy -> Amazon MCF fulfillment report - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "*** DRY RUN - matching/reporting only, no MCF orders are being placed yet ***",
        "",
        f"Matched ({len(matched)} line item(s) - would be sent to MCF once live):",
    ]
    if matched:
        for m in matched:
            lines.append(f"  - Receipt {m['receiptId']} / {m['buyer']}: {m['quantity']}x {m['title']} "
                          f"[SKU {m['sku']} -> ASIN {m['asin']}, group {m['group']}]")
    else:
        lines.append("  (none)")

    lines.append("")
    lines.append(f"Unmatched / skipped ({len(unmatched)} line item(s)):")
    if unmatched:
        for u in unmatched:
            lines.append(f"  - Receipt {u['receiptId']} / {u['buyer']}: {u['quantity']}x {u['title']} "
                          f"[SKU {u['sku'] or '(none)'}] - {u['reason']}")
    else:
        lines.append("  (none)")

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


def send_telegram_report(matched, unmatched):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    lines = [
        f"Etsy -> MCF fulfillment (DRY RUN) - {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        f"Matched: {len(matched)} | Unmatched: {len(unmatched)}",
    ]
    for u in unmatched[:15]:
        lines.append(f"- {u['sku'] or '(no sku)'} — {u['title'][:40]} — {u['reason']}")
    if len(unmatched) > 15:
        lines.append(f"...and {len(unmatched) - 15} more unmatched, see email for full report")
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": "\n".join(lines)},
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
        matched, unmatched = build_fulfillment_plan(receipts, sku_map)

        report_text = format_report_text(matched, unmatched)
        send_email_report(report_text)
        send_telegram_report(matched, unmatched)

        return json_response({
            "dryRun": True,
            "pendingOrdersScanned": sum(1 for r in receipts if not r.get("is_shipped")),
            "matchedLineItems": len(matched),
            "unmatchedLineItems": len(unmatched),
            "errors": errors,
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
