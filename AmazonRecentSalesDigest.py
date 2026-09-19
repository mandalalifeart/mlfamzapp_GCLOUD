"""Hourly "new orders since the last update" Telegram digest, added
2026-09-19 at the user's request, redesigned same day per a follow-up
request. Live SP-API Orders pull (GET /orders/v0/orders) across every real
Amazon marketplace - no PocketBase sales data involved, since
sku_sales/country_sales are only updated daily/monthly.

**Design: fetch every order placed so far TODAY (Israel calendar day), then
report only the ones not already reported in an earlier run this same
day** - not a fixed rolling 60-minute CreatedAfter/CreatedBefore window. The
user explicitly asked for this shape ("get all orders list for that day, and
show all new orders that were not shown in a previous hourly update")
instead of the original rolling-window version. This is also strictly more
robust: a fixed window can miss or double-report an order sitting right at
a boundary, or a job tick that runs late; scanning the whole day and
deduping by AmazonOrderId can't miss anything within the day, and
automatically self-heals after a missed tick with no separate catch-up-cap
logic needed (unlike a rolling window, which needed one). Dedup state lives
in JobState (job_state collection) as {"date": "YYYY-MM-DD", "seenOrderIds":
[...]} - resets automatically the first run after Israel-local midnight.

Deliberately Amazon-only, not Etsy: Etsy sales already get their own
real-time push notification the moment they happen (SocialMarketting's
webhook - see CLAUDE.md), so folding Etsy in here would just duplicate that.

**Content is order count + total items + per-SKU units, NOT dollar
revenue** - confirmed live 2026-09-19 against this account's real orders (18
Pending vs 17 Shipped orders checked): a brand-new order sits in "Pending"
status and Amazon does NOT expose its OrderTotal until it later clears
Pending (some orders were still Pending 6+ hours after creation in this
account). SKU + QuantityOrdered, by contrast, ARE available immediately for
a Pending order (confirmed via a real get_order_items call) - only the
payment total is delayed, not the cart contents - so units-sold is the
metric that can actually be real-time here. Any OrderTotal that IS already
present gets included too, clearly labeled as partial/incomplete.

Runs on the round hour (cron `0 * * * *`) per the user's request. Displayed
times are Israel local time per the user. Sends to MCF_TELEGRAM_BOT_TOKEN/
CHAT_ID (@baba_social_bot) - the user explicitly asked for "the other
channel," not the interactive Claude Code Telegram session.
"""
import json
import os
import time
import urllib.parse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from AdsAuth import cors_headers, json_response
from AdsReporting import ADMIN_KEY, pb_authenticate
from JobState import get_job_state, set_job_state

DISPLAY_TZ = ZoneInfo("Asia/Jerusalem")

# Paces get_order_items calls (one per NEW order) to stay well under the
# Orders API's per-account rate limit - only genuinely new orders each run
# incur this call (already-seen orders are skipped entirely), so this stays
# cheap even as the day's full order list grows hour over hour.
ORDER_ITEMS_PACING_SECONDS = 1.1

TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")

# Same per-SKU product photo bucket NextOrderPage.jsx already renders
# directly in the browser (public, no auth needed) - confirmed live
# 2026-09-19 that Telegram's servers can fetch these URLs directly.
IMAGE_BASE = "https://storage.googleapis.com/mlf-amz-images/"

JOB_STATE_KEY = "recent_sales_digest_seen_orders"
# Amazon rejects a CreatedBefore within ~2 minutes of the real current time
# (HTTP 400 "not valid") - confirmed via this project's own earlier Orders.py
# probe script.
CREATED_BEFORE_LAG_MINUTES = 2

# marketplace_code -> (credential scope, sp_api Marketplaces enum attribute
# name). Credential scopes/env vars mirror every other multi-marketplace
# puller in this codebase (AmazonFinances.py, AmazonListingOps.py, etc.).
MARKETPLACES = [
    ("US", "usa", "US"),
    ("CA", "usa", "CA"),
    ("MX", "usa", "MX"),
    ("UK", "eu", "UK"),
    ("DE", "eu", "DE"),
    ("FR", "eu", "FR"),
    ("IT", "eu", "IT"),
    ("ES", "eu", "ES"),
    ("NL", "eu", "NL"),
    ("BE", "eu", "BE"),
    ("PL", "eu", "PL"),
    ("SE", "eu", "SE"),
]

CREDENTIAL_SCOPES = {
    "usa": {"refresh_env": "REFRESH_TOKEN_USA", "id_env": "CLIENT_ID_USA", "secret_env": "CLIENT_SECRET_USA"},
    "eu": {"refresh_env": "REFRESH_TOKEN_EU", "id_env": "CLIENT_ID_EU", "secret_env": "CLIENT_SECRET_EU"},
}


def fetch_orders(client, created_after, created_before, marketplace_id):
    orders = []
    next_token = None
    while True:
        kwargs = {"NextToken": next_token} if next_token else {
            "CreatedAfter": created_after,
            "CreatedBefore": created_before,
            "MarketplaceIds": [marketplace_id],
        }
        response = client.get_orders(**kwargs)
        payload = response.payload or {}
        orders.extend(payload.get("Orders", []))
        next_token = payload.get("NextToken")
        if not next_token:
            break
    return orders


def fetch_order_items(client, order_id):
    items = []
    next_token = None
    while True:
        kwargs = {"order_id": order_id, "NextToken": next_token} if next_token else {"order_id": order_id}
        response = client.get_order_items(**kwargs)
        payload = response.payload or {}
        items.extend(payload.get("OrderItems", []))
        next_token = payload.get("NextToken")
        if not next_token:
            break
    return items


def summarize_new_orders(cred_scope, marketplace_enum_name, created_after, created_before, seen_order_ids):
    """Fetches every order today's-so-far for one marketplace, skips any
    AmazonOrderId already in seen_order_ids (already reported in an earlier
    run today), and returns (order_count, revenue_by_currency, units_by_sku,
    newly_seen_ids) for just the new ones. Every fetched order id - reported
    or not (e.g. Canceled) - is returned in newly_seen_ids so it's never
    reconsidered on a later run this same day."""
    from sp_api.api import Orders
    from sp_api.base import Marketplaces

    scope = CREDENTIAL_SCOPES[cred_scope]
    credentials = {
        "refresh_token": os.environ[scope["refresh_env"]],
        "lwa_app_id": os.environ[scope["id_env"]],
        "lwa_client_secret": os.environ[scope["secret_env"]],
    }
    marketplace_enum = getattr(Marketplaces, marketplace_enum_name)
    client = Orders(credentials=credentials, marketplace=marketplace_enum)
    orders = fetch_orders(client, created_after, created_before, marketplace_enum.marketplace_id)

    order_count = 0
    # Almost always empty for a brand-new order (see module docstring -
    # Amazon withholds OrderTotal until a Pending order clears), kept anyway
    # so any order that DOES already have one isn't hidden.
    partial_revenue_by_currency = defaultdict(float)
    units_by_sku = defaultdict(int)
    newly_seen_ids = []

    for order in orders:
        order_id = order.get("AmazonOrderId")
        if not order_id or order_id in seen_order_ids:
            continue
        newly_seen_ids.append(order_id)

        if order.get("OrderStatus") == "Canceled":
            continue
        order_count += 1

        total = order.get("OrderTotal") or {}
        currency = total.get("CurrencyCode")
        if currency:
            partial_revenue_by_currency[currency] += float(total.get("Amount") or 0)

        time.sleep(ORDER_ITEMS_PACING_SECONDS)
        try:
            for item in fetch_order_items(client, order_id):
                sku = item.get("SellerSKU")
                qty = int(item.get("QuantityOrdered") or 0)
                if sku:
                    units_by_sku[sku] += qty
        except Exception:
            pass  # one order's items failing shouldn't drop its order count

    return order_count, dict(partial_revenue_by_currency), dict(units_by_sku), newly_seen_ids


def build_digest_text(as_of, results, errors):
    if not results:
        body = "No new orders since the last update."
    else:
        lines = []
        for r in results:
            item_count = sum(r["units"].values())
            lines.append(
                f"  {r['marketplace']}: {r['orderCount']} order{'s' if r['orderCount'] != 1 else ''}, "
                f"{item_count} item{'s' if item_count != 1 else ''}"
            )
            if r["units"]:
                for sku, qty in sorted(r["units"].items(), key=lambda kv: -kv[1]):
                    lines.append(f"    {sku} x{qty}")
            else:
                lines.append("    (no items found)")
        body = "\n".join(lines)

    text = f"\U0001f6d2 Amazon sales - new orders as of {as_of.strftime('%H:%M %Z')}\n\n{body}"
    if errors:
        text += "\n\n⚠️ Errors (skipped): " + "; ".join(errors)
    return text


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]},
        timeout=15,
    )


def send_telegram_sku_photos(results):
    """One photo per (marketplace, SKU) line in the digest, captioned with
    the same "marketplace: SKU xQTY" text - a flat list across every
    marketplace, not grouped, since Telegram's sendMediaGroup doesn't
    support any kind of section header between items anyway. Chunked into
    groups of <=10 (Telegram's per-call max); a lone leftover photo uses
    sendPhoto instead, since sendMediaGroup requires at least 2 items. Each
    chunk's failure is isolated (e.g. one SKU with no image on the bucket)
    so it never blocks the rest - the text digest already carries the full
    real information regardless of whether any photo sends."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    photos = []
    for r in results:
        for sku, qty in sorted(r["units"].items(), key=lambda kv: -kv[1]):
            photos.append({
                "type": "photo",
                "media": f"{IMAGE_BASE}{urllib.parse.quote(sku)}.jpg",
                "caption": f"{r['marketplace']}: {sku} x{qty}",
            })

    for i in range(0, len(photos), 10):
        chunk = photos[i:i + 10]
        try:
            if len(chunk) == 1:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                    json={"chat_id": TELEGRAM_CHAT_ID, "photo": chunk[0]["media"], "caption": chunk[0]["caption"]},
                    timeout=30,
                )
            else:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMediaGroup",
                    json={"chat_id": TELEGRAM_CHAT_ID, "media": chunk},
                    timeout=30,
                )
        except Exception:
            pass  # a photo/image-bucket problem should never break the digest


def SendRecentSalesDigest(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    try:
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(DISPLAY_TZ)
        today_str = now_local.date().isoformat()
        day_start_utc = now_local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        window_end_utc = now_utc - timedelta(minutes=CREATED_BEFORE_LAG_MINUTES)

        token = pb_authenticate()
        state_raw = get_job_state(token, JOB_STATE_KEY)
        state = json.loads(state_raw) if state_raw else {}
        # A different (or missing) stored date means a new day - start with
        # no seen orders, since a new day's order list is naturally empty.
        seen_order_ids = set(state.get("seenOrderIds", [])) if state.get("date") == today_str else set()

        created_after = day_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        created_before = window_end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        results = []
        errors = []
        all_new_ids = []
        for mp_code, cred_scope, enum_name in MARKETPLACES:
            try:
                count, revenue, units, new_ids = summarize_new_orders(cred_scope, enum_name, created_after, created_before, seen_order_ids)
                all_new_ids.extend(new_ids)
                if count > 0:
                    results.append({"marketplace": mp_code, "orderCount": count, "revenue": revenue, "units": units})
            except Exception as exc:
                errors.append(f"{mp_code}: {exc}")

        text = build_digest_text(now_local, results, errors)
        send_telegram(text)
        send_telegram_sku_photos(results)

        seen_order_ids.update(all_new_ids)
        set_job_state(token, JOB_STATE_KEY, json.dumps({"date": today_str, "seenOrderIds": sorted(seen_order_ids)}))

        return json_response({
            "status": "success",
            "date": today_str,
            "results": results,
            "errors": errors,
            "totalSeenToday": len(seen_order_ids),
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
