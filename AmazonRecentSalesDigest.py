"""Hourly "sales in the last hour" Telegram digest, added 2026-09-19 at the
user's request. Live SP-API Orders pull (GET /orders/v0/orders,
CreatedAfter/CreatedBefore) across every real Amazon marketplace - no
PocketBase sales data involved, since sku_sales/country_sales are only
updated daily/monthly and can't answer "in the last hour."

Deliberately Amazon-only, not Etsy: Etsy sales already get their own
real-time push notification the moment they happen (SocialMarketting's
webhook - see CLAUDE.md), so folding Etsy in here would just duplicate that.

**Content is order count + per-SKU units, NOT dollar revenue** - confirmed
live 2026-09-19 against this account's real orders (18 Pending vs 17 Shipped
orders checked): a brand-new order sits in "Pending" status and Amazon does
NOT expose its OrderTotal until it later clears Pending (some orders were
still Pending 6+ hours after creation in this account) - so a revenue figure
for a genuine last-60-minutes window would show ~$0 almost every run, not
because nothing sold, just because Amazon hasn't released the dollar amount
yet. SKU + QuantityOrdered, by contrast, ARE available immediately for a
Pending order (confirmed via a real get_order_items call) - only the payment
total is delayed, not the cart contents - so units-sold is the metric that
can actually be real-time here. Any OrderTotal that IS already present gets
included too, clearly labeled as partial/incomplete, rather than hidden.

Runs on the round hour (cron `0 * * * *`, not "every 60 minutes from
whenever this was first deployed") per the user's explicit request.

Sends to MCF_TELEGRAM_BOT_TOKEN/CHAT_ID (@baba_social_bot) - the user
explicitly asked for "the other channel," i.e. this app's existing shared
alert bot, not the interactive Claude Code Telegram session.
"""
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

from AdsAuth import cors_headers, json_response
from AdsReporting import ADMIN_KEY, pb_authenticate
from JobState import get_job_state, set_job_state

# Paces get_order_items calls (one per order) to stay well under the Orders
# API's per-account rate limit - this account's real order volume is low
# enough (a handful per marketplace per hour, confirmed by testing) that
# this adds negligible run time.
ORDER_ITEMS_PACING_SECONDS = 1.1

TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")

JOB_STATE_KEY = "recent_sales_digest_window_end"
DEFAULT_LOOKBACK_MINUTES = 60
# Amazon rejects a CreatedBefore within ~2 minutes of the real current time
# (HTTP 400 "not valid") - confirmed via this project's own earlier Orders.py
# probe script. The window's end is always pinned this far behind "now."
CREATED_BEFORE_LAG_MINUTES = 2
# Caps a catch-up window after a missed tick (a long WSL-idle outage, etc.)
# so recovery doesn't try to pull many hours of orders in one run - matches
# the same bounded-catch-up shape as AdsReporting's 31-day report cap.
MAX_CATCHUP_HOURS = 6

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


def summarize_marketplace(cred_scope, marketplace_enum_name, created_after, created_before):
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
    # Almost always empty for a genuine last-hour window (see module
    # docstring - Amazon withholds OrderTotal until a Pending order clears),
    # kept anyway so any order that DOES already have one isn't hidden.
    partial_revenue_by_currency = defaultdict(float)
    units_by_sku = defaultdict(int)

    for order in orders:
        if order.get("OrderStatus") == "Canceled":
            continue
        order_count += 1

        total = order.get("OrderTotal") or {}
        currency = total.get("CurrencyCode")
        if currency:
            partial_revenue_by_currency[currency] += float(total.get("Amount") or 0)

        time.sleep(ORDER_ITEMS_PACING_SECONDS)
        try:
            for item in fetch_order_items(client, order["AmazonOrderId"]):
                sku = item.get("SellerSKU")
                qty = int(item.get("QuantityOrdered") or 0)
                if sku:
                    units_by_sku[sku] += qty
        except Exception:
            pass  # one order's items failing shouldn't drop its order count

    return order_count, dict(partial_revenue_by_currency), dict(units_by_sku)


def build_digest_text(window_start, window_end, results, errors):
    if not results:
        body = "No orders in this window."
    else:
        lines = []
        for r in results:
            sku_str = ", ".join(f"{sku} x{qty}" for sku, qty in sorted(r["units"].items(), key=lambda kv: -kv[1]))
            lines.append(f"  {r['marketplace']}: {r['orderCount']} order{'s' if r['orderCount'] != 1 else ''} - {sku_str or '(no items found)'}")
            if r["revenue"]:
                revenue_str = ", ".join(f"{amount:,.2f} {ccy}" for ccy, amount in r["revenue"].items())
                lines.append(f"    (partial revenue already available: {revenue_str} - most new orders' totals aren't released by Amazon yet)")
        body = "\n".join(lines)

    text = (
        f"\U0001f6d2 Amazon sales - last {int((window_end - window_start).total_seconds() / 60)} min\n"
        f"({window_start.strftime('%H:%M')} - {window_end.strftime('%H:%M')} UTC)\n\n"
        f"{body}"
    )
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


def SendRecentSalesDigest(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    try:
        now = datetime.now(timezone.utc)
        window_end = now - timedelta(minutes=CREATED_BEFORE_LAG_MINUTES)

        token = pb_authenticate()
        last_window_end_str = get_job_state(token, JOB_STATE_KEY)
        if last_window_end_str:
            window_start = datetime.strptime(last_window_end_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            window_start = max(window_start, window_end - timedelta(hours=MAX_CATCHUP_HOURS))
        else:
            window_start = window_end - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)

        created_after = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        created_before = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        results = []
        errors = []
        for mp_code, cred_scope, enum_name in MARKETPLACES:
            try:
                count, revenue, units = summarize_marketplace(cred_scope, enum_name, created_after, created_before)
                if count > 0:
                    results.append({"marketplace": mp_code, "orderCount": count, "revenue": revenue, "units": units})
            except Exception as exc:
                errors.append(f"{mp_code}: {exc}")

        text = build_digest_text(window_start, window_end, results, errors)
        send_telegram(text)
        set_job_state(token, JOB_STATE_KEY, created_before)

        return json_response({
            "status": "success",
            "windowStart": created_after,
            "windowEnd": created_before,
            "results": results,
            "errors": errors,
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
