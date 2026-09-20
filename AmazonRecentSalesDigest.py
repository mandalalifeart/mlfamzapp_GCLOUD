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

The per-item visual is a single generated table image (Image/SKU/# rows,
matching a reference screenshot the user provided 2026-09-19) rather than a
Telegram photo album, since chat messages can't render an actual HTML
table. Needs Pillow, installed directly into this project's local .venv
(NOT added to requirements.txt, which only feeds GCP Cloud Function
deploys - this digest is local-only, so bloating every unrelated GCP
function's deploy package with Pillow would be pointless).
"""
import io
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
# Amazon documents/rejects a CreatedBefore within 2 minutes of the real
# current time (HTTP 400 "not valid"). Using exactly 2 minutes here left
# zero margin for the real latency between computing window_end_utc and
# Amazon's own server evaluating it (token refresh + network round-trip) -
# confirmed live 2026-09-20: the FIRST marketplace processed each run (US)
# occasionally errored on exactly this, while later marketplaces (processed
# a few seconds later in the same run, by which point more real time had
# elapsed) didn't. 3 minutes gives real headroom instead of sitting exactly
# on Amazon's stated minimum.
CREATED_BEFORE_LAG_MINUTES = 3

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


TABLE_FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
TABLE_FONT_BOLD_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
# Narrower + a bit shorter per row than the first version - confirmed live
# 2026-09-20 that the original 640px-wide table rendered too wide relative
# to its height (a handful of rows made for a short, very wide image), and
# Telegram's inline chat-feed preview crops an image with that kind of
# extreme aspect ratio instead of showing all of it - the Image column and
# thumbnails were getting cut off entirely without tapping to open. A
# narrower table with the same row count is meaningfully less wide relative
# to its height, so more of it stays visible in the inline preview.
TABLE_THUMB_SIZE = 46
TABLE_ROW_HEIGHT = 60
TABLE_HEADER_HEIGHT = 38
TABLE_WIDTH = 380
TABLE_COL_IMAGE = 60
TABLE_COL_QTY = 46
TABLE_BORDER_COLOR = (204, 204, 204)
TABLE_HEADER_BG = (244, 244, 244)


def fetch_circular_thumbnail(sku):
    """Downloads a SKU's product photo (same public bucket used everywhere
    else in this app) and returns it center-cropped to a square + masked
    into a circle, matching the reference screenshot's look. Returns a
    plain gray circle placeholder (never raises) if the image is missing or
    the download fails, so one bad SKU image never breaks the whole table."""
    from PIL import Image, ImageDraw

    size = TABLE_THUMB_SIZE
    try:
        url = f"{IMAGE_BASE}{urllib.parse.quote(sku)}.jpg"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        img = Image.open(io.BytesIO(resp.content)).convert("RGB")
        side = min(img.size)
        left = (img.width - side) // 2
        top = (img.height - side) // 2
        img = img.crop((left, top, left + side, top + side)).resize((size, size), Image.LANCZOS)
    except Exception:
        img = Image.new("RGB", (size, size), (220, 220, 220))

    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
    circular = Image.new("RGBA", (size, size))
    circular.paste(img, (0, 0), mask)
    return circular


def render_sku_table_image(line_items):
    """One PNG, styled after the user's reference screenshot: a plain table
    with Image / SKU / # columns, one row per (marketplace, SKU) line item -
    the marketplace is folded into the SKU cell (small gray tag under the
    SKU name) rather than a 4th column, to keep the exact 3-column look
    while not losing which marketplace each line came from. Returns the
    local file path of the saved PNG."""
    import tempfile
    from PIL import Image, ImageDraw, ImageFont

    font = ImageFont.truetype(TABLE_FONT_PATH, 15)
    font_small = ImageFont.truetype(TABLE_FONT_PATH, 11)
    font_bold = ImageFont.truetype(TABLE_FONT_BOLD_PATH, 15)

    height = TABLE_HEADER_HEIGHT + TABLE_ROW_HEIGHT * len(line_items) + 1
    img = Image.new("RGB", (TABLE_WIDTH, height), "white")
    draw = ImageDraw.Draw(img)

    col_sku_x0 = TABLE_COL_IMAGE
    col_qty_x0 = TABLE_WIDTH - TABLE_COL_QTY

    draw.rectangle((0, 0, TABLE_WIDTH, TABLE_HEADER_HEIGHT), fill=TABLE_HEADER_BG)
    draw.text((TABLE_COL_IMAGE / 2, TABLE_HEADER_HEIGHT / 2), "Image", font=font_bold, fill="black", anchor="mm")
    draw.text(((col_sku_x0 + col_qty_x0) / 2, TABLE_HEADER_HEIGHT / 2), "SKU", font=font_bold, fill="black", anchor="mm")
    draw.text((col_qty_x0 + TABLE_COL_QTY / 2, TABLE_HEADER_HEIGHT / 2), "#", font=font_bold, fill="black", anchor="mm")

    y = TABLE_HEADER_HEIGHT
    for marketplace, sku, qty in line_items:
        thumb = fetch_circular_thumbnail(sku)
        thumb_x = (TABLE_COL_IMAGE - TABLE_THUMB_SIZE) // 2
        thumb_y = y + (TABLE_ROW_HEIGHT - TABLE_THUMB_SIZE) // 2
        img.paste(thumb, (thumb_x, thumb_y), thumb)

        sku_cx = (col_sku_x0 + col_qty_x0) / 2
        draw.text((sku_cx, y + TABLE_ROW_HEIGHT / 2 - 9), sku, font=font, fill="black", anchor="mm")
        draw.text((sku_cx, y + TABLE_ROW_HEIGHT / 2 + 12), marketplace, font=font_small, fill=(120, 120, 120), anchor="mm")

        draw.text((col_qty_x0 + TABLE_COL_QTY / 2, y + TABLE_ROW_HEIGHT / 2), str(qty), font=font_bold, fill="black", anchor="mm")

        y += TABLE_ROW_HEIGHT
        draw.line((0, y, TABLE_WIDTH, y), fill=TABLE_BORDER_COLOR)

    draw.rectangle((0, 0, TABLE_WIDTH - 1, height - 1), outline=TABLE_BORDER_COLOR)
    draw.line((TABLE_COL_IMAGE, 0, TABLE_COL_IMAGE, height), fill=TABLE_BORDER_COLOR)
    draw.line((col_qty_x0, 0, col_qty_x0, height), fill=TABLE_BORDER_COLOR)

    fd, path = tempfile.mkstemp(suffix=".png", prefix="amzbot_sales_table_")
    os.close(fd)
    img.save(path, "PNG")
    return path


def send_telegram_sku_table_image(results):
    """Single generated table image (Image/SKU/# rows) instead of a photo
    album - per the user's explicit reference screenshot (2026-09-19), since
    Telegram chat messages can't render an actual HTML table."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    line_items = [
        (r["marketplace"], sku, qty)
        for r in results
        for sku, qty in sorted(r["units"].items(), key=lambda kv: -kv[1])
    ]
    if not line_items:
        return

    path = None
    try:
        path = render_sku_table_image(line_items)
        with open(path, "rb") as f:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": TELEGRAM_CHAT_ID},
                files={"photo": ("sales.png", f, "image/png")},
                timeout=30,
            )
    except Exception:
        pass  # an image-generation/send problem should never break the digest
    finally:
        if path and os.path.exists(path):
            os.remove(path)


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
        # Right at local midnight, day_start_utc is essentially "now," so
        # subtracting the lag below would land BEFORE day_start_utc and
        # Amazon rejects CreatedAfter > CreatedBefore outright (hit live
        # 2026-09-20: every marketplace errored on the first run of a new
        # day). Clamping keeps the range valid (possibly zero-width, which
        # Amazon accepts) instead of erroring for the first minute or two
        # of every single day.
        window_end_utc = max(day_start_utc, now_utc - timedelta(minutes=CREATED_BEFORE_LAG_MINUTES))

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
        send_telegram_sku_table_image(results)

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
