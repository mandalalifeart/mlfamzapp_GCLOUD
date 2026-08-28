import os
from datetime import datetime, timezone

import requests

from EtsyAuth import (
    POCKETBASE_URL,
    cors_headers,
    json_response,
    pb_authenticate,
)
from EtsyListings import load_order_counts_by_listing

POCKETBASE_LISTINGS_COLLECTION = os.environ.get("POCKETBASE_ETSY_LISTINGS_COLLECTION", "etsy_listings")
POCKETBASE_EXPERIMENTS_COLLECTION = os.environ.get("POCKETBASE_ETSY_EXPERIMENTS_COLLECTION", "etsy_listing_experiments")
POCKETBASE_SNAPSHOTS_COLLECTION = os.environ.get("POCKETBASE_ETSY_SNAPSHOTS_COLLECTION", "etsy_listing_snapshots")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")

# Weekly summaries rather than daily noise - a listing's views/orders don't
# move meaningfully day to day, and this is a slow-moving experiment.
SUMMARY_INTERVAL_DAYS = 7


def get_current_stats(pb_token, listing_id):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_LISTINGS_COLLECTION}/records",
        headers={"Authorization": pb_token},
        params={"filter": f'listing_id = "{listing_id}"', "perPage": 1},
        timeout=15,
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    if not items:
        return None
    item = items[0]
    order_counts = load_order_counts_by_listing(pb_token)
    bucket = order_counts.get(str(listing_id), {"orders": 0, "quantity": 0})
    return {
        "views": item.get("views", 0),
        "numFavorers": item.get("num_favorers", 0),
        "ordersCount": bucket["orders"],
        "orderedQuantity": bucket["quantity"],
    }


def StartEtsyExperiment(request):
    """Marks one listing as under active growth-experiment monitoring, with
    today's stats recorded as the baseline to compare future snapshots
    against. Call this right before (or right after) making the live change
    being tested, so the baseline reflects pre-change performance."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    body = request.get_json(silent=True) or {}
    listing_id = body.get("listing_id")
    change_description = body.get("change_description", "")
    if not listing_id:
        return json_response({"error": "listing_id is required"}, 400)

    try:
        pb_token = pb_authenticate()
        stats = get_current_stats(pb_token, listing_id)
        if stats is None:
            return json_response({"error": f"Listing {listing_id} not found in etsy_listings - pull listings first"}, 404)

        now = datetime.now(timezone.utc)
        record = {
            "listing_id": str(listing_id),
            "change_description": change_description,
            "started_at": int(now.timestamp()),
            "status": "active",
            "baseline_views": stats["views"],
            "baseline_favorites": stats["numFavorers"],
            "baseline_orders": stats["ordersCount"],
            "last_summary_at": int(now.timestamp()),
        }
        response = requests.post(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_EXPERIMENTS_COLLECTION}/records",
            headers={"Authorization": pb_token},
            json=record,
            timeout=15,
        )
        if response.status_code not in (200, 201):
            return json_response({"error": f"HTTP {response.status_code} - {response.text}"}, 502)

        return json_response({"started": True, "listingId": listing_id, "baseline": stats})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def format_summary(experiment, current, days_running):
    delta_views = current["views"] - experiment["baseline_views"]
    delta_favs = current["numFavorers"] - experiment["baseline_favorites"]
    delta_orders = current["ordersCount"] - experiment["baseline_orders"]
    return (
        f"Etsy growth experiment update - listing {experiment['listing_id']} - day {days_running}\n"
        f"Change: {experiment.get('change_description', '(not recorded)')}\n\n"
        f"Views: {experiment['baseline_views']} -> {current['views']} ({delta_views:+d})\n"
        f"Favorites: {experiment['baseline_favorites']} -> {current['numFavorers']} ({delta_favs:+d})\n"
        f"Orders: {experiment['baseline_orders']} -> {current['ordersCount']} ({delta_orders:+d})\n"
    )


def send_summary(text):
    # Telegram only - this is a routine status update (views/orders
    # before-after), never an alert, so it doesn't go to email.
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=15,
        )


def MonitorEtsyExperiments(request):
    """Daily job: snapshots every active experiment's current stats, and
    every SUMMARY_INTERVAL_DAYS sends a before/after comparison against the
    recorded baseline. Snapshots every day (cheap, useful history) but only
    notifies weekly so this doesn't turn into daily noise."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    try:
        pb_token = pb_authenticate()
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_EXPERIMENTS_COLLECTION}/records",
            headers={"Authorization": pb_token},
            params={"filter": 'status = "active"', "perPage": 200},
            timeout=15,
        )
        response.raise_for_status()
        experiments = response.json().get("items", [])

        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        summarized = []
        errors = []

        for exp in experiments:
            try:
                current = get_current_stats(pb_token, exp["listing_id"])
                if current is None:
                    errors.append(f"Listing {exp['listing_id']}: not found in etsy_listings")
                    continue

                requests.post(
                    f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SNAPSHOTS_COLLECTION}/records",
                    headers={"Authorization": pb_token},
                    json={
                        "listing_id": exp["listing_id"],
                        "date": today,
                        "views": current["views"],
                        "num_favorers": current["numFavorers"],
                        "orders_count": current["ordersCount"],
                        "ordered_quantity": current["orderedQuantity"],
                    },
                    timeout=15,
                )

                started_at = exp.get("started_at", 0)
                days_running = int((now.timestamp() - started_at) / 86400)
                last_summary_at = exp.get("last_summary_at", started_at)
                days_since_summary = int((now.timestamp() - last_summary_at) / 86400)

                if days_since_summary >= SUMMARY_INTERVAL_DAYS:
                    text = format_summary(exp, current, days_running)
                    send_summary(text)
                    requests.patch(
                        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_EXPERIMENTS_COLLECTION}/records/{exp['id']}",
                        headers={"Authorization": pb_token},
                        json={"last_summary_at": int(now.timestamp())},
                        timeout=15,
                    )
                    summarized.append(exp["listing_id"])
            except Exception as exc:
                errors.append(f"Listing {exp.get('listing_id')}: {exc}")

        return json_response({"experimentsChecked": len(experiments), "summarized": summarized, "errors": errors})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
