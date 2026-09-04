import os

import requests

from EtsyAuth import (
    ETSY_API_BASE,
    api_key_header,
    cors_headers,
    json_response,
    pb_authenticate,
    pb_get_connection,
    pb_save_connection,
    refresh_access_token,
)

ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

# Real values copied from an existing, live, comparable listing (1820250173,
# "Velvet Floor Pouf Ottoman: Embroidered Boho Accent Pillow") rather than
# guessed - same "a real live sibling listing already answers what Amazon/
# Etsy accepts for this exact kind of product" technique already used for
# Amazon standalone-relists (see CLAUDE.md). Only used as defaults - every
# field is overridable via the request body.
POUF_COVER_TEMPLATE_DEFAULTS = {
    "who_made": "i_did",
    "when_made": "2020_2026",
    "is_supply": False,
    "taxonomy_id": 996,
    "shop_section_id": 51735093,
    "shipping_profile_id": 249133465980,
    "return_policy_id": 1074420118744,
    "materials": ["cotton"],
    "processing_min": 1,
    "processing_max": 2,
    "item_weight": 32,
    "item_weight_unit": "oz",
    "item_length": 10,
    "item_width": 10,
    "item_height": 2,
    "item_dimensions_unit": "in",
    "readiness_state_id": 1402821341167,
}


def get_etsy_access_token():
    pb_token = pb_authenticate()
    connection = pb_get_connection(pb_token)
    if not connection or connection.get("status") != "connected":
        raise RuntimeError("Etsy is not connected")

    shop_id = connection["shop_id"]
    access_token, new_refresh_token = refresh_access_token(connection["refresh_token"])
    if new_refresh_token != connection.get("refresh_token"):
        pb_save_connection(pb_token, {"refresh_token": new_refresh_token})

    return shop_id, access_token


def create_draft_listing(shop_id, access_token, listing_fields):
    """POST /shops/{shop_id}/listings - creates the listing itself (no images,
    no SKU/per-variation price yet - those go on via a separate inventory
    call). `state` defaults to "draft" here and is NOT meant to be widely
    overridden - a draft is private/free and reviewable before ever
    publishing, matching this project's "real, consequential Etsy writes
    need a safe intermediate step" posture (same reasoning as the Amazon
    Listings API standalone-relist work)."""
    headers = {
        "x-api-key": api_key_header(),
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    body = {**POUF_COVER_TEMPLATE_DEFAULTS, "state": "draft", **listing_fields}
    response = requests.post(
        f"{ETSY_API_BASE}/shops/{shop_id}/listings",
        headers=headers,
        json=body,
        timeout=30,
    )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Create listing failed: HTTP {response.status_code} - {response.text}")
    return response.json()


def set_listing_sku_and_price(shop_id, listing_id, access_token, sku, price, quantity):
    """PUT /listings/{listing_id}/inventory - a freshly-created listing has
    no SKU/price of its own yet (create_draft_listing's `price`/`quantity`
    are just an initial default), so this is required, not optional, to get
    a real SKU + the intended price onto a single-variation listing. Single
    product/offering (no variation properties), matching a standalone
    (non-variation-family) listing."""
    headers = {
        "x-api-key": api_key_header(),
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    body = {
        "products": [
            {
                "sku": sku,
                "property_values": [],
                "offerings": [
                    {
                        "price": price,
                        "quantity": quantity,
                        "is_enabled": True,
                        "readiness_state_id": POUF_COVER_TEMPLATE_DEFAULTS["readiness_state_id"],
                    }
                ],
            }
        ]
    }
    response = requests.put(
        f"{ETSY_API_BASE}/listings/{listing_id}/inventory",
        headers=headers,
        json=body,
        timeout=30,
    )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Set inventory failed: HTTP {response.status_code} - {response.text}")
    return response.json()


def upload_listing_image(shop_id, listing_id, access_token, image_path, rank):
    headers = {"x-api-key": api_key_header(), "Authorization": f"Bearer {access_token}"}
    with open(image_path, "rb") as f:
        files = {"image": (os.path.basename(image_path), f, "image/jpeg")}
        data = {"rank": str(rank)}
        response = requests.post(
            f"{ETSY_API_BASE}/shops/{shop_id}/listings/{listing_id}/images",
            headers=headers,
            files=files,
            data=data,
            timeout=60,
        )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Image upload failed ({image_path}): HTTP {response.status_code} - {response.text}")
    return response.json()


def CreateEtsyListing(request):
    """JSON-only listing creation (title/description/tags/price/qty/sku) -
    no image upload (Etsy needs real files, not practical over a JSON POST
    body from this app's frontend/telegram flow; use upload_listing_image
    directly from a local script for that part, same as this feature's own
    first real use). Real write, gated behind ADMIN_KEY like every other
    Etsy write in this project - creates a DRAFT (private) listing only."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    body = request.get_json(silent=True) or {}
    sku = body.get("sku")
    title = body.get("title")
    description = body.get("description")
    price = body.get("price")
    quantity = body.get("quantity")
    tags = body.get("tags") or []

    missing = [f for f in ["sku", "title", "description", "price", "quantity"] if not body.get(f)]
    if missing:
        return json_response({"error": f"Missing required field(s): {', '.join(missing)}"}, 400)
    if len(tags) > 13:
        return json_response({"error": f"Etsy allows at most 13 tags, got {len(tags)}"}, 400)

    try:
        shop_id, access_token = get_etsy_access_token()

        listing_fields = {
            "title": title,
            "description": description,
            "price": price,
            "quantity": quantity,
            "tags": tags,
        }
        for override_field in POUF_COVER_TEMPLATE_DEFAULTS:
            if override_field in body:
                listing_fields[override_field] = body[override_field]

        listing = create_draft_listing(shop_id, access_token, listing_fields)
        listing_id = listing["listing_id"]

        set_listing_sku_and_price(shop_id, listing_id, access_token, sku, price, quantity)

        return json_response({
            "status": "success",
            "listingId": listing_id,
            "editUrl": f"https://www.etsy.com/your/shops/me/listing-editor/edit/{listing_id}",
            "state": listing.get("state"),
        })
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
