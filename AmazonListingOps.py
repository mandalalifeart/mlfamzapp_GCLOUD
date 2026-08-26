import os

ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
SELLER_ID = os.environ.get("AMAZON_SELLER_ID", "")


def cors_headers():
    return {
        "Access-Control-Allow-Origin": os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app"),
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    import json
    return json.dumps(body), status, cors_headers()


def listings_client():
    from sp_api.api import ListingsItems
    from sp_api.base import Marketplaces

    credentials = {
        "refresh_token": os.environ["REFRESH_TOKEN_USA"],
        "lwa_app_id": os.environ["CLIENT_ID_USA"],
        "lwa_client_secret": os.environ["CLIENT_SECRET_USA"],
    }
    return ListingsItems(credentials=credentials, marketplace=Marketplaces.US)


def GetAmazonListingItem(request):
    """Read-only: fetches the current live listing data for one seller SKU
    on amazon.com via SP-API's Listings Items API - both a feasibility probe
    (confirms this app's credentials actually have the Listings role granted
    in Seller Central, which nothing else in this codebase has used before)
    and the source of truth for "same values" if the listing is later
    deleted and relisted standalone."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    sku = request.args.get("sku") if hasattr(request, "args") else None
    if not sku:
        return json_response({"error": "sku is required"}, 400)
    if not SELLER_ID:
        return json_response({"error": "AMAZON_SELLER_ID env var is not set"}, 500)

    try:
        client = listings_client()
        resp = client.get_listings_item(
            sellerId=SELLER_ID,
            sku=sku,
            marketplaceIds=[client.marketplace.marketplace_id],
            includedData=["attributes", "issues", "offers", "fulfillmentAvailability", "summaries"],
        )
        return json_response({"listingsAccessGranted": True, "sku": sku, "data": resp.payload})
    except Exception as exc:
        return json_response({
            "listingsAccessGranted": False,
            "sku": sku,
            "error": str(exc),
            "type": exc.__class__.__name__,
        }, 200)
