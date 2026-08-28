import os

import requests

ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
SELLER_ID = os.environ.get("AMAZON_SELLER_ID", "")
POCKETBASE_URL = os.environ.get("POCKETBASE_URL", "").rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ.get("POCKETBASE_ADMIN_EMAIL", "")
POCKETBASE_ADMIN_PASSWORD = os.environ.get("POCKETBASE_ADMIN_PASSWORD", "")
POCKETBASE_RELIST_QUEUE_COLLECTION = os.environ.get("POCKETBASE_RELIST_QUEUE_COLLECTION", "amazon_relist_queue")

GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
REPORT_EMAIL_TO = os.environ.get("REPORT_EMAIL_TO", "")
TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()["token"]


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=15,
    )


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


EU_SELLER_ID = os.environ.get("AMAZON_SELLER_ID_EU", "") or SELLER_ID

# marketplace code -> (credential prefix, sp_api Marketplaces attr name, seller id)
MARKETPLACE_CONFIG = {
    "US": ("USA", "US", SELLER_ID),
    "DE": ("EU", "DE", EU_SELLER_ID),
    "UK": ("EU", "UK", EU_SELLER_ID),
    "FR": ("EU", "FR", EU_SELLER_ID),
    "IT": ("EU", "IT", EU_SELLER_ID),
    "ES": ("EU", "ES", EU_SELLER_ID),
}


def listings_client(marketplace="US"):
    from sp_api.api import ListingsItems
    from sp_api.base import Marketplaces

    cred_prefix, mp_attr, _ = MARKETPLACE_CONFIG[marketplace]
    credentials = {
        "refresh_token": os.environ[f"REFRESH_TOKEN_{cred_prefix}"],
        "lwa_app_id": os.environ[f"CLIENT_ID_{cred_prefix}"],
        "lwa_client_secret": os.environ[f"CLIENT_SECRET_{cred_prefix}"],
    }
    return ListingsItems(credentials=credentials, marketplace=getattr(Marketplaces, mp_attr))


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
    marketplace = (request.args.get("marketplace") if hasattr(request, "args") else None) or "US"
    if not sku:
        return json_response({"error": "sku is required"}, 400)
    if marketplace not in MARKETPLACE_CONFIG:
        return json_response({"error": f"unknown marketplace {marketplace}"}, 400)
    _, _, seller_id = MARKETPLACE_CONFIG[marketplace]
    if not seller_id:
        return json_response({"error": "AMAZON_SELLER_ID env var is not set"}, 500)

    try:
        from sp_api.base import Marketplaces

        client = listings_client(marketplace)
        mp_marketplace_id = getattr(Marketplaces, MARKETPLACE_CONFIG[marketplace][1]).marketplace_id
        resp = client.get_listings_item(
            sellerId=seller_id,
            sku=sku,
            marketplaceIds=[mp_marketplace_id],
            includedData=["attributes", "issues", "offers", "fulfillmentAvailability", "summaries"],
        )
        return json_response({"listingsAccessGranted": True, "sku": sku, "marketplace": marketplace, "data": resp.payload})
    except Exception as exc:
        return json_response({
            "listingsAccessGranted": False,
            "sku": sku,
            "marketplace": marketplace,
            "error": str(exc),
            "type": exc.__class__.__name__,
        }, 200)


def DeleteAmazonListingItem(request):
    """Deletes one live SKU's listing on amazon.com via SP-API Listings
    Items API. Real, customer-facing, largely irreversible (the ASIN's
    existing reviews/sales history/organic ranking are gone once deleted) -
    double-gated (ADMIN_KEY + confirm=yes) same as the MCF order-creation
    function, no bulk path."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    sku = request.args.get("sku") if hasattr(request, "args") else None
    confirm = request.args.get("confirm") if hasattr(request, "args") else None
    if not sku:
        return json_response({"error": "sku is required"}, 400)
    if confirm != "yes":
        return json_response({"error": "Pass confirm=yes to actually delete this live listing"}, 400)
    if not SELLER_ID:
        return json_response({"error": "AMAZON_SELLER_ID env var is not set"}, 500)

    try:
        from sp_api.base import Marketplaces

        client = listings_client()
        resp = client.delete_listings_item(
            sellerId=SELLER_ID,
            sku=sku,
            marketplaceIds=[Marketplaces.US.marketplace_id],
        )
        return json_response({"deleted": True, "sku": sku, "response": resp.payload})
    except Exception as exc:
        return json_response({"deleted": False, "sku": sku, "error": str(exc), "type": exc.__class__.__name__}, 500)


def ProcessAmazonRelistQueue(request):
    """Daily-scheduled: submits the queued relist for any amazon_relist_queue
    row that's due (scheduled_for <= now) and still pending, via
    put_listings_item - creates the new standalone listing (no parentage
    attributes) using the exact attribute set captured at queue time.
    Notifies email + Telegram on each outcome."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    import time

    try:
        from sp_api.base import Marketplaces

        pb_token = pb_authenticate()
        now = int(time.time())
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_RELIST_QUEUE_COLLECTION}/records",
            headers={"Authorization": pb_token},
            params={"filter": f'status = "pending" && scheduled_for <= {now}', "perPage": 50},
            timeout=15,
        )
        response.raise_for_status()
        due = response.json().get("items", [])

        processed = []
        for item in due:
            sku = item["sku"]
            try:
                client = listings_client()
                resp = client.put_listings_item(
                    sellerId=SELLER_ID,
                    sku=sku,
                    marketplaceIds=[Marketplaces.US.marketplace_id],
                    body={
                        "productType": item.get("product_type"),
                        "attributes": item.get("attributes"),
                    },
                )
                # Amazon can return HTTP 200 with status="INVALID" and a real
                # issues list when required attributes are missing - a
                # non-exception response is NOT the same as success, and
                # treating it as one previously reported false "done" here.
                resp_status = (resp.payload or {}).get("status")
                if resp_status not in ("ACCEPTED", "VALID"):
                    raise RuntimeError(
                        f"Amazon rejected the listing (status={resp_status}): {(resp.payload or {}).get('issues')}"
                    )
                requests.patch(
                    f"{POCKETBASE_URL}/api/collections/{POCKETBASE_RELIST_QUEUE_COLLECTION}/records/{item['id']}",
                    headers={"Authorization": pb_token},
                    json={"status": "done"},
                    timeout=15,
                )
                processed.append({"sku": sku, "ok": True, "response": resp.payload})
                text = f"Amazon relist complete: SKU {sku} is live again as a standalone listing (no longer part of its old variation family)."
                succeeded = True
            except Exception as exc:
                requests.patch(
                    f"{POCKETBASE_URL}/api/collections/{POCKETBASE_RELIST_QUEUE_COLLECTION}/records/{item['id']}",
                    headers={"Authorization": pb_token},
                    json={"status": "error", "last_error": str(exc)},
                    timeout=15,
                )
                processed.append({"sku": sku, "ok": False, "error": str(exc)})
                text = f"Amazon relist FAILED for SKU {sku}: {exc}"
                succeeded = False

            from NotificationRouting import notify
            notify(
                "amazon-relist-queue-processor", "amzbot", text,
                is_error=not succeeded, subject=f"Amazon relist - {sku}",
            )

        return json_response({"checked": len(due), "processed": processed})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def PatchAmazonListingAttribute(request):
    """Partial update to ONE attribute path on a live listing via SP-API's
    JSON-Patch-based patch_listings_item, rather than put_listings_item's
    full-attribute-set replace - much lower risk for touching a single field
    (e.g. variation_theme) on a listing with other attributes whose exact
    current state isn't fully known, since a full PUT re-submission risks
    silently dropping/omitting something not captured. Real write, but far
    more surgical than delete+relist; still gated behind ADMIN_KEY."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    body = request.get_json(silent=True) or {}
    sku = body.get("sku")
    path = body.get("path")
    value = body.get("value")
    op = body.get("op", "replace")
    if not sku or not path or (op != "delete" and value is None):
        return json_response({"error": "sku and path are required (value required unless op=delete)"}, 400)

    patch = {"op": op, "path": path}
    if op != "delete":
        patch["value"] = value

    try:
        from sp_api.base import Marketplaces

        client = listings_client()

        # productType must match what Amazon's catalog already has for this
        # SKU's ASIN, not a guessed constant - submitting the wrong one
        # (this used to hardcode "OTTOMAN") produces its own ERROR-severity
        # issue (101067 "product type doesn't match the ASIN's product
        # type") and can prevent the intended patch from actually landing.
        # Caller can still override explicitly via body.product_type.
        product_type = body.get("product_type")
        if not product_type:
            current = client.get_listings_item(
                sellerId=SELLER_ID,
                sku=sku,
                marketplaceIds=[Marketplaces.US.marketplace_id],
                includedData=["summaries"],
            )
            summaries = (current.payload or {}).get("summaries") or []
            product_type = summaries[0].get("productType") if summaries else None
        if not product_type:
            return json_response({"error": f"Could not determine productType for {sku} - pass product_type explicitly"}, 400)

        resp = client.patch_listings_item(
            sellerId=SELLER_ID,
            sku=sku,
            marketplaceIds=[Marketplaces.US.marketplace_id],
            body={
                "productType": product_type,
                "patches": [patch],
            },
        )
        return json_response({"sku": sku, "productType": product_type, "response": resp.payload})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def pb_active_skus():
    """All SKUs from asin_group_mapping, excluding IGNORE-group placeholder
    rows - same exclusion rule GetSalesDepartmentReport uses, so the audit
    doesn't waste calls on SKUs deliberately marked excluded."""
    token = pb_authenticate()
    skus = []
    page = 1
    while True:
        resp = requests.get(
            f"{POCKETBASE_URL}/api/collections/asin_group_mapping/records",
            headers={"Authorization": token},
            params={"page": page, "perPage": 200, "filter": 'group != "IGNORE"'},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        skus.extend(item["sku"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    # de-dupe, preserve order
    seen = set()
    unique = []
    for sku in skus:
        if sku not in seen:
            seen.add(sku)
            unique.append(sku)
    return unique


def AuditAmazonListings(request):
    """Weekly-scheduled: checks every catalog SKU's live listing for
    ERROR-severity issues across all 6 marketplaces this account sells in
    (US via the USA account, DE/UK/FR/IT/ES via the separate EU account),
    and reports a summary. Read-only - only calls get_listings_item, no
    writes - but gated behind ADMIN_KEY like the other Ads/Listings pulls
    since it's a real, paced burst of paid-API calls (~100 SKUs x 6
    marketplaces)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    import time

    # Issue codes to suppress from the audit entirely, per user request
    # (2026-08-28) - "can't access your media at <url>" (300060/300403).
    IGNORED_ISSUE_CODES = {"300060", "300403"}

    try:
        from sp_api.base import Marketplaces

        skus = pb_active_skus()
        by_marketplace = {}
        errored_calls = []

        for marketplace in MARKETPLACE_CONFIG:
            _, mp_attr, seller_id = MARKETPLACE_CONFIG[marketplace]
            mp_id = getattr(Marketplaces, mp_attr).marketplace_id
            client = listings_client(marketplace)
            flagged = []
            checked = 0
            for sku in skus:
                try:
                    resp = client.get_listings_item(
                        sellerId=seller_id,
                        sku=sku,
                        marketplaceIds=[mp_id],
                        includedData=["issues", "summaries"],
                    )
                    payload = resp.payload or {}
                    if not payload.get("summaries"):
                        continue  # not listed in this marketplace, not an error
                    checked += 1
                    error_issues = [
                        iss for iss in payload.get("issues", [])
                        if iss.get("severity") == "ERROR"
                        and str(iss.get("code")) not in IGNORED_ISSUE_CODES
                    ]
                    if error_issues:
                        flagged.append({
                            "sku": sku,
                            "asin": payload["summaries"][0].get("asin"),
                            "issues": [f"{iss.get('code')}: {iss.get('message', '')[:140]}" for iss in error_issues],
                        })
                except Exception as exc:
                    if "NOT_FOUND" not in str(exc):
                        # NOT_FOUND just means this SKU isn't sold in this
                        # marketplace - expected and common, not a real
                        # API failure worth reporting.
                        errored_calls.append(f"{marketplace}/{sku}: {exc}")
                time.sleep(0.4)
            by_marketplace[marketplace] = {"checked": checked, "flagged": flagged}

        total_flagged = sum(len(v["flagged"]) for v in by_marketplace.values())

        # One notification per marketplace (not one combined message) - per
        # user request 2026-08-28, so each marketplace's issues arrive as
        # their own distinct Telegram message instead of one long combined
        # one covering all 6.
        from NotificationRouting import notify
        for marketplace, result in by_marketplace.items():
            marketplace_errors = [e for e in errored_calls if e.startswith(f"{marketplace}/")]
            lines = [f"Weekly Amazon listings audit - {marketplace}: {result['checked']} listed, {len(result['flagged'])} with errors"]
            for item in result["flagged"]:
                lines.append(f"  - {item['sku']} ({item['asin']}): {'; '.join(item['issues'])}")
            if marketplace_errors:
                lines.append(f"\n{len(marketplace_errors)} API call(s) failed during the audit:")
                lines.extend(f"  - {e}" for e in marketplace_errors[:10])
            text = "\n".join(lines)
            notify(
                "amazon-weekly-listings-audit", "amzbot", text,
                is_error=bool(result["flagged"] or marketplace_errors),
                subject=f"Amazon listings audit ({marketplace}) - {len(result['flagged'])} SKU(s) with errors",
            )

        return json_response({
            "skuCount": len(skus),
            "totalFlagged": total_flagged,
            "byMarketplace": by_marketplace,
            "erroredCalls": errored_calls,
        })
    except Exception as exc:
        from NotificationRouting import notify
        notify("amazon-weekly-listings-audit", "amzbot", f"Weekly Amazon listings audit FAILED: {exc}", is_error=True)
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def ProbeEuSellerId(request):
    """One-off diagnostic (not part of the audit pipeline): dumps the raw
    Sellers API marketplace-participation payload for the EU credentials,
    to check whether it reveals the EU account's own seller/account id
    without needing to ask the user to look it up in Seller Central."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    try:
        from sp_api.api import Sellers
        from sp_api.base import Marketplaces

        credentials = {
            "refresh_token": os.environ["REFRESH_TOKEN_EU"],
            "lwa_app_id": os.environ["CLIENT_ID_EU"],
            "lwa_client_secret": os.environ["CLIENT_SECRET_EU"],
        }
        client = Sellers(credentials=credentials, marketplace=Marketplaces.DE)
        resp = client.get_marketplace_participation()
        return json_response({"payload": resp.payload})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
