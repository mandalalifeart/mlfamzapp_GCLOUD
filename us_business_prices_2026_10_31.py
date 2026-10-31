"""US business price + quantity discounts (per the user 2026-10-31): business
price (B2B offer) = the normal price on every pouf/home-decor and pareo SKU;
quantity discounts for business buyers - home decor 3+ -10%, 5+ -12%, 10+ -15%;
pareo 3+ -10%, 5+ -15%, 10+ -20%. Only the B2B entry of purchasable_offer is
replaced; a replace on purchasable_offer only touches the entry matching the
value's selectors (marketplace_id/currency/audience), so the normal (ALL)
offer is never sent.
Default VALIDATION_PREVIEW; --apply submits. Usage: [--apply] [--only=SKU,SKU]"""
import json
import os
import sys
import time

import requests
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces as M

MID = M.US.marketplace_id
SID = os.environ["AMAZON_SELLER_ID"]
TIERS = {"home": [(3, 10), (5, 12), (10, 15)], "pareo": [(3, 10), (5, 15), (10, 20)]}
HOME_GROUPS = {"VELVET", "COVER_BOHO", "COVER_VINTAGE", "STUFFED"}
PB = os.environ.get("LOCAL_POCKETBASE_URL", "http://127.0.0.1:8090")


def mapping():
    tok = requests.post(f"{PB}/api/collections/_superusers/auth-with-password",
                        json={"identity": os.environ["POCKETBASE_ADMIN_EMAIL"], "password": os.environ["POCKETBASE_ADMIN_PASSWORD"]}).json()["token"]
    r = requests.get(f"{PB}/api/collections/asin_group_mapping/records", params={"perPage": 1000}, headers={"Authorization": tok}).json()
    return r["items"]


def main():
    apply = "--apply" in sys.argv
    only = next((x.split("=", 1)[1].split(",") for x in sys.argv if x.startswith("--only=")), None)
    li = ListingsItems(credentials=dict(refresh_token=os.environ["REFRESH_TOKEN_USA"], lwa_app_id=os.environ["CLIENT_ID_USA"],
                                        lwa_client_secret=os.environ["CLIENT_SECRET_USA"]), marketplace=M.US, timeout=120)
    results = {}
    for row in mapping():
        g = row["group"]
        kind = "home" if g in HOME_GROUPS else "pareo" if g.startswith("PAREO") else None
        sku = row["sku"]
        if not kind or (only and sku not in only):
            continue
        try:
            p = li.get_listings_item(SID, sku, marketplaceIds=[MID], includedData=["attributes", "summaries"]).payload
        except Exception as exc:
            if "NOT_FOUND" not in str(exc):
                results[sku] = {"error": str(exc)[:200]}
            continue
        po = (p.get("attributes") or {}).get("purchasable_offer") or []
        base = next((x for x in po if x.get("audience") == "ALL"), None)
        if not base or not base.get("our_price"):
            results[sku] = {"skipped": "no normal price"}
            continue
        price = base["our_price"][0]["schedule"][0]["value_with_tax"]
        b2b = {"currency": "USD", "audience": "B2B", "marketplace_id": MID,
               "our_price": [{"schedule": [{"value_with_tax": price}]}],
               "quantity_discount_plan": [{"schedule": [{"discount_type": "percent", "levels": [
                   {"lower_bound": q, "value": pct} for q, pct in TIERS[kind]]}]}]}
        body = {"productType": p["summaries"][0]["productType"],
                "patches": [{"op": "replace", "path": "/attributes/purchasable_offer",
                             "value": [b2b]}]}
        kw = {"marketplaceIds": [MID], "body": body}
        if not apply:
            kw["mode"] = "VALIDATION_PREVIEW"
        r = li.patch_listings_item(SID, sku, **kw).payload
        old = next((x["our_price"][0]["schedule"][0]["value_with_tax"] for x in po if x.get("audience") == "B2B" and x.get("our_price")), None)
        results[sku] = {"kind": kind, "price": price, "oldB2B": old, "status": r.get("status"),
                        "issues": [(i.get("code"), i.get("severity"), i.get("message", "")[:200]) for i in r.get("issues", [])]}
        print(sku, kind, price, old, r.get("status"), results[sku]["issues"] or "", flush=True)
        time.sleep(0.5)
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/us_business_prices_{'apply' if apply else 'preview'}.json", "w") as fh:
        json.dump(results, fh, indent=1)
    print(json.dumps({s: v.get("status") or v for s, v in results.items() if v.get("status") not in ("VALID", "ACCEPTED")}))


if __name__ == "__main__":
    main()
