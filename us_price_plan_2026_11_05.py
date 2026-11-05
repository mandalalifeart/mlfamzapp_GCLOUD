"""US price plan 2026-11-05 (per the user "apply prices for usa from price-simulation").
Reads the approved C rows (page_notes plan|US|<asin> = approved|C|<price>|<date>), resolves the US
seller SKU from price_history, and sets our_price of the normal (ALL) offer AND the business (B2B)
offer to the new price (B2B quantity-discount tiers are kept). Preview by default, --apply to write.
US prices are not BIL-managed (BIL copies from DE to the EU only)."""
import copy, json, os, sys
import requests
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces
from AsinAnalytics import pb_authenticate, pb_list_all

pb = pb_authenticate()
notes = requests.get("http://127.0.0.1:8092/GetPageNotes?page=price-simulation", timeout=30).json()["notes"]
plan = {}
for k, v in notes.items():
    parts = (v.get("text") or "").split("|")
    if k.startswith("plan|US|") and parts[0] == "approved" and parts[1] == "C":
        plan[k.split("|")[2]] = float(parts[2])
hist = {}
for r in pb_list_all(pb, "price_history", 'marketplace = "US"', fields="asin,sku,price,since"):
    if r["asin"] in plan and (r["asin"] not in hist or r["since"] >= hist[r["asin"]]["since"]):
        hist[r["asin"]] = r
mp = Marketplaces.US.marketplace_id
cred = dict(refresh_token=os.environ["REFRESH_TOKEN_USA"], lwa_app_id=os.environ["CLIENT_ID_USA"],
            lwa_client_secret=os.environ["CLIENT_SECRET_USA"])
seller = os.environ["AMAZON_SELLER_ID"]
li = ListingsItems(credentials=cred, marketplace=Marketplaces.US, version="2021-08-01", timeout=120)
apply = "--apply" in sys.argv
for asin, price in plan.items():
    sku = hist.get(asin, {}).get("sku")
    if not sku:
        print(asin, "no US SKU in price_history - skipped"); continue
    cur = li.get_listings_item(seller, sku, marketplaceIds=mp, includedData="summaries,attributes").payload
    ptype = cur["summaries"][0]["productType"]
    offers = cur["attributes"].get("purchasable_offer") or []
    new = copy.deepcopy(offers)
    seen = []
    for o in new:
        if o.get("marketplace_id") != mp:
            continue
        aud = o.get("audience", "ALL")
        old = o["our_price"][0]["schedule"][0].get("value_with_tax")
        o["our_price"] = [{"schedule": [{"value_with_tax": price}]}]
        seen.append(f"{aud} {old}->{price}")
    print(asin, sku, ptype, "; ".join(seen))
    if not apply:
        continue
    resp = li.patch_listings_item(seller, sku, marketplaceIds=mp, body={
        "productType": ptype, "patches": [{"op": "replace", "path": "/attributes/purchasable_offer", "value": new}]}).payload
    print("  ", resp.get("status"), [f"{i.get('severity')} {i.get('code')}: {i.get('message','')[:150]}" for i in resp.get("issues", [])])
