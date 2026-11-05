"""EU + UK price plan 2026-11-05. Per the user ("lets update change everything and follow it, when bil
changes the prices of german listing to other mp ... we will see how to solve it") - an explicit one-off
exception to the never-edit-BIL-prices rule. Reads approved C rows (page_notes plan|<MP>|<asin>) for
DE/FR/IT/UK, sets our_price of the ALL and B2B offers on that marketplace's listing. Preview by
default, --apply to write."""
import copy, os, sys
import requests
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces
from AsinAnalytics import pb_authenticate, pb_list_all

MPS = {"DE": Marketplaces.DE, "FR": Marketplaces.FR, "IT": Marketplaces.IT, "UK": Marketplaces.UK}
pb = pb_authenticate()
notes = requests.get("http://127.0.0.1:8092/GetPageNotes?page=price-simulation", timeout=30).json()["notes"]
plan = []
for k, v in notes.items():
    parts = (v.get("text") or "").split("|")
    _, mp, asin = k.split("|")
    if k.startswith("plan|") and mp in MPS and parts[0] == "approved" and parts[1] == "C":
        plan.append((mp, asin, float(parts[2])))
cred = dict(refresh_token=os.environ["REFRESH_TOKEN_EU"], lwa_app_id=os.environ["CLIENT_ID_EU"],
            lwa_client_secret=os.environ["CLIENT_SECRET_EU"])
seller = os.environ["AMAZON_SELLER_ID_EU"]
apply = "--apply" in sys.argv
for mp, asin, price in sorted(plan):
    m = MPS[mp]
    hist = sorted(pb_list_all(pb, "price_history", f'marketplace = "{mp}" && asin = "{asin}"', fields="sku,since"),
                  key=lambda r: r["since"])
    if not hist:
        print(mp, asin, "no SKU in price_history - skipped"); continue
    sku = hist[-1]["sku"]
    li = ListingsItems(credentials=cred, marketplace=m, version="2021-08-01", timeout=120)
    cur = li.get_listings_item(seller, sku, marketplaceIds=m.marketplace_id, includedData="summaries,attributes").payload
    ptype = cur["summaries"][0]["productType"]
    new = copy.deepcopy(cur["attributes"].get("purchasable_offer") or [])
    seen = []
    for o in new:
        if o.get("marketplace_id") != m.marketplace_id:
            continue
        old = o["our_price"][0]["schedule"][0].get("value_with_tax")
        o["our_price"] = [{"schedule": [{"value_with_tax": price}]}]
        seen.append(f"{o.get('audience', 'ALL')} {old}->{price}")
    print(mp, asin, sku, ptype, "; ".join(seen))
    if not apply or not seen:
        continue
    resp = li.patch_listings_item(seller, sku, marketplaceIds=m.marketplace_id, body={
        "productType": ptype, "patches": [{"op": "replace", "path": "/attributes/purchasable_offer", "value": new}]}).payload
    print("  ", resp.get("status"), [f"{i.get('severity')} {i.get('code')}: {i.get('message','')[:150]}" for i in resp.get("issues", [])])
