"""USA pareo price changes 2026-10-25 (per the user on Telegram).
Fetches each listing's current purchasable_offer, changes only our_price, PATCHes it back."""
import copy, json, os, sys
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

NEW = {"PAREO_B_13": 25.55, "PAREO_ACA_7A": 25.55, "PAREO_ACA_7B": 25.55, "PAREO_ACA_6D": 25.55,
       "PAREO_ACA_6C": 25.55, "PAREO_ACA_6B_NEW": 25.55, "PAREO_B_10": 23.33, "PAREO_ZEN_23": 23.33,
       "SkullPareo": 19.99}
mp = Marketplaces.US.marketplace_id
cred = dict(refresh_token=os.environ["REFRESH_TOKEN_USA"], lwa_app_id=os.environ["CLIENT_ID_USA"],
            lwa_client_secret=os.environ["CLIENT_SECRET_USA"])
seller = os.environ["AMAZON_SELLER_ID"]
li = ListingsItems(credentials=cred, marketplace=Marketplaces.US, version="2021-08-01", timeout=120)
apply = "--apply" in sys.argv
for sku, price in NEW.items():
    cur = li.get_listings_item(seller, sku, marketplaceIds=mp, includedData="summaries,attributes").payload
    ptype = cur["summaries"][0]["productType"]
    offers = cur["attributes"].get("purchasable_offer") or []
    new = copy.deepcopy(offers)
    old = None
    for o in new:
        if o.get("marketplace_id") == mp and o.get("audience", "ALL") == "ALL":
            old = o["our_price"][0]["schedule"][0].get("value_with_tax")
            o["our_price"] = [{"schedule": [{"value_with_tax": price}]}]
    print(sku, ptype, old, "->", price)
    if not apply:
        continue
    resp = li.patch_listings_item(seller, sku, marketplaceIds=mp, body={
        "productType": ptype, "patches": [{"op": "replace", "path": "/attributes/purchasable_offer", "value": new}]}).payload
    print("  ", resp.get("status"), [f"{i.get('severity')} {i.get('code')}: {i.get('message','')[:150]}" for i in resp.get("issues", [])])
