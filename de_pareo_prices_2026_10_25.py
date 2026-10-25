"""UK pareo prices -> GBP 23.33 (2026-10-25, per the user on Telegram).
Preview by default; --apply PATCHes. Fetches each listing's purchasable_offer,
changes only our_price, PATCHes it back.
Note: a direct UK price edit takes these SKUs out of the BIL DE->UK price
sync (the user is removing UK from BIL anyway)."""
import copy
import os
import sys

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

SKUS = ["PAREO_ZEN_5", "PAREO_ZEN_22", "SkullPareo", "PAREO_SOL_551"]
# Per the user ("set maximal prices (maximum 23.33) ... price that amazon will
# not reject the coupon on them"): coupon SKUs (15% coupon) get the highest
# price where price x 0.85 stays >=3% under their 30-day low (lowest of the
# live and the set price today); everything else 23.33.
PRICES = {"PAREO_ZEN_5": 17.49, "PAREO_ZEN_22": 17.49, "SkullPareo": 21.99, "PAREO_SOL_551": 25.55}
PRICE = 23.33
mp = Marketplaces.DE.marketplace_id
cred = dict(refresh_token=os.environ["REFRESH_TOKEN_EU"], lwa_app_id=os.environ["CLIENT_ID_EU"],
            lwa_client_secret=os.environ["CLIENT_SECRET_EU"])
seller = os.environ["AMAZON_SELLER_ID_EU"]
li = ListingsItems(credentials=cred, marketplace=Marketplaces.DE, version="2021-08-01", timeout=120)
apply = "--apply" in sys.argv
for sku in SKUS:
    cur = li.get_listings_item(seller, sku, marketplaceIds=mp, includedData="summaries,attributes").payload
    ptype = cur["summaries"][0]["productType"]
    new = copy.deepcopy(cur["attributes"].get("purchasable_offer") or [])
    old = None
    for o in new:
        if o.get("marketplace_id") == mp and o.get("audience", "ALL") == "ALL":
            old = o["our_price"][0]["schedule"][0].get("value_with_tax")
            o["our_price"] = [{"schedule": [{"value_with_tax": PRICES.get(sku, PRICE)}]}]
    if old is None:
        print(sku, "no UK offer found - skipped")
        continue
    print(sku, ptype, old, "->", PRICES.get(sku, PRICE))
    if not apply:
        continue
    resp = li.patch_listings_item(seller, sku, marketplaceIds=mp, body={
        "productType": ptype, "patches": [{"op": "replace", "path": "/attributes/purchasable_offer", "value": new}]}).payload
    print("  ", resp.get("status"), [f"{i.get('severity')} {i.get('code')}: {i.get('message', '')[:150]}" for i in resp.get("issues", [])])
