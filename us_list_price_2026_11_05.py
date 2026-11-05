"""US list price $79.99 for 3 covers whose price went above their list price (2026-11-05, user "yes")."""
import os, sys
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces
mp = Marketplaces.US.marketplace_id
li = ListingsItems(credentials=dict(refresh_token=os.environ["REFRESH_TOKEN_USA"], lwa_app_id=os.environ["CLIENT_ID_USA"],
                   lwa_client_secret=os.environ["CLIENT_SECRET_USA"]), marketplace=Marketplaces.US, version="2021-08-01", timeout=120)
seller = os.environ["AMAZON_SELLER_ID"]
for sku in ["CoverPoufRectYellow94A", "CoverPoufRectGray101", "CoverPoufRectGreen104"]:
    cur = li.get_listings_item(seller, sku, marketplaceIds=mp, includedData="summaries,attributes").payload
    ptype = cur["summaries"][0]["productType"]
    print(sku, ptype, cur["attributes"].get("list_price"), "-> 79.99")
    if "--apply" in sys.argv:
        resp = li.patch_listings_item(seller, sku, marketplaceIds=mp, body={"productType": ptype, "patches": [
            {"op": "replace", "path": "/attributes/list_price",
             "value": [{"currency": "USD", "value": 79.99, "marketplace_id": mp}]}]}).payload
        print("  ", resp.get("status"), [f"{i.get('severity')} {i.get('code')}: {i.get('message','')[:150]}" for i in resp.get("issues", [])])
