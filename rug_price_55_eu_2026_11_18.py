"""PinkRug46_222 (B0933CLWMK) to EUR 55 in every EU store, per the user
2026-11-18 ("set ASIN B0933CLWMK to 55 euro on all eu mp") - an explicit
exception to the never-edit-BIL rule. PL/SE = EUR 55 at the BIL formula
(PL x4.4117, SE x10.965), rounded. Only the ALL offer is replaced.
Default VALIDATION_PREVIEW, --apply submits."""
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

SKU = "PinkRug46_222"
PRICES = {"DE": ("EUR", 55.0), "FR": ("EUR", 55.0), "IT": ("EUR", 55.0), "ES": ("EUR", 55.0), "NL": ("EUR", 55.0),
          "BE": ("EUR", 55.0), "IE": ("EUR", 55.0), "PL": ("PLN", 243.0), "SE": ("SEK", 603.0)}
cred, _ = creds("DE")
for mp, (cur, price) in PRICES.items():
    m = getattr(Marketplaces, mp)
    li = ListingsItems(credentials=cred, marketplace=m, timeout=120)
    patch = [{"op": "replace", "path": "/attributes/purchasable_offer", "value": [
        {"marketplace_id": m.marketplace_id, "currency": cur, "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": price}]}]}]}]
    kw = {} if "--apply" in sys.argv else {"mode": "VALIDATION_PREVIEW"}
    r = li.patch_listings_item(os.environ["AMAZON_SELLER_ID_EU"], SKU, marketplaceIds=[m.marketplace_id],
                               body={"productType": "RUG", "patches": patch}, includedData=["issues"], **kw).payload
    print(mp, cur, price, r.get("status"), [(i["code"], i["message"][:100]) for i in r.get("issues", []) if i["severity"] == "ERROR"])
    time.sleep(0.3)
