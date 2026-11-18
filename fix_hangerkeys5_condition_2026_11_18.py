"""HangerKeys5 (B085XMKX3P): DE/FR/BE listings had no condition_type (warning
8115, offer not buyable). Per Seller Support (forwarded by the user 2026-11-18):
child SKU -> set Condition and do a FULL update, not a partial one. This PUTs the
whole offer (LISTING_OFFER_ONLY) with the existing price/fulfilment values
unchanged (BIL prices kept) + condition_type new_new.
Default VALIDATION_PREVIEW, --apply submits."""
import json
import os
import sys

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

S = os.environ["AMAZON_SELLER_ID_EU"]
c, _ = creds("DE")
for mp in ["DE", "FR", "BE"]:
    m = getattr(Marketplaces, mp)
    mid = m.marketplace_id
    li = ListingsItems(credentials=c, marketplace=m, timeout=120)
    a = li.get_listings_item(S, "HangerKeys5", marketplaceIds=[mid], includedData=["attributes"]).payload["attributes"]
    attrs = {k: v for k, v in a.items() if k in ("purchasable_offer", "fulfillment_availability", "country_of_origin",
                                                 "batteries_required", "merchant_shipping_group", "skip_offer")}
    attrs["condition_type"] = [{"value": "new_new", "marketplace_id": mid}]
    attrs["merchant_suggested_asin"] = [{"value": "B085XMKX3P", "marketplace_id": mid}]
    attrs.setdefault("batteries_required", [{"value": False, "marketplace_id": mid}])
    attrs["supplier_declared_dg_hz_regulation"] = [{"value": "not_applicable", "marketplace_id": mid}]
    kw = {} if "--apply" in sys.argv else {"mode": "VALIDATION_PREVIEW"}
    x = li.put_listings_item(S, "HangerKeys5", marketplaceIds=[mid], body={"productType": "STORAGE_HOOK", "requirements": "LISTING_OFFER_ONLY",
                             "attributes": attrs}, includedData=["issues"], **kw).payload
    print(mp, x.get("status"), json.dumps([(i["code"], i["message"][:140]) for i in x.get("issues", [])], ensure_ascii=False))
