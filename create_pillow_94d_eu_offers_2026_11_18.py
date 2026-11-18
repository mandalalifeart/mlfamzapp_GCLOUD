"""Offer-only listings for CoverPillowRoundYellow94D on its existing ASIN
B0933HJFF9 in every other EU store (per the user 2026-11-18 "create in all eu
mp", no UK). DE/FR were re-activated by fix_pillow_94d_eu_2026_11_18.py.
Prices = what the sibling round 60 cm pillow cover CoverPillowPlue56A has in
each store (EUR stores 39.99 = DE, PL 139 PLN, SE 399 SEK); FR gets the DE price.
GPSR manufacturer reference copied from the 56A listing of the same store.
Default VALIDATION_PREVIEW, --apply submits."""
import json
import os
import sys

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

APPLY = "--apply" in sys.argv
SKU, ASIN, SIBLING = "CoverPillowRoundYellow94D", "B0933HJFF9", "CoverPillowPlue56A"
SELLER = os.environ["AMAZON_SELLER_ID_EU"]
B2B_LEVELS = [{"lower_bound": 3, "value": 10.0}, {"lower_bound": 5, "value": 12.0}, {"lower_bound": 10, "value": 15.0}]


def main():
    cred, _ = creds("DE")
    for mp in [a for a in sys.argv[1:] if not a.startswith("--")] or ["FR", "IT", "ES", "NL", "BE", "PL", "SE", "IE"]:
        m = getattr(Marketplaces, mp)
        mid = m.marketplace_id
        li = ListingsItems(credentials=cred, marketplace=m, timeout=120)
        sib = li.get_listings_item(SELLER, SIBLING, marketplaceIds=[mid], includedData=["attributes"]).payload["attributes"]
        offers = [o for o in sib.get("purchasable_offer", []) if o.get("audience") in ("ALL", "B2B")]
        if mp == "FR" or not offers:
            offers = [{"currency": "EUR", "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": 39.99}]}]},
                      {"currency": "EUR", "audience": "B2B", "our_price": [{"schedule": [{"value_with_tax": 39.99}]}],
                       "quantity_discount_plan": [{"schedule": [{"discount_type": "percent", "levels": B2B_LEVELS}]}]}]
        offers = [{k: v for k, v in o.items() if k in ("currency", "audience", "our_price", "quantity_discount_plan")} | {"marketplace_id": mid}
                  for o in offers]
        attrs = {"condition_type": [{"value": "new_new", "marketplace_id": mid}],
                 "merchant_suggested_asin": [{"value": ASIN, "marketplace_id": mid}],
                 "purchasable_offer": offers,
                 "fulfillment_availability": [{"fulfillment_channel_code": "AMAZON_EU"}],
                 "batteries_required": [{"value": False, "marketplace_id": mid}],
                 "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mid}]}
        if mp == "IE":
            tag = ((sib.get("item_name") or [{}])[0].get("language_tag")) or "en_IE"
            attrs["item_name"] = [{"value": "Mandala Life ART Round Floor Cushion Cover 24 Inch (60 cm) with Fringe, Turquoise & Pink "
                                            "Oriental Print – Cover Only, Bohemian Cotton Floor Pillow Cover for Meditation, Yoga & Boho Seating",
                                   "language_tag": tag, "marketplace_id": mid}]
        if sib.get("gpsr_manufacturer_reference"):
            attrs["gpsr_manufacturer_reference"] = sib["gpsr_manufacturer_reference"]
        try:
            exists = bool(li.get_listings_item(SELLER, SKU, marketplaceIds=[mid], includedData=["summaries"]).payload)
        except Exception:
            exists = False
        kw = {} if APPLY else {"mode": "VALIDATION_PREVIEW"}
        if exists:
            patches = [{"op": "replace", "path": f"/attributes/{k}", "value": v} for k, v in attrs.items() if k != "merchant_suggested_asin"]
            r = li.patch_listings_item(SELLER, SKU, marketplaceIds=[mid], body={"productType": "DECORATIVE_PILLOW_COVER", "patches": patches},
                                       includedData=["issues"], **kw).payload
        else:
            r = li.put_listings_item(SELLER, SKU, marketplaceIds=[mid], body={"productType": "DECORATIVE_PILLOW_COVER",
                                     "requirements": "LISTING_OFFER_ONLY", "attributes": attrs}, includedData=["issues"], **kw).payload
        price = [(o["audience"], o["currency"], o["our_price"][0]["schedule"][0]["value_with_tax"]) for o in offers]
        print(mp, "patch" if exists else "new", price, r.get("status"),
              json.dumps([(i["code"], i["severity"], i["message"][:140]) for i in r.get("issues", [])], ensure_ascii=False))


if __name__ == "__main__":
    main()
