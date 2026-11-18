"""BE velvet family (parent B0DNF3ZSWD, theme SIZE/COLOR) showed mixed size
buttons ("59T x 59B x 20H cm", "59P x 59l x 20A cm", "60D x 20W x 60H cm",
"Square Pouf Cover - 24x24x8 Inches", ...): children without our own `size`
got one generated from item_depth_width_height (per the user 2026-11-18 "some
items are getting size automatically from DWH, others from size field").
Fix: explicit size + matching item_depth_width_height (cm) on every child:
round 59T x 59B x 20H cm, square 60T x 60B x 20H cm (the user's exact strings);
stools 40 x 30 x 40 cm (both, all stores).
Same fix in every EU store (per the user "do the same on all eu marketplaces"):
MP=BE,DE,FR,... (default BE); SKUs not listed in a store are skipped; the product
type is read from each listing.
Default VALIDATION_PREVIEW, --apply submits."""
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

APPLY = "--apply" in sys.argv
MPS = next((a.split("=")[1] for a in sys.argv if a.startswith("MP=")), "BE").split(",")
LANG = {"BE": "fr_BE", "DE": "de_DE", "FR": "fr_FR", "IT": "it_IT", "ES": "es_ES", "NL": "nl_NL", "SE": "sv_SE", "PL": "pl_PL",
        "IE": "en_IE"}
ROUND = ("59T x 59B x 20H cm", (59, 59, 20))
SQUARE = ("60T x 60B x 20H cm", (60, 60, 20))
STOOL = ("40 x 30 x 40 cm", (40, 30, 40))  # per the user 2026-11-18 "Taille: 40 x 30 x 40 cm", both stools
SKUS = {"CoverPoufV255": SQUARE, "CoverPoufV254": SQUARE,
        "Stool259": STOOL, "Stool260": STOOL, "Stool260_New": STOOL,
        **{s: ROUND for s in ("CoverPoufVLeo541", "CoverPoufVLeo540", "CoverPoufVMagic535", "CoverPoufV252", "CoverPoufV257",
                              "CoverPoufV256", "CoverPoufV251", "CoverPoufV511", "CoverPoufV514")}}


def main():
    for cc in MPS:
        run(cc)


def run(cc):
    c, _ = creds("DE")
    m = getattr(Marketplaces, cc)
    mid = m.marketplace_id
    seller = os.environ["AMAZON_SELLER_ID_EU"]
    li = ListingsItems(credentials=c, marketplace=m, timeout=120)
    kw = {} if APPLY else {"mode": "VALIDATION_PREVIEW"}
    for sku, (size, (d, w, h)) in SKUS.items():
        cm = lambda v: {"unit": "centimeters", "value": v}
        patch = [{"op": "replace", "path": "/attributes/size", "value": [{"marketplace_id": mid, "language_tag": LANG[cc], "value": size}]},
                 {"op": "replace", "path": "/attributes/item_depth_width_height",
                  "value": [{"marketplace_id": mid, "depth": cm(d), "width": cm(w), "height": cm(h)}]}]
        try:
            summ = li.get_listings_item(seller, sku, marketplaceIds=[mid], includedData=["summaries"]).payload["summaries"][0]
        except Exception as e:
            print(cc, sku, "not listed" if "NOT_FOUND" in str(e) else str(e)[:120])
            continue
        r = li.patch_listings_item(seller, sku, marketplaceIds=[mid], body={"productType": summ["productType"], "patches": patch},
                                   includedData=["issues"], **kw).payload
        print(cc, sku, size, r.get("status"), [(i["code"], i["message"][:150]) for i in r.get("issues", []) if i["severity"] == "ERROR"])
        time.sleep(0.3)


if __name__ == "__main__":
    main()
