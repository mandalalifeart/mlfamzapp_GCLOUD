"""Own pareo parent SKU per EU store (user's option "B", 2026-11-18).

Root cause found 2026-11-18: the same SKU PAREO_PARENT is the main pareo parent
in FR/IT/ES/NL/BE/SE but on a different ASIN in each store (+ an empty stub in
PL / DE), so Amazon flags it 101078/101077 ("inconsistent across marketplaces")
everywhere and stops processing family changes. Fix: a new parent SKU
PAREO_PARENT_<CC> per store (new clean parent ASIN), the store's children of the
DE family (PAREO_PARENT_XXX, B0FX9BRR1G) relinked to it, each with a colour that
is unique inside the family. Pilot = NL; other stores only after the pilot works.

Step 1 creates the parent (LISTING, APPAREL, parentage parent, theme COLOR,
texts copied from the store's PAREO_PARENT). Step 2 patches each child's
child_parent_sku_relationship / parentage_level / variation_theme / colour.
One SKU per ASIN (the one already linked, else the first).
Default VALIDATION_PREVIEW, --apply submits; MP=NL selects the store."""
import json
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds, listings

APPLY = "--apply" in sys.argv
CC = next((a.split("=")[1] for a in sys.argv if a.startswith("MP=")), "NL")
LANG = {"NL": "nl_NL", "FR": "fr_FR", "IT": "it_IT", "ES": "es_ES", "BE": "fr_BE", "SE": "sv_SE", "PL": "pl_PL", "IE": "en_IE"}[CC]
NEW_PARENT = f"PAREO_PARENT_{CC}"
# Skull pareo + the 2 kaftans stay standalone (per the user 2026-11-18 "remove the kaftans and the skull")
DE_KIDS = ["B072BB24WX", "B07B921LZY", "B07B939QZK", "B07CBMX9BS", "B07QMQKS5J", "B07Q4FKR8C", "B07Q4FM2CL",
           "B07Q5JDW16", "B07Q5JHJRJ", "B08GC3B7C1", "B0B59W8L1P"]
# colour overrides where the store's value is missing or duplicated inside the family
COLOR_FIX = {"NL": {"B0B59W8L1P": "Mandala blauw", "B07CBMX9BS": "Zwart wit", "B06Y22LRTP": "Schedel",
                    "B09ZPQT14V": "Kaftan blauw", "B09ZPRF5NW": "Kaftan paars"},
             # BE (fr_BE): the FR listings' colour names, Pareo4BlackNew has none in FR
             "BE": {"B072BB24WX": "Jaipur", "B07B921LZY": "Pushkar", "B07B939QZK": "Jodpur", "B07CBMX9BS": "Noir blanc",
                    "B07QMQKS5J": "Saphir", "B07Q4FKR8C": "Pourpre", "B07Q4FM2CL": "Demi Bleu", "B07Q5JDW16": "Demi Pourpre",
                    "B07Q5JHJRJ": "Demi Rouge", "B08GC3B7C1": "Varansi", "B0B59W8L1P": "Bleue"}}
FABRIC = {"NL": "Rayon", "FR": "Rayonne", "IT": "Rayon", "ES": "Rayón", "BE": "Rayonne", "SE": "Rayon", "PL": "Wiskoza", "IE": "Rayon"}
SELLER = os.environ["AMAZON_SELLER_ID_EU"]
KEEP_PARENT = ("brand", "bullet_point", "item_name", "product_description", "recommended_browse_nodes", "condition_type")


def main():
    cred, _ = creds("DE")
    m = getattr(Marketplaces, CC)
    mid = m.marketplace_id
    li = ListingsItems(credentials=cred, marketplace=m, timeout=120)
    kw = {} if APPLY else {"mode": "VALIDATION_PREVIEW"}
    try:
        old = li.get_listings_item(SELLER, "PAREO_PARENT", marketplaceIds=[mid], includedData=["attributes"]).payload.get("attributes") or {}
    except Exception:
        old = {}
    if not old.get("item_name") and os.path.exists(f"logs/pareo_parent_{CC}_before_delete_2026_11_18.json"):
        # old PAREO_PARENT deleted in this store (NL/BE 2026-11-18) - texts from the backup taken before the delete
        old = json.load(open(f"logs/pareo_parent_{CC}_before_delete_2026_11_18.json"))["attributes"]
    attrs = {k: v for k, v in old.items() if k in KEEP_PARENT}
    attrs.update({"parentage_level": [{"marketplace_id": mid, "value": "parent"}],
                  "child_parent_sku_relationship": [{"marketplace_id": mid, "child_relationship_type": "variation"}],
                  "variation_theme": [{"name": "COLOR"}],
                  "supplier_declared_has_product_identifier_exemption": [{"marketplace_id": mid, "value": True}],
                  "supplier_declared_dg_hz_regulation": [{"marketplace_id": mid, "value": "not_applicable"}],
                  "batteries_required": [{"marketplace_id": mid, "value": False}],
                  "fabric_type": [{"marketplace_id": mid, "language_tag": LANG, "value": FABRIC[CC]}],
                  "country_of_origin": [{"marketplace_id": mid, "value": "IN"}]})
    r = li.put_listings_item(SELLER, NEW_PARENT, marketplaceIds=[mid], body={"productType": "APPAREL", "requirements": "LISTING",
                                                                            "attributes": attrs}, includedData=["issues"], **kw).payload
    print("parent", NEW_PARENT, r.get("status"), json.dumps([(i["code"], i["severity"], i["message"][:160]) for i in r.get("issues", [])],
                                                            ensure_ascii=False))
    if r.get("status") not in ("VALID", "ACCEPTED"):
        return
    L = listings(CC, "summaries")
    by_asin = {}
    for sku, v in L.items():
        if v["asin"] in DE_KIDS:
            by_asin.setdefault(v["asin"], []).append(sku)
    used = {}
    for asin in DE_KIDS:
        skus = by_asin.get(asin) or []
        if not skus:
            print(asin, "no listing in", CC)
            continue
        best = None
        for s in skus:
            a = li.get_listings_item(SELLER, s, marketplaceIds=[mid], includedData=["attributes"]).payload["attributes"]
            if a.get("child_parent_sku_relationship"):
                best = (s, a)
                break
            best = best or (s, a)
        sku, a = best
        color = COLOR_FIX.get(CC, {}).get(asin) or next((x.get("value") for x in a.get("color", [])), None)
        if not color or color.lower() in used:
            print(asin, sku, "colour missing/duplicate:", color, "(add to COLOR_FIX)")
            continue
        used[color.lower()] = asin
        patch = [{"op": "replace", "path": "/attributes/child_parent_sku_relationship",
                  "value": [{"marketplace_id": mid, "child_relationship_type": "variation", "parent_sku": NEW_PARENT}]},
                 {"op": "replace", "path": "/attributes/parentage_level", "value": [{"marketplace_id": mid, "value": "child"}]},
                 {"op": "replace", "path": "/attributes/variation_theme", "value": [{"name": "COLOR"}]},
                 {"op": "replace", "path": "/attributes/color", "value": [{"marketplace_id": mid, "language_tag": LANG, "value": color}]},
                 {"op": "replace", "path": "/attributes/manufacturer", "value": [{"marketplace_id": mid, "language_tag": LANG, "value": "Mandala Life ART"}]},
                 {"op": "replace", "path": "/attributes/item_type_name",
                  "value": [{"marketplace_id": mid, "language_tag": LANG, "value": "Kaftan" if "aftan" in sku else "Pareo"}]},
                 {"op": "replace", "path": "/attributes/outer", "value": [{"marketplace_id": mid, "material": [
                     {"language_tag": LANG, "value": next((x.get("value") for x in a.get("fabric_type", [])), FABRIC[CC])}]}]}]
        x = li.patch_listings_item(SELLER, sku, marketplaceIds=[mid], body={"productType": "APPAREL", "patches": patch},
                                   includedData=["issues"], **kw).payload
        print(asin, sku, color, x.get("status"), json.dumps([(i["code"], i["message"][:140]) for i in x.get("issues", [])
                                                             if i["severity"] == "ERROR"], ensure_ascii=False))
        time.sleep(0.3)


if __name__ == "__main__":
    main()
