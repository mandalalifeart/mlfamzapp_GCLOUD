"""SECOND Etsy hanger listing with a completely different title/tags (per the user 2026-11-18 "do the same with the hangers, completely new keywords"); same options, photos, prices as listing 4620038777. Original notes: Etsy listing for the two mango-wood hangers, per the user 2026-11-18 ("create
hanger listing on etsy, copy all images from amazon, add HangerKeys5 and the
hanger we just sent from lg, price 29.99 for hanger keys and 27.77 for the one
from lg, shipping profile FR").
Options (property 513 "Style"):
  HangerKeys5        - 5-hook half-moon key hanger (B085XMKX3P)   $29.99
  HangerNaturalHeart - 4-pack wooden stamp wall hooks (B085XN6K84) $27.77
Images = the Amazon US catalog images of both (distinct, max 10). Shipping
profile FR (146902273177), return policy / who_made / auto-renew copied from
the shop's existing listings. Taxonomy 12232 Key Holders & Key Hooks.
Default dry run (prints the plan), --apply creates and activates."""
import io
import json
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from EtsyMcfFulfillment import (ETSY_API_BASE, api_key_header, pb_authenticate, pb_get_connection, pb_save_connection,
                                refresh_access_token)

APPLY = "--apply" in sys.argv
SHIPPING_PROFILE = 146902273177
RETURN_POLICY = 1074420118744
TAXONOMY = 12232
READY = 1402821341167  # shop readiness state "ready_to_ship", 1-2 days
OPTIONS = [("5-Hook Key Hanger", "HangerKeys5", 29.99, 5), ("4-Pack Wall Hooks", "HangerNaturalHeart", 27.77, 10)]
IMAGES = ["71tyutu7XFL", "818vamlMj3L", "71BHBGdzGHL", "81OyIoj2haL", "61vSH+VdV6L", "81P3JOieq0L", "714rV0RcPkL",
          "71PhStnBgeL", "81DRrfIgqHL", "8150or7Kx6L"]
TITLE = ("Hand Carved Indian Printing Block Wall Organizer, Rustic Wooden Towel Hook, Hat & Bag Rack, Dog Leash Holder, "
         "Artisan Housewarming Gift")
DESCRIPTION = """A little piece of Indian craft for your wall: these organizers are made from traditional wooden printing blocks - the same carved blocks used to hand-print fabrics in Rajasthan - turned into useful hooks.

✨ Choose your style:

1. 5-Hook Key Hanger – a half-moon carved mandala block with 5 black metal hooks. Perfect for keys, jewelry, masks, dog leashes or small bags by the door.

2. 4-Pack Wall Hooks – four round carved wooden stamps (approx. 5 cm / 2 in diameter, 3.5 cm / 1.4 in deep) used as wall knobs for coats, towels, bags, headphones or jewelry. Screws and wall plugs included.

🌿 Natural materials:
Raw mango wood, hand-carved and hand-finished. Every piece is unique – natural grain, small variations and imperfections are part of its handmade character.

🏡 Perfect for:
Entryway, hallway, kitchen, bathroom, bedroom or kids' room – a creative, useful boho accent for any space. A lovely housewarming, birthday or Christmas gift.

📦 Ships from our EU warehouse."""
TAGS = ["indian wood block", "printing block decor", "rustic towel hook", "hat rack wall", "bag hook", "wooden coat rack",
        "entryway organizer", "wall organizer", "carved wood decor", "dog leash holder", "necklace holder", "artisan made gift",
        "bathroom hooks"]


def main():
    print(json.dumps({"title": TITLE, "len": len(TITLE), "options": OPTIONS, "images": len(IMAGES), "tags": TAGS}, indent=1))
    assert len(TITLE) <= 140 and all(len(t) <= 20 for t in TAGS) and len(TAGS) <= 13
    if not APPLY:
        return
    import requests as _rq
    _pb = pb_authenticate(); _c = pb_get_connection(_pb); _t, _n = refresh_access_token(_c["refresh_token"])
    if _n != _c.get("refresh_token"):
        pb_save_connection(_pb, {"refresh_token": _n})
    for st in ("active", "draft", "inactive"):
        res = _rq.get(f"{ETSY_API_BASE}/shops/{_c['shop_id']}/listings", headers={"x-api-key": api_key_header(), "Authorization": f"Bearer {_t}"},
                      params={"state": st, "limit": 100}).json()
        if any(l["title"] == TITLE for l in res.get("results", [])):
            raise SystemExit(f"already exists ({st}) - not creating a duplicate")
    pb = pb_authenticate()
    conn = pb_get_connection(pb)
    tok, new = refresh_access_token(conn["refresh_token"])
    if new != conn.get("refresh_token"):
        pb_save_connection(pb, {"refresh_token": new})
    h = {"x-api-key": api_key_header(), "Authorization": f"Bearer {tok}"}
    shop = conn["shop_id"]
    r = requests.post(f"{ETSY_API_BASE}/shops/{shop}/listings", headers=h, timeout=60, data={
        "quantity": sum(o[3] for o in OPTIONS), "title": TITLE, "description": DESCRIPTION, "price": min(o[2] for o in OPTIONS),
        "who_made": "i_did", "when_made": "2020_2026", "taxonomy_id": TAXONOMY, "shipping_profile_id": SHIPPING_PROFILE,
        "return_policy_id": RETURN_POLICY, "readiness_state_id": READY, "is_supply": "false", "should_auto_renew": "true", "type": "physical",
        "materials[]": ["mango wood", "metal"], "tags[]": TAGS})
    if r.status_code >= 300:
        raise SystemExit(f"create failed {r.status_code}: {r.text[:500]}")
    lid = r.json()["listing_id"]
    print("draft listing", lid)
    for rank, img in enumerate(IMAGES, 1):
        data = requests.get(f"https://m.media-amazon.com/images/I/{img}.jpg", timeout=60).content
        ir = requests.post(f"{ETSY_API_BASE}/shops/{shop}/listings/{lid}/images", headers=h, timeout=120,
                           files={"image": (f"{img}.jpg", io.BytesIO(data), "image/jpeg")}, data={"rank": rank})
        print(" image", rank, ir.status_code, ir.text[:120] if ir.status_code >= 300 else "")
    inv = {"products": [{"sku": sku, "property_values": [{"property_id": 513, "property_name": "Style", "values": [name]}],
                         "offerings": [{"price": price, "quantity": qty, "is_enabled": True,
                                        "readiness_state_id": READY}]} for name, sku, price, qty in OPTIONS],
           "price_on_property": [513], "quantity_on_property": [513], "sku_on_property": [513],
           "readiness_state_on_property": []}
    ir = requests.put(f"{ETSY_API_BASE}/listings/{lid}/inventory", headers={**h, "Content-Type": "application/json"},
                      json=inv, timeout=60)
    print(" inventory", ir.status_code, ir.text[:300] if ir.status_code >= 300 else "")
    ir.raise_for_status()
    ar = requests.patch(f"{ETSY_API_BASE}/shops/{shop}/listings/{lid}", headers=h, data={"state": "active"}, timeout=60)
    print(" activate", ar.status_code, ar.text[:300] if ar.status_code >= 300 else ar.json().get("state"), ar.json().get("url"))


if __name__ == "__main__":
    main()
