"""Etsy listings for SkullPareo and PinkRug46_222, per the user 2026-11-18
("for the pink rug and skull pareo/flag, skull usa-eu shipping profile",
prices "skull pareo 35.55 rug 59.99").
  skull-pareo : SkullPareo as a beach sarong / pareo          EU USA profile
  skull-flag  : SkullPareo as a skull flag / wall tapestry    EU USA profile
  pink-rug    : PinkRug46_222 4x6 ft printed cotton rug       FR profile (stock only at DE FBA)
Photos = Amazon catalog images (SkullPareo US B06Y22LRTP, rug DE B0933CLWMK).
Each listing is created only if no listing with the same title exists.
usage: python tools/etsy_skull_rug_listings_2026_11_18.py [skull-pareo skull-flag pink-rug] [--apply]"""
import io
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from EtsyMcfFulfillment import (ETSY_API_BASE, api_key_header, pb_authenticate, pb_get_connection, pb_save_connection,
                                refresh_access_token)

APPLY = "--apply" in sys.argv
RETURN_POLICY, READY = 1074420118744, 1402821341167
EU_USA, FR = 249133465980, 146902273177
SKULL_IMG = ["81dSvAWrbdL", "91Xw88tnkZL", "71z7Xc+RUUL", "81ZViuoogXL", "81pe3Bu7t8L", "71sER+wNnkL", "A1Ne-YUp8eL", "91bpXAvKrIL"]
RUG_IMG = ["A15DsZqcQML", "A1nLVUpW1NL", "81lB-kyzo7L", "91J4+XmpoHL", "A1Vw+zMe-XL", "A1Hs-EDP71L", "81uRG7bR39L", "91ewlI1nkDL"]
SPECS = {
    "skull-pareo": dict(
        sku="SkullPareo", price=35.55, qty=20, profile=EU_USA, taxonomy=11124, section=33151858, materials=["rayon"], images=SKULL_IMG,
        title="Skull Sarong Beach Cover Up for Women, Boho Skull Pareo Wrap Skirt with Fringe, Rayon Swimsuit Cover Up, Beach Dress & Scarf",
        tags=["skull sarong", "skull pareo", "beach cover up", "swimsuit cover up", "sarong wrap", "beach wrap skirt", "boho beach dress",
              "skull beach wrap", "rayon sarong", "fringe sarong", "pirate beach outfit", "gothic summer wear", "vacation outfit"],
        description="""A bold skull-print sarong for the beach - wear it as a wrap skirt, beach dress, swimsuit cover-up, scarf or beach blanket.

✨ Details:
• Lightweight, soft rayon - drapes beautifully and dries fast
• Hand-printed skull / mandala design with tied fringe edges
• One size, large wrap that fits most

👙 Many ways to wear:
Knot it as a dress, skirt or halter top, throw it over your shoulders on cool evenings, or use it as a beach or picnic throw.

🎁 A fun gift for beach lovers, festival goers and anyone with a dark sense of style.

📦 Ships from our US / EU warehouse."""),
    "skull-flag": dict(
        sku="SkullPareo", price=35.55, qty=20, profile=EU_USA, taxonomy=12109, section=None, materials=["rayon"], images=SKULL_IMG,
        title="Skull Flag Wall Tapestry, Gothic Boho Wall Hanging with Fringe, Large Skull Banner for Dorm, Bedroom & Man Cave Decor",
        tags=["skull flag", "skull tapestry", "gothic wall decor", "skull wall hanging", "dorm room decor", "man cave decor",
              "skull banner", "goth bedroom decor", "halloween tapestry", "large wall fabric", "boho tapestry", "biker decor",
              "alternative decor"],
        description="""Make a statement on your wall with this skull-print fabric flag / tapestry with fringe edges.

✨ Details:
• Lightweight rayon fabric with a hand-printed skull / mandala design
• Tied fringe on the edges for a boho look
• Large size - big enough to cover a wall section, headboard area or window

🏴 Ideas:
Wall hanging or flag in a bedroom, dorm room, studio, man cave or bar; Halloween decoration; window curtain; festival or camping banner. It also doubles as a beach sarong!

📦 Ships from our US / EU warehouse."""),
    "pink-rug": dict(
        sku="PinkRug46_222", price=59.99, qty=8, profile=FR, taxonomy=929, section=None, materials=["cotton"], images=RUG_IMG,
        title="Pink Red Moroccan Area Rug 4x6 ft with Fringe, Vintage Boho Printed Cotton Rug, Washable Bedroom & Living Room Carpet",
        tags=["pink area rug", "moroccan rug", "4x6 rug", "boho rug", "vintage style rug", "washable rug", "cotton rug",
              "fringe rug", "bedroom rug", "living room rug", "pink red decor", "bohemian carpet", "printed rug"],
        description="""A pink and red Moroccan-style area rug with a vintage look and fringe ends - an easy way to warm up any room.

✨ Details:
• Size approx. 4 x 6 ft (120 x 180 cm)
• Digitally printed cotton, flat woven with fringe on both ends
• Lightweight and easy to move
• Machine washable on a gentle, low-temperature cycle - do not tumble dry

🏡 Perfect for:
Bedroom, living room, kids' room, entryway or as a boho wall tapestry.

📦 Ships from our EU warehouse."""),
}


def main():
    names = [a for a in sys.argv[1:] if not a.startswith("--")] or list(SPECS)
    for n in names:
        s = SPECS[n]
        assert len(s["title"]) <= 140 and len(s["tags"]) <= 13 and all(len(t) <= 20 for t in s["tags"]), (n, len(s["title"]), [t for t in s["tags"] if len(t) > 20])
        print(n, len(s["title"]), s["title"], s["price"], s["qty"])
    if not APPLY:
        return
    pb = pb_authenticate()
    conn = pb_get_connection(pb)
    tok, new = refresh_access_token(conn["refresh_token"])
    if new != conn.get("refresh_token"):
        pb_save_connection(pb, {"refresh_token": new})
    h = {"x-api-key": api_key_header(), "Authorization": f"Bearer {tok}"}
    shop = conn["shop_id"]
    existing = set()
    for st in ("active", "draft", "inactive"):
        res = requests.get(f"{ETSY_API_BASE}/shops/{shop}/listings", headers=h, params={"state": st, "limit": 100}).json()
        existing |= {l["title"] for l in res.get("results", [])}
    for n in names:
        s = SPECS[n]
        if s["title"] in existing:
            print(n, "already exists - skipped")
            continue
        data = {"quantity": s["qty"], "title": s["title"], "description": s["description"], "price": s["price"], "who_made": "i_did",
                "when_made": "2020_2026", "taxonomy_id": s["taxonomy"], "shipping_profile_id": s["profile"],
                "return_policy_id": RETURN_POLICY, "readiness_state_id": READY, "is_supply": "false", "should_auto_renew": "true",
                "type": "physical", "materials[]": s["materials"], "tags[]": s["tags"]}
        if s["section"]:
            data["shop_section_id"] = s["section"]
        r = requests.post(f"{ETSY_API_BASE}/shops/{shop}/listings", headers=h, timeout=60, data=data)
        if r.status_code >= 300:
            print(n, "create failed", r.status_code, r.text[:300])
            continue
        lid = r.json()["listing_id"]
        for rank, img in enumerate(s["images"], 1):
            blob = requests.get(f"https://m.media-amazon.com/images/I/{img}.jpg", timeout=60).content
            requests.post(f"{ETSY_API_BASE}/shops/{shop}/listings/{lid}/images", headers=h, timeout=120,
                          files={"image": (f"{img}.jpg", io.BytesIO(blob), "image/jpeg")}, data={"rank": rank})
        inv = {"products": [{"sku": s["sku"], "property_values": [],
                             "offerings": [{"price": s["price"], "quantity": s["qty"], "is_enabled": True, "readiness_state_id": READY}]}]}
        ir = requests.put(f"{ETSY_API_BASE}/listings/{lid}/inventory", headers={**h, "Content-Type": "application/json"}, json=inv, timeout=60)
        ar = requests.patch(f"{ETSY_API_BASE}/shops/{shop}/listings/{lid}", headers=h, data={"state": "active"}, timeout=60)
        print(n, lid, "inventory", ir.status_code, "activate", ar.status_code, ar.json().get("url"))


if __name__ == "__main__":
    main()
