"""Etsy listing for CoverPillowRoundYellow94D, per the user 2026-11-18 ("create
also etsy listing for 94D", "FR shipping profile as well"): round flat 60 cm
floor cushion cover with turquoise fringe, turquoise/pink oriental print, 100%
cotton, back zip, cover only. Photos = the Amazon catalog images of B0933HJFF9
(no Amazon video exists). Price $45.55 (the Etsy pillow-cover price rule of
2026-11-06), quantity 10 (21 at LG). Taxonomy / section as the shop's other
floor-cushion covers (996 / 55782890). Refuses to run twice (title check).
Default dry run, --apply creates and activates."""
import io
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from EtsyMcfFulfillment import (ETSY_API_BASE, api_key_header, pb_authenticate, pb_get_connection, pb_save_connection,
                                refresh_access_token)

APPLY = "--apply" in sys.argv
SKU, PRICE, QTY = "CoverPillowRoundYellow94D", 45.55, 10
SHIPPING_PROFILE, RETURN_POLICY, READY, TAXONOMY, SECTION = 146902273177, 1074420118744, 1402821341167, 996, 55782890
IMAGES = ["81cSMu2A09L", "917iOzndt6L", "91cy7dOdBhL", "81LDj+A3lXL", "91TrH3ExPOL", "91j9clEUB1L", "91s6ZMU08NL",
          "911GM9PU5SL", "81rEmMHyMuL"]
TITLE = ("Round Floor Cushion Cover 24 inch with Fringe, Turquoise Pink Boho Meditation Pillow Cover, Cotton Floor Pillow"
         " Case, Yoga Cushion Cover")
DESCRIPTION = """Bring a relaxed boho vibe to your floor seating with this round floor cushion cover, finished with a playful turquoise fringe all around.

✨ Details:
• Round flat floor cushion cover, approx. 60 cm / 24 inch diameter
• Oriental medallion print in turquoise, pink and cream
• Turquoise fringe trim
• 100% cotton – soft, breathable and easy care
• Zipper on the back
• COVER ONLY – insert / filling not included (fits a 24" round cushion insert)

🧘 Perfect for:
Meditation and yoga corners, reading nooks, living room floor seating, kids' rooms or as a decorative accent cushion. A lovely gift for yoga lovers.

🤲 Handmade in India – small variations in print and color are part of its handmade charm.

📦 Ships from our EU warehouse."""
TAGS = ["floor cushion cover", "round floor pillow", "meditation cushion", "yoga cushion cover", "boho floor pillow",
        "fringe pillow cover", "turquoise pillow", "pink boho decor", "floor seating", "round cushion cover",
        "24 inch floor pillow", "bohemian decor", "zafu cover"]


def main():
    assert len(TITLE) <= 140 and all(len(t) <= 20 for t in TAGS) and len(TAGS) <= 13, (len(TITLE), [t for t in TAGS if len(t) > 20])
    print(TITLE, len(TITLE), PRICE, QTY, len(IMAGES), "images")
    if not APPLY:
        return
    pb = pb_authenticate()
    conn = pb_get_connection(pb)
    tok, new = refresh_access_token(conn["refresh_token"])
    if new != conn.get("refresh_token"):
        pb_save_connection(pb, {"refresh_token": new})
    h = {"x-api-key": api_key_header(), "Authorization": f"Bearer {tok}"}
    shop = conn["shop_id"]
    for st in ("active", "draft", "inactive"):
        res = requests.get(f"{ETSY_API_BASE}/shops/{shop}/listings", headers=h, params={"state": st, "limit": 100}).json()
        if any(l["title"] == TITLE for l in res.get("results", [])):
            raise SystemExit(f"already exists ({st}) - not creating a duplicate")
    r = requests.post(f"{ETSY_API_BASE}/shops/{shop}/listings", headers=h, timeout=60, data={
        "quantity": QTY, "title": TITLE, "description": DESCRIPTION, "price": PRICE, "who_made": "i_did",
        "when_made": "2020_2026", "taxonomy_id": TAXONOMY, "shipping_profile_id": SHIPPING_PROFILE, "return_policy_id": RETURN_POLICY,
        "readiness_state_id": READY, "shop_section_id": SECTION, "is_supply": "false", "should_auto_renew": "true",
        "type": "physical", "materials[]": ["cotton"], "tags[]": TAGS})
    if r.status_code >= 300:
        raise SystemExit(f"create failed {r.status_code}: {r.text[:500]}")
    lid = r.json()["listing_id"]
    print("draft listing", lid)
    for rank, img in enumerate(IMAGES, 1):
        data = requests.get(f"https://m.media-amazon.com/images/I/{img}.jpg", timeout=60).content
        ir = requests.post(f"{ETSY_API_BASE}/shops/{shop}/listings/{lid}/images", headers=h, timeout=120,
                           files={"image": (f"{img}.jpg", io.BytesIO(data), "image/jpeg")}, data={"rank": rank})
        print(" image", rank, ir.status_code)
    inv = {"products": [{"sku": SKU, "property_values": [],
                         "offerings": [{"price": PRICE, "quantity": QTY, "is_enabled": True, "readiness_state_id": READY}]}]}
    ir = requests.put(f"{ETSY_API_BASE}/listings/{lid}/inventory", headers={**h, "Content-Type": "application/json"}, json=inv, timeout=60)
    print(" inventory", ir.status_code, ir.text[:300] if ir.status_code >= 300 else "")
    ar = requests.patch(f"{ETSY_API_BASE}/shops/{shop}/listings/{lid}", headers=h, data={"state": "active"}, timeout=60)
    print(" activate", ar.status_code, ar.json().get("state"), ar.json().get("url"))


if __name__ == "__main__":
    main()
