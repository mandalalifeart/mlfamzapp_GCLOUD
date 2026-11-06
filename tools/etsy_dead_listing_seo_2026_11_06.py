"""New titles + tags for the low-converting Etsy listings (per the user
2026-11-06: "change title and keywords to give those products more
visibility"). Lead keywords come from real buyer searches (Amazon US search
terms with orders, Jan-Oct 2026), one distinct lead keyword per listing so the
listings stop competing with each other and with the best sellers.
Usage: python etsy_dead_listing_seo_2026_11_06.py [--apply] [--only=<id>]"""
import json, sys, time

P = {
 "966813592": ("Moroccan Floor Pillow Cover, Round Embroidered Boho Pouf Cover 24x8 in, Mandala Floor Cushion for Living Room",
  ["moroccan pillow", "floor pillow cover", "round pouf cover", "boho floor cushion", "embroidered pouf", "mandala pouf", "unstuffed pouf", "pouf ottoman cover", "floor seating", "boho living room", "moroccan decor", "large floor pillow", "pouf slipcover"]),
 "1001576486": ("Floor Pillows for Adults, Boho Floor Cushion Covers 24x8 Round or Square, Set Option, Printed Cotton Moroccan Pouf Covers",
  ["floor pillows adults", "floor cushion set", "set of 2 poufs", "boho floor cushion", "pouf cover set", "moroccan pouf cover", "floor seating", "large floor pillow", "living room seating", "unstuffed pouf", "printed pouf cover", "pouf ottoman cover", "boho floor pillows"]),
 "981164345": ("Mandala Floor Cushion Cover, Round Embroidered Cotton Pouf Cover 24x8, Boho Meditation Floor Pillow",
  ["mandala cushion", "mandala floor pillow", "round floor cushion", "embroidered pouf", "cotton pouf cover", "meditation pillow", "boho floor cushion", "round pouf cover", "floor pillow cover", "unstuffed pouf", "yoga floor cushion", "boho chic decor", "pouf slipcover"]),
 "1101048770": ("Meditation Cushion Cover, Round Mandala Floor Pillow 24x8, Embroidered Cotton Boho Pouf Cover for Yoga Room",
  ["meditation cushion", "meditation pillow", "yoga cushion cover", "floor meditation", "meditation room", "round floor pillow", "mandala floor pillow", "embroidered pouf", "boho pouf cover", "floor seating", "yoga room decor", "spiritual decor", "cotton pouf cover"]),
 "967491848": ("Green Pouf Cover Square 24x24x8, Boho Floor Cushion Cover, Printed Cotton Moroccan Ottoman Slipcover, Unstuffed",
  ["green pouf", "square pouf cover", "green floor cushion", "boho floor cushion", "moroccan ottoman", "pouf slipcover", "unstuffed pouf", "square floor pillow", "floor cushion cover", "boho living room", "pouf ottoman cover", "printed pouf", "floor seating"]),
 "1060787586": ("Yoga Pillow for Sitting on Floor, Round Mandala Pouf Cover 24x8, Embroidered Cotton Boho Floor Cushion Cover",
  ["yoga pillow", "floor sitting pillow", "yoga floor cushion", "meditation pillow", "round pouf cover", "mandala floor pillow", "embroidered pouf", "boho floor cushion", "floor cushion cover", "yoga room decor", "unstuffed pouf", "cotton pouf cover", "boho meditation"]),
 "1106681062": ("Square Pouf Covers 24x24x8, Boho Floor Cushion Cover, Printed Cotton Moroccan Pouf Ottoman Slipcover",
  ["square pouf cover", "pouf covers square", "square floor pillow", "square ottoman", "boho floor cushion", "moroccan pouf cover", "pouf slipcover", "unstuffed pouf", "floor cushion cover", "colorful pouf", "pouf ottoman cover", "floor seating", "boho living room"]),
 "1025998051": ("Boho Ottoman Cover, Round Embroidered Mandala Pouf Cover 24x8, Cotton Floor Pouf Slipcover for Living Room",
  ["boho ottoman", "ottoman cover", "boho pouf", "embroidered pouf", "mandala pouf", "round pouf cover", "floor pouf", "pouf slipcover", "bohemian ottoman", "unstuffed pouf", "boho living room", "cotton pouf cover", "foot rest cushion"]),
 "1008334251": ("Round Floor Pillow Cover 24x8, Vintage Printed Cotton Pouf Cover, Boho Floor Cushion Case",
  ["round floor pillow", "floor pillow cover", "round pouf cover", "pouf cover round", "vintage pouf", "printed pouf cover", "boho floor cushion", "floor cushion case", "unstuffed pouf", "pouf slipcover", "moroccan pouf cover", "colorful pouf", "floor seating"]),
 "967413736": ("Outdoor Pouf Cover, Vintage Print Boho Floor Pillow Cover Round or Square, Patio & Balcony Floor Cushion",
  ["outdoor pouf", "outdoor pouf cover", "outdoor ottoman", "patio floor pillow", "balcony cushion", "outdoor cushion", "outdoor floor pillow", "garden seating", "boho patio decor", "pouf slipcover", "square pouf cover", "round pouf cover", "unstuffed pouf"]),
 "981461383": ("Pouf Ottoman Foot Rest Cover, Square Vintage Rug Pouf Cover 24x24x8, Boho Floor Cushion Cover, Unstuffed",
  ["pouf foot rest", "foot rest cushion", "ottoman foot rest", "foot stool cover", "vintage rug pouf", "square pouf cover", "pouf ottoman cover", "boho floor cushion", "unstuffed pouf", "pouf slipcover", "square floor pillow", "living room pouf", "floor seating"]),
 "1386561679": ("Embroidered Pouf Cover Round 24x8, Boho Mandala Floor Pillow Cover, Moroccan Style Ottoman Slipcover, Unstuffed",
  ["embroidered pouf", "embroidered cushion", "round pouf cover", "mandala floor pillow", "moroccan ottoman", "ottoman slipcover", "boho floor cushion", "unstuffed pouf", "floor pillow cover", "boho chic decor", "pouf ottoman cover", "cotton pouf cover", "floor seating"]),
 "967612812": ("Teal Pouf Cover Round 24x8, Boho Floor Cushion Cover, Printed Cotton Moroccan Floor Pillow, Unstuffed",
  ["teal pouf", "teal floor pillow", "teal decor", "round pouf cover", "boho floor cushion", "moroccan pillow", "floor pillow cover", "unstuffed pouf", "pouf slipcover", "printed pouf cover", "pouf ottoman cover", "boho living room", "floor seating"]),
 "1031367921": ("Blue Floor Cushion Cover Square 24x24x8, Large Boho Floor Pillow Cover, Printed Cotton Pouf Ottoman Slipcover",
  ["blue floor cushion", "blue pouf", "square floor pillow", "large floor pillow", "floor cushion cover", "square pouf cover", "boho floor cushion", "pouf ottoman cover", "pouf slipcover", "unstuffed pouf", "floor seating", "blue boho decor", "printed pouf cover"]),
 "967241982": ("Blue Pouf Cover Round or Square, Boho Moroccan Floor Pillow Cover 24x8, Printed Cotton Unstuffed Pouf Ottoman",
  ["blue pouf cover", "blue pouf", "blue floor pillow", "round pouf cover", "square pouf cover", "moroccan pillow", "boho floor cushion", "unstuffed pouf", "pouf ottoman cover", "floor pillow cover", "pouf slipcover", "blue boho decor", "floor seating"]),
 "967409956": ("Orange Pouf Cover Square 24x24x8, Boho Floor Cushion Cover, Printed Cotton Moroccan Ottoman Slipcover",
  ["orange pouf", "orange floor pillow", "orange floor cushion", "square pouf cover", "boho floor cushion", "moroccan ottoman", "ottoman slipcover", "unstuffed pouf", "floor cushion cover", "terracotta decor", "pouf ottoman cover", "boho living room", "floor seating"]),
 "1101687842": ("Patchwork Pouf Cover, Recycled Cotton Boho Ottoman Slipcover 24x8, Colorful Moroccan Floor Cushion, Unstuffed",
  ["patchwork pouf", "patchwork cushion", "recycled cotton", "upcycled decor", "colorful pouf", "boho ottoman", "ottoman slipcover", "moroccan pouf cover", "boho floor cushion", "unstuffed pouf", "eco friendly decor", "pouf ottoman cover", "floor seating"]),
 "981209297": ("Gray Pouf Cover Square 24x24x8, Neutral Boho Floor Cushion Cover, Printed Cotton Moroccan Ottoman, Unstuffed",
  ["gray pouf", "grey pouf cover", "neutral boho decor", "beige floor cushion", "square pouf cover", "boho floor cushion", "moroccan ottoman", "unstuffed pouf", "floor cushion cover", "pouf slipcover", "pouf ottoman cover", "square floor pillow", "floor seating"]),
 "1112982137": ("Floor Cushions for Adults, Square Boho Floor Pillow Cover 24x24x8, Vintage Print Cotton Pouf Cover, Unstuffed",
  ["floor cushions", "floor pillows adults", "large floor cushion", "square floor pillow", "boho floor cushion", "vintage pouf", "square pouf cover", "floor cushion cover", "unstuffed pouf", "pouf slipcover", "living room seating", "pouf ottoman cover", "floor seating"]),
 "981572963": ("Pouf Cover Round 24x8, Moroccan Printed Cotton Floor Cushion Cover, Boho Pouf Ottoman Slipcover",
  ["pouf cover round", "round pouf cover", "pouf cover", "moroccan pouf cover", "floor cushion cover", "boho pouf", "pouf slipcover", "unstuffed pouf", "round floor cushion", "colorful pouf", "pouf ottoman cover", "printed pouf cover", "floor seating"]),
 "967530206": ("Orange Round Pouf Cover 24x8, Boho Floor Pillow Cover, Printed Cotton Moroccan Pouf Ottoman Slipcover",
  ["orange pouf cover", "orange round pouf", "orange floor pillow", "round pouf cover", "boho floor pillow", "moroccan pouf cover", "pouf slipcover", "unstuffed pouf", "floor pillow cover", "terracotta decor", "pouf ottoman cover", "boho living room", "floor seating"]),
 "967205460": ("Colorful Vintage Pouf Cover Round, Patchwork Boho Ottoman Slipcover 24x8, Handmade Moroccan Floor Cushion",
  ["vintage pouf", "colorful pouf", "patchwork pouf", "vintage pouf cover", "boho ottoman", "ottoman slipcover", "round pouf cover", "moroccan pouf cover", "handmade pouf", "unstuffed pouf", "boho floor cushion", "eclectic decor", "floor seating"]),
 "1887154375": ("Embroidered Velvet Pouf Cover, Velvet Meditation Cushion Cover Round 24x8, Boho Velvet Floor Pillow",
  ["velvet pouf cover", "velvet pouf", "embroidered velvet", "velvet cushion", "meditation cushion", "velvet floor pillow", "velvet ottoman", "round pouf cover", "boho velvet decor", "pouf slipcover", "unstuffed pouf", "luxury pouf", "floor seating"]),
}

def check():
    bad = []
    for lid, (title, tags) in P.items():
        if len(title) > 140: bad.append((lid, "title", len(title)))
        if len(tags) != 13 or len(set(t.lower() for t in tags)) != 13: bad.append((lid, "tags count/dupe", len(tags)))
        for t in tags:
            if len(t) > 20: bad.append((lid, t, len(t)))
    return bad

if __name__ == "__main__":
    bad = check()
    if bad:
        print("INVALID", bad); sys.exit(1)
    if "--apply" not in sys.argv:
        print("ok", len(P)); sys.exit(0)
    import requests
    sys.path.insert(0, "/home/bababot/AmzBot/gcloud_git")
    from EtsyAuth import pb_authenticate, pb_get_connection, pb_save_connection, refresh_access_token, api_key_header
    pb = pb_authenticate(); c = pb_get_connection(pb)
    at, rt = refresh_access_token(c["refresh_token"])
    if rt != c["refresh_token"]:
        pb_save_connection(pb, {"refresh_token": rt})
    H = {"x-api-key": api_key_header(), "Authorization": f"Bearer {at}"}
    only = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--only=")), None)
    log = []
    for lid, (title, tags) in P.items():
        if only and lid != only: continue
        old = requests.get(f"https://openapi.etsy.com/v3/application/listings/{lid}", headers=H, timeout=30).json()
        r = requests.patch(f"https://openapi.etsy.com/v3/application/shops/{c['shop_id']}/listings/{lid}",
                           headers=H, data={"title": title, "tags": ",".join(tags)}, timeout=60)
        log.append({"listing_id": lid, "old_title": old.get("title"), "old_tags": old.get("tags"), "title": title, "tags": tags,
                    "status": r.status_code, "error": None if r.status_code == 200 else r.text[:300]})
        print(lid, r.status_code, "" if r.status_code == 200 else r.text[:200])
        time.sleep(0.3)
    json.dump(log, open("/home/bababot/AmzBot/gcloud_git/logs/etsy_dead_listing_seo_2026_11_06.json", "w"), indent=1)
