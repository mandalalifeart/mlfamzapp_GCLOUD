"""Etsy velvet listings: one main product per listing (per the user 2026-11-06:
"prepare it according to the main product, adding others is a bonus").
Main product = title + tags + first variation + first photo; the other velvet
options stay in the listing as extras. Best sellers are left alone.
Preview by default; --apply PATCHes title/tags (old values to logs/)."""
import json, sys

KEEP = {
    "1820248989": "best seller (17 sold) - general embroidered velvet collection",
    "1805545268": "13 sold, orange square V255 leads already",
    "4381419552": "9 sold",
    "1887154375": "6 sold, turquoise sells most",
    "4568866015": "already mushroom only",
    "4609473777": "already leopard main (new)",
    "4609496854": "already footstool main (new)",
}

P = {
    "1820250173": dict(main="CoverPoufV251", why="green V251 = 3 of its 6 sales",
        title="Green Velvet Pouf Cover, Round Embroidered Ottoman Slipcover 24x8, Moroccan Floor Cushion Cover, Boho Footstool Cover",
        tags=["green velvet pouf", "green pouf cover", "velvet pouf cover", "round pouf cover", "embroidered pouf",
              "moroccan pouf cover", "velvet ottoman", "floor cushion cover", "emerald green decor", "boho pouf cover",
              "pouf slipcover", "ottoman cover", "green floor pillow"]),
    "4379916211": dict(main="CoverPoufV256", why="turquoise round, most stock (73); title said green",
        title="Turquoise Velvet Pouf Cover, Round Embroidered Ottoman Slipcover 24x8, Teal Moroccan Floor Cushion Cover, Boho Living Room Decor",
        tags=["turquoise pouf", "turquoise velvet", "teal pouf cover", "velvet pouf cover", "round pouf cover",
              "embroidered pouf", "moroccan pouf cover", "velvet ottoman", "floor cushion cover", "boho pouf cover",
              "pouf slipcover", "ottoman cover", "turquoise decor"]),
    "4378074509": dict(main="CoverPoufV254", why="turquoise square sells in 4 listings",
        title="Turquoise Square Velvet Pouf Cover, Embroidered Floor Pillow Cover 24x24x8, Moroccan Ottoman Slipcover, Boho Square Floor Cushion",
        tags=["square pouf cover", "turquoise pouf", "turquoise velvet", "square floor pillow", "velvet pouf cover",
              "embroidered pouf", "square ottoman", "moroccan pouf cover", "floor cushion cover", "boho pouf cover",
              "pouf slipcover", "teal floor cushion", "square cushion"]),
    "4378065958": dict(main="CoverPoufV257", why="orange round; 'orange pouf' is our best converting Amazon search",
        title="Orange Velvet Pouf Cover, Round Embroidered Ottoman Slipcover 24x8, Burnt Orange Moroccan Floor Cushion Cover, Boho Pouf",
        tags=["orange pouf", "orange pouf cover", "orange ottoman", "burnt orange decor", "velvet pouf cover",
              "round pouf cover", "embroidered pouf", "moroccan pouf cover", "orange floor pillow", "floor cushion cover",
              "boho pouf cover", "pouf slipcover", "velvet ottoman"]),
    "4379838101": dict(main="CoverPoufV511", why="dusty teal with lace has no listing of its own",
        title="Dusty Teal Velvet Pouf Cover with Lace, Mandala Embroidered Round Ottoman Slipcover 24x8, Boho Moroccan Floor Cushion Cover",
        tags=["teal pouf cover", "teal velvet pouf", "dusty teal decor", "mandala pouf cover", "lace pouf cover",
              "velvet pouf cover", "round pouf cover", "embroidered pouf", "moroccan pouf cover", "floor cushion cover",
              "boho pouf cover", "pouf slipcover", "velvet ottoman"]),
    "4377940048": dict(main="CoverPoufV255", why="title says Black but V252 is out of stock; orange square = our best Etsy velvet seller",
        title="Orange Square Velvet Pouf Cover, Embroidered Floor Pillow Cover 24x24x8, Burnt Orange Moroccan Ottoman Slipcover, Boho Floor Cushion",
        tags=["orange pouf", "square pouf cover", "orange pouf cover", "square floor pillow", "orange ottoman",
              "burnt orange decor", "velvet pouf cover", "embroidered pouf", "square ottoman", "moroccan pouf cover",
              "floor cushion cover", "boho pouf cover", "pouf slipcover"]),
    "4569960488": dict(main="CoverPoufVPlain*", why="solid colours 523-527 (22-45 each in stock) have no listing",
        title="Solid Velvet Pouf Cover, Round Moroccan Ottoman Slipcover 24x8 with Mandala Stitching, Mustard Terracotta Maroon Olive Brown",
        tags=["solid velvet pouf", "plain velvet pouf", "velvet pouf cover", "round pouf cover", "mustard pouf",
              "terracotta pouf", "maroon pouf", "olive green pouf", "brown pouf cover", "moroccan pouf cover",
              "velvet ottoman", "floor cushion cover", "pouf slipcover"]),
    "4378075654": dict(main="CoverPoufVLeo540", why="tiger has no listing (leopard has 4609473777)",
        title="Tiger Print Velvet Pouf Cover, Round Jungle Animal Print Ottoman Slipcover 24x8 with Tassels, Boho Floor Cushion Cover",
        tags=["tiger pouf", "tiger print decor", "animal print pouf", "jungle decor", "velvet pouf cover",
              "round pouf cover", "tassel pouf cover", "boho pouf cover", "pouf slipcover", "ottoman cover",
              "floor cushion cover", "leopard pouf cover", "velvet ottoman"]),
}


def check():
    bad = []
    for lid, p in P.items():
        if len(p["title"]) > 140: bad.append((lid, "title", len(p["title"])))
        if len(p["tags"]) != 13 or len(set(p["tags"])) != 13: bad.append((lid, "tag count"))
        bad += [(lid, t, len(t)) for t in p["tags"] if len(t) > 20]
    return bad


BASE = "https://openapi.etsy.com/v3/application"


def auth():
    import requests  # noqa
    sys.path.insert(0, "/home/bababot/AmzBot/gcloud_git")
    from EtsyAuth import pb_authenticate, pb_get_connection, pb_save_connection, refresh_access_token, api_key_header
    pb = pb_authenticate(); c = pb_get_connection(pb)
    at, rt = refresh_access_token(c["refresh_token"])
    if rt != c["refresh_token"]:
        pb_save_connection(pb, {"refresh_token": rt})
    return c["shop_id"], {"x-api-key": api_key_header(), "Authorization": f"Bearer {at}"}


def reorder_inventory(lid, main, H):
    """Main product's rows first (Etsy shows options in product order)."""
    import requests
    inv = requests.get(f"{BASE}/listings/{lid}/inventory", headers=H, timeout=30).json()
    m = main.rstrip("*")
    prods = inv["products"]
    first = [p for p in prods if (p.get("sku") or "").startswith(m)]
    if not first or prods[:len(first)] == first:
        return "already first" if first else "main not found"
    prods = first + [p for p in prods if p not in first]
    body = {"products": [{"sku": p["sku"],
                          "property_values": [{k: v[k] for k in ("property_id", "value_ids", "scale_id", "property_name", "values")}
                                              for v in p["property_values"]],
                          "offerings": [{"price": o["price"]["amount"] / o["price"]["divisor"], "quantity": o["quantity"],
                                         "is_enabled": o["is_enabled"], "readiness_state_id": o.get("readiness_state_id")}
                                        for o in p["offerings"]]} for p in prods]}
    for k in ("price_on_property", "quantity_on_property", "sku_on_property", "readiness_state_on_property"):
        if inv.get(k) is not None:
            body[k] = inv[k]
    for p in body["products"]:
        for o in p["offerings"]:
            if o["readiness_state_id"] is None: o.pop("readiness_state_id")
    r = requests.put(f"{BASE}/listings/{lid}/inventory", headers=H, json=body, timeout=60)
    return r.status_code if r.status_code == 200 else f"{r.status_code} {r.text[:200]}"


if __name__ == "__main__":
    bad = check()
    if bad:
        print("INVALID", bad); sys.exit(1)
    if "--apply" not in sys.argv:
        for lid, p in P.items():
            print(lid, p["main"], len(p["title"]), p["title"])
        sys.exit(0)
    import requests, time
    shop, H = auth()
    only = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--only=")), None)
    log = []
    for lid, p in P.items():
        if only and lid != only: continue
        old = requests.get(f"{BASE}/listings/{lid}", headers=H, timeout=30).json()
        r = requests.patch(f"{BASE}/shops/{shop}/listings/{lid}", headers=H,
                           data={"title": p["title"], "tags": ",".join(p["tags"])}, timeout=60)
        inv = reorder_inventory(lid, p["main"], H) if "--no-reorder" not in sys.argv else "skipped"
        log.append({"listing_id": lid, "main": p["main"], "old_title": old.get("title"), "old_tags": old.get("tags"),
                    "title": p["title"], "tags": p["tags"], "status": r.status_code,
                    "error": None if r.status_code == 200 else r.text[:300], "reorder": inv})
        print(lid, r.status_code, "" if r.status_code == 200 else r.text[:200], "| reorder:", inv)
        time.sleep(0.3)
    path = "/home/bababot/AmzBot/gcloud_git/logs/etsy_velvet_main_product_2026_11_06.json"
    try:
        prev = json.load(open(path))
    except Exception:
        prev = []
    json.dump(prev + log, open(path, "w"), indent=1)
