"""Etsy prices per the user 2026-11-06: every round pouf cover $85, every square
$89, CoverPoufRectPatch111 $139, pillow covers (CoverPillow*) $45.55.
Classification from the listing-inventory dump + rules in etsy_classify.py
(scratchpad). Skipped: disabled offerings, rows without SKU and without a
Shape value, stuffed/stool/bundle/sets, odd current prices (<20 or >110 unless
Patch111). Usage: etsy_price_rule_2026_11_06.py CLASSIFY_JSON [--apply] [--only=<listing_id>]"""
import json, sys, time
import requests
sys.path.insert(0, "/home/bababot/AmzBot/gcloud_git")
from EtsyAuth import pb_authenticate, pb_get_connection, pb_save_connection, refresh_access_token, api_key_header

plan = json.load(open(sys.argv[1]))
apply = "--apply" in sys.argv
only = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--only=")), None)
pb = pb_authenticate(); c = pb_get_connection(pb)
at, rt = refresh_access_token(c["refresh_token"])
if rt != c["refresh_token"]:
    pb_save_connection(pb, {"refresh_token": rt})
H = {"x-api-key": api_key_header(), "Authorization": f"Bearer {at}"}
BASE = "https://openapi.etsy.com/v3/application/listings"

def target(row, product, offering):
    new = row["new"]
    if not new or not offering.get("is_enabled"):
        return None
    has_shape = any(pv["property_name"] == "Shape" for pv in product.get("property_values", []))
    if not product.get("sku") and not has_shape:
        return None
    cur = row["cur"]
    if cur < 20 or (cur > 110 and row["why"] != "patch111"):
        return None
    return new

log = []
for lid, d in plan.items():
    if only and lid != only:
        continue
    inv = requests.get(f"{BASE}/{lid}/inventory", headers=H, timeout=30).json()
    products, changes = [], []
    rows = {(p.get("sku"), json.dumps(p.get("property_values"), sort_keys=True)): None for p in inv["products"]}
    by_key = {}
    for r, p in zip(d["rows"], [p for p in inv["products"] if not p.get("is_deleted")]):
        by_key[p["product_id"]] = r
    props = set()
    for p in inv["products"]:
        if p.get("is_deleted"):
            continue
        o = p["offerings"][0]
        cur = o["price"]["amount"] / o["price"]["divisor"]
        r = by_key.get(p["product_id"])
        new = target(r, p, o) if r and r.get("sku") == p.get("sku") else None
        price = new if new is not None else cur
        if new is not None and abs(new - cur) > 0.001:
            changes.append((p.get("sku") or "/".join(" ".join(v["values"]) for v in p["property_values"]), cur, new))
        for pv in p.get("property_values", []):
            props.add(pv["property_id"])
        products.append({
            "sku": p.get("sku") or "",
            "property_values": [{"property_id": pv["property_id"], "value_ids": pv["value_ids"],
                                 "scale_id": pv.get("scale_id"), "property_name": pv["property_name"],
                                 "values": pv["values"]} for pv in p.get("property_values", [])],
            "offerings": [{"price": price, "quantity": o["quantity"], "is_enabled": o["is_enabled"],
                           "readiness_state_id": o.get("readiness_state_id")}],
        })
    if not changes:
        continue
    pop = inv.get("price_on_property") or []
    prices = {pr["offerings"][0]["price"] for pr in products}
    if len(prices) > 1:
        pop = sorted(props)[:2]  # price may now differ per variation
    body = {"products": products, "price_on_property": pop,
            "quantity_on_property": inv.get("quantity_on_property") or [],
            "sku_on_property": inv.get("sku_on_property") or [],
            "readiness_state_on_property": inv.get("readiness_state_on_property") or []}
    print(lid, d["title"][:50], len(changes), "changes", changes[:4])
    entry = {"listing_id": lid, "changes": changes}
    if apply:
        r = requests.put(f"{BASE}/{lid}/inventory", headers={**H, "Content-Type": "application/json"}, json=body, timeout=60)
        entry["status"] = r.status_code
        if r.status_code != 200:
            entry["error"] = r.text[:400]
            print("  ERROR", r.status_code, r.text[:300])
        time.sleep(0.3)
    log.append(entry)
json.dump(log, open("/home/bababot/AmzBot/gcloud_git/logs/etsy_price_rule_2026_11_06.json", "w"), indent=1)
print("listings", len(log), "changes", sum(len(e["changes"]) for e in log))
