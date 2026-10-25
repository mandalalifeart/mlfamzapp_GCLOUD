"""Fix USA listing policy violations (2026-10-25, per the user "fix usa policy
violations"): error 99300 "Bullet Point contains false/promotional claims or
external links" on 27 SKUs (Account Health showed 22 listing policy
violations). Rewrites only the offending phrases in bullet_point (guarantee,
superlatives, comparison with competitors, "perfect gift", store links/arrows)
and PATCHes the whole bullet_point array back (per-index patches fail).
Preview by default; --apply to send."""
import json
import os
import re
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

SKUS = ['POUF_MANDALA', 'POUF_C_SQUARE', 'POUF_PEAC_FILLED', 'MANDALA_TOWEL', 'C8-AW5W-28EY', 'SilverPouf', 'POUF_C_ROUND',
        'black 4', 'MandalaBathTowelSet', 'KT-S4QL-ZX1O', 'CM-XTJJ-8Q50', 'CoverPoufPBlue60_99', 'CoverPillowPRed57A',
        'CoverPoufRengali', 'CoverPoufPBlue56', '6E-6DZD-WH51', 'MandalaHandTowelSet', 'L3-8MLU-32PG', 'BlueMandalaTowelSet',
        'EA-K5IK-I11K', 'PP-LQVQ-OPQ6', 'OrangeMandalaTowelSet', 'O4-AB3B-YYQI', 'MD-NHF4-5ZVV', 'EL-JM5D-4UWG',
        '7Z-2U2K-N15G', 'GreenMandalaTowelSet']

# (pattern, replacement) - applied in order, case-insensitive.
RULES = [
    (r"\s*-?\s*We guarantee your satisfaction[^.!]*[.!]?", ""),
    (r"\b(?:guarantee[ds]?|guaranteed)\b", ""),
    (r"carry an air of quality, detail, and authenticity unmatched by mass-produced imitators", "show real handmade detail"),
    (r"THE PERFECT POUF", "STURDY POUF"),
    (r"unmatched by [^.!,]*", ""),
    (r"\bThose BEST\b", "These"),
    (r"\bBEST\b", ""),
    (r"\bPERFECT GIFT\b", "GIFT IDEA"),
    (r"\bperfect\b", "great"),
    (r"100% NATURAL MATERIALS", "NATURAL COTTON"),
    (r"OUR LUXURY BATH TOWELS", "OUR BATH TOWELS"),
    (r"\bThose Luxury\b", "These"),
    (r"\s*-?\s*Visit Mandala Life Art Store for Pillow Inserts purchase recommendations\.?", " - insert sold separately."),
    (r"\bluxurious\b", "decorative"),
    (r",?\s*visit Mandala Life Art Store\s*[↑]*\s*for insert recommendations", ""),
    (r"\s*available on amazon", ""),
    (r"[↑]+", ""),
    (r"Capture the magic of India with a floor cushion from Mandala Life ART!", ""),
    (r"\s{2,}", " "),
    (r"\s+([,.!])", r"\1"),
]


def clean(text):
    out = text
    for pat, rep in RULES:
        out = re.sub(pat, rep, out, flags=re.IGNORECASE)
    return out.strip(" -–")


mp = Marketplaces.US.marketplace_id
cred = dict(refresh_token=os.environ["REFRESH_TOKEN_USA"], lwa_app_id=os.environ["CLIENT_ID_USA"],
            lwa_client_secret=os.environ["CLIENT_SECRET_USA"])
seller = os.environ["AMAZON_SELLER_ID"]
li = ListingsItems(credentials=cred, marketplace=Marketplaces.US, version="2021-08-01", timeout=120)
apply = "--apply" in sys.argv
for sku in SKUS:
    cur = li.get_listings_item(seller, sku, marketplaceIds=mp, includedData="summaries,attributes").payload
    ptype = cur["summaries"][0]["productType"]
    bullets = cur["attributes"].get("bullet_point") or []
    new = json.loads(json.dumps(bullets))
    changed = []
    for b in new:
        c = clean(b["value"])
        if c != b["value"]:
            changed.append((b["value"], c))
            b["value"] = c
    print(f"\n{sku} ({ptype}): {len(changed)} bullet(s) changed")
    for old, c in changed:
        print("  -", old[:160])
        print("  +", c[:160])
    if apply and changed:
        resp = li.patch_listings_item(seller, sku, marketplaceIds=mp, body={
            "productType": ptype, "patches": [{"op": "replace", "path": "/attributes/bullet_point", "value": new}]}).payload
        print("  ", resp.get("status"), [f"{i.get('severity')} {i.get('code')}: {i.get('message', '')[:120]}" for i in resp.get("issues", [])])
    time.sleep(0.3)
