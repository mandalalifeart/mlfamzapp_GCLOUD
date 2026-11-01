"""Fix the DE/FR/IT/ES titles of the 3 patchwork recycled-cotton pouf covers
(per the user 2026-11-01): the EU catalog showed velvet mushroom / velvet
embroidery titles. Only item_name is patched. Default VALIDATION_PREVIEW;
--apply submits. Usage: python fix_patchwork_titles_2026_11_01.py [--apply]"""
import json
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

SELLER = os.environ["AMAZON_SELLER_ID_EU"]
CRED = dict(refresh_token=os.environ["REFRESH_TOKEN_EU"], lwa_app_id=os.environ["CLIENT_ID_EU"],
            lwa_client_secret=os.environ["CLIENT_SECRET_EU"])
SKUS = {"CoverPoufRengali": 0, "CoverPoufRengaliGreen514": 1, "CoverPoufRengalieBlue515": 2}
COLORS = {
    "DE": ["Bunt", "Aquagrün Mix", "Indigoblau"],
    "FR": ["Multicolore", "Vert Aqua Mix", "Bleu Indigo"],
    "IT": ["Multicolore", "Verde Acqua Mix", "Blu Indaco"],
    "ES": ["Multicolor", "Verde Agua Mix", "Azul Índigo"],
}
TITLES = {
    "DE": ("Mandala Life ART Vintage Patchwork Pouf Bezug Rund 60 x 20 cm, {c} - Handgefertigter Indischer "
           "Sitzpouf Bezug aus Recycelter Baumwolle, Boho Bodenkissen Hülle ohne Füllung", "de_DE"),
    "FR": ("Mandala Life ART Housse de Pouf Patchwork Vintage Ronde 60 x 20 cm, {c} - Housse de Coussin de Sol "
           "Indienne Faite Main en Coton Recyclé, Déco Bohème, sans Rembourrage", "fr_FR"),
    "IT": ("Mandala Life ART Fodera per Pouf Patchwork Vintage Rotonda 60 x 20 cm, {c} - Fodera Cuscino da "
           "Pavimento Indiana Fatta a Mano in Cotone Riciclato, Decorazione Boho, senza Imbottitura", "it_IT"),
    "ES": ("Mandala Life ART Funda de Puf Patchwork Vintage Redonda 60 x 20 cm, {c} - Funda de Cojín de Suelo "
           "India Hecha a Mano de Algodón Reciclado, Decoración Boho, sin Relleno", "es_ES"),
}


# DE B0GLMFWBCC: the catalog's own title wins (error 8541) - needs a Brand
# Registry / Seller Support catalog correction instead.
SKIP = {("DE", "CoverPoufRengalieBlue515")}


def main():
    apply = "--apply" in sys.argv
    out = {}
    for mp, (tpl, lang) in TITLES.items():
        m = getattr(Marketplaces, mp)
        li = ListingsItems(credentials=CRED, marketplace=m, timeout=120)
        for sku, i in SKUS.items():
            if apply and (mp, sku) in SKIP:
                continue
            title = tpl.format(c=COLORS[mp][i])
            cur = li.get_listings_item(SELLER, sku, marketplaceIds=[m.marketplace_id], includedData=["summaries"]).payload
            summ = (cur.get("summaries") or [{}])[0]
            body = {"productType": summ.get("productType"), "patches": [{
                "op": "replace", "path": "/attributes/item_name",
                "value": [{"value": title, "language_tag": lang, "marketplace_id": m.marketplace_id}]}]}
            kw = {"marketplaceIds": [m.marketplace_id], "body": body}
            if not apply:
                kw["mode"] = "VALIDATION_PREVIEW"
            r = li.patch_listings_item(SELLER, sku, **kw).payload
            issues = [(x.get("code"), x.get("severity"), x.get("message", "")[:1200]) for x in r.get("issues", [])]
            out[f"{mp}|{sku}"] = {"asin": summ.get("asin"), "old": summ.get("itemName"), "new": title, "len": len(title),
                                  "status": r.get("status"), "issues": issues}
            print(mp, sku, summ.get("asin"), summ.get("productType"), r.get("status"), len(title), issues)
            time.sleep(1)
    os.makedirs("logs", exist_ok=True)
    json.dump(out, open(f"logs/patchwork_titles_{'apply' if apply else 'preview'}.json", "w"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
