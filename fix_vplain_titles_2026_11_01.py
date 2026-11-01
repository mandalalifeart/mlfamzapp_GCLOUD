"""Fix FR/IT/ES titles of the 5 plain velvet pouf covers (per the user
2026-11-01 "check also plain velvet covers titles"): FR had "green velvet
embroidery" on all 5, IT "100% cotton", ES wrong colours. New titles are the
US/DE title translated, with each colour. Only item_name is patched. Default
VALIDATION_PREVIEW; --apply submits."""
import json
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

SELLER = os.environ["AMAZON_SELLER_ID_EU"]
CRED = dict(refresh_token=os.environ["REFRESH_TOKEN_EU"], lwa_app_id=os.environ["CLIENT_ID_EU"],
            lwa_client_secret=os.environ["CLIENT_SECRET_EU"])
SKUS = ["CoverPoufVPlainOrange523", "CoverPoufVPlainBrown524", "CoverPoufVPlainYellow525",
        "CoverPoufVPlainGreen526", "CoverPoufVPlainPurple527"]
COLORS = {
    "FR": ["Orange Brûlé", "Marron Chocolat", "Jaune Moutarde", "Vert Olive", "Violet Prune"],
    "IT": ["Arancione Bruciato", "Marrone Cioccolato", "Giallo Senape", "Verde Oliva", "Viola Prugna"],
    "ES": ["Naranja Tostado", "Marrón Chocolate", "Amarillo Mostaza", "Verde Oliva", "Morado Ciruela"],
}
TITLES = {
    "FR": ("Mandala Life ART Pouf de Sol Marocain en Velours {c} - Coussin Ottoman Rond avec Broderie Mandala, "
           "Coutures en Panneaux Faites Main, Assise Bohème pour Salon, Méditation et Décoration", "fr_FR"),
    "IT": ("Mandala Life ART Pouf da Pavimento Marocchino in Velluto {c} - Cuscino Ottomana Rotondo con Ricamo "
           "Mandala, Cuciture a Pannelli Fatte a Mano, Seduta Boho per Soggiorno e Meditazione", "it_IT"),
    "ES": ("Mandala Life ART Puf de Suelo Marroquí de Terciopelo {c} - Cojín Otomana Redondo con Bordado Mandala, "
           "Costuras en Paneles Hechas a Mano, Asiento Boho para Salón, Meditación y Decoración", "es_ES"),
}


def main():
    apply = "--apply" in sys.argv
    out = {}
    for mp, (tpl, lang) in TITLES.items():
        m = getattr(Marketplaces, mp)
        li = ListingsItems(credentials=CRED, marketplace=m, timeout=120)
        for i, sku in enumerate(SKUS):
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
            print(mp, sku, summ.get("asin"), summ.get("productType"), r.get("status"), len(title),
                  [(c, s, msg[:400]) for c, s, msg in issues])
            time.sleep(1)
    os.makedirs("logs", exist_ok=True)
    json.dump(out, open(f"logs/vplain_titles_{'apply' if apply else 'preview'}.json", "w"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
