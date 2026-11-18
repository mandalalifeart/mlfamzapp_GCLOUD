"""Stool259 (B0DCQ6QRHQ, turquoise) / Stool260 (B0DCQ7FY65, orange) fixes, per
the user 2026-11-18 ("yes" to fixing IT + SE, "what the size says?").
- IT: title described a round cotton floor cushion cover -> new Italian title,
  bullets and description for the footstool.
- SE: English title/bullets -> Swedish.
- size in all EU stores: values were "Hocker", "repose-pieds", "L", "XL",
  "60x20cm" (wrong - that is the pouf size) -> "40 x 30 x 40 cm" (the item
  dimensions stored in every store).
Default VALIDATION_PREVIEW, --apply submits."""
import json
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

SELLER = os.environ["AMAZON_SELLER_ID_EU"]
SIZE = "40 x 30 x 40 cm"
LANG = {"DE": "de_DE", "UK": "en_GB", "FR": "fr_FR", "IT": "it_IT", "ES": "es_ES", "NL": "nl_NL", "BE": "fr_BE", "PL": "pl_PL",
        "SE": "sv_SE"}
COLOR = {"Stool259": {"IT": "Turchese", "SE": "Turkos"}, "Stool260": {"IT": "Arancione", "SE": "Orange"}}
IT_DESIGN = {"Stool259": "turchese con ricamo in velluto", "Stool260": "arancione con ricamo marocchino in velluto"}
SE_DESIGN = {"Stool259": "turkos med sammetsbroderi", "Stool260": "orange med marockanskt sammetsbroderi"}


def texts(sku, mp):
    if mp == "IT":
        d = IT_DESIGN[sku]
        return ("Mandala Life ART Poggiapiedi decorativo in velluto ricamato con struttura in legno di mango – 40x30x40 cm, sgabello "
                f"imbottito {d.split(' con')[0]}, pouf boho per soggiorno e camera da letto",
                [f"VELLUTO RICAMATO: Seduta imbottita in morbido velluto {d}, un accento boho ricco di colore e texture.",
                 "STRUTTURA IN LEGNO DI MANGO: Robusta base in legno massello di mango, stabile e durevole.",
                 "40 x 30 x 40 CM: Misura compatta, ideale come poggiapiedi, piccola seduta o sgabello d'appoggio.",
                 "VERSATILE: Perfetto in soggiorno, camera da letto, ingresso, angolo lettura o spazio yoga.",
                 "FATTO A MANO IN INDIA: Realizzato da artigiani – un bel regalo per gli amanti dello stile boho."],
                f"Poggiapiedi boho con seduta imbottita in velluto {d} e struttura in legno di mango, 40 x 30 x 40 cm. "
                "Fatto a mano in India.")
    d = SE_DESIGN[sku]
    return ("Mandala Life ART Dekorativ fotpall i broderad sammet med ram av mangoträ – 40x30x40 cm, stoppad pall "
            f"{d.split(' med')[0]}, boho puff för vardagsrum och sovrum",
            [f"BRODERAD SAMMET: Stoppad sits i mjuk sammet, {d} – en boho-accent full av färg och textur.",
             "RAM AV MANGOTRÄ: Stadig bas i massivt mangoträ, stabil och hållbar.",
             "40 x 30 x 40 CM: Kompakt storlek, perfekt som fotpall, liten sittplats eller avlastningspall.",
             "MÅNGSIDIG: Passar i vardagsrum, sovrum, hall, läshörna eller yogarum.",
             "HANDGJORD I INDIEN: Tillverkad av hantverkare – en fin present till boho-älskare."],
            f"Boho-fotpall med stoppad sits i sammet, {d}, och ram av mangoträ, 40 x 30 x 40 cm. Handgjord i Indien.")


def main():
    cred, _ = creds("DE")
    kw = {} if "--apply" in sys.argv else {"mode": "VALIDATION_PREVIEW"}
    for sku in ("Stool259", "Stool260"):
        for mp, lang in LANG.items():
            m = getattr(Marketplaces, mp)
            mid = m.marketplace_id
            li = ListingsItems(credentials=cred, marketplace=m, timeout=120)
            item = li.get_listings_item(SELLER, sku, marketplaceIds=[mid], includedData=["attributes", "summaries"]).payload
            cur = item["attributes"]
            ptype = item["summaries"][0]["productType"]
            v = lambda val: [{"value": val, "language_tag": lang, "marketplace_id": mid}]
            patches = [{"op": "replace", "path": "/attributes/size", "value": v(SIZE)}]
            if mp in ("IT", "SE"):
                title, bullets, desc = texts(sku, mp)
                for k in ("item_name", "bullet_point", "product_description", "color"):
                    other = [x for x in cur.get(k, []) if isinstance(x, dict) and x.get("language_tag") not in (None, lang)]
                    if other:
                        patches.append({"op": "delete", "path": f"/attributes/{k}", "value": other})
                patches += [{"op": "replace", "path": "/attributes/item_name", "value": v(title)},
                            {"op": "replace", "path": "/attributes/bullet_point",
                             "value": [{"value": b, "language_tag": lang, "marketplace_id": mid} for b in bullets]},
                            {"op": "replace", "path": "/attributes/product_description", "value": v(desc)},
                            {"op": "replace", "path": "/attributes/color", "value": v(COLOR[sku][mp])}]
            r = li.patch_listings_item(SELLER, sku, marketplaceIds=[mid], body={"productType": ptype, "patches": patches},
                                       includedData=["issues"], **kw).payload
            print(sku, mp, ptype, r.get("status"), json.dumps([(i["code"], i["message"][:140]) for i in r.get("issues", [])
                                                               if i["severity"] == "ERROR"], ensure_ascii=False))
            time.sleep(0.3)


if __name__ == "__main__":
    main()
