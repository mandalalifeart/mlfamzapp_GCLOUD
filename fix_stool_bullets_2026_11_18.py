"""Stool259 (turquoise) / Stool260 (orange): replace the old pouf-cover bullets
("fill your cover", "20 colours") and descriptions with footstool text in DE,
UK, FR, ES, NL, BE, PL (IT/SE were done by fix_stools_2026_11_18.py), per the
user 2026-11-18 ("yes"). Titles are kept. Facts: velvet-embroidered padded seat,
mango wood frame, 40 x 30 x 40 cm, handmade in India.
Default VALIDATION_PREVIEW, --apply submits."""
import json
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

SELLER = os.environ["AMAZON_SELLER_ID_EU"]
# per language: (design text 259, design text 260, bullets template, description template); {d} = design text
T = {
    "de_DE": ("in Türkis mit Samtstickerei", "in Orange mit marokkanischer Samtstickerei",
              ["BESTICKTER SAMT: Gepolsterte Sitzfläche aus weichem Samt {d} – ein Boho-Akzent voller Farbe und Textur.",
               "RAHMEN AUS MANGOHOLZ: Stabiles Gestell aus massivem Mangoholz – standfest und langlebig.",
               "40 x 30 x 40 CM: Kompakte Größe – ideal als Fußhocker, kleine Sitzgelegenheit oder Ablage.",
               "VIELSEITIG: Passt ins Wohnzimmer, Schlafzimmer, in den Flur, die Leseecke oder den Yoga-Raum.",
               "HANDGEMACHT IN INDIEN: Von Kunsthandwerkern gefertigt – ein schönes Geschenk für Boho-Liebhaber."],
              "Boho-Fußhocker mit gepolsterter Sitzfläche aus Samt {d} und Rahmen aus Mangoholz, 40 x 30 x 40 cm. Handgemacht in Indien."),
    "en_GB": ("in turquoise with velvet embroidery", "in orange with Moroccan velvet embroidery",
              ["EMBROIDERED VELVET: Padded seat in soft velvet {d} – a boho accent full of colour and texture.",
               "MANGO WOOD FRAME: Sturdy solid mango wood base – stable and durable.",
               "40 x 30 x 40 CM: Compact size – ideal as a footstool, small seat or side stool.",
               "VERSATILE: Fits the living room, bedroom, hallway, reading nook or yoga space.",
               "HANDMADE IN INDIA: Crafted by artisans – a lovely gift for boho lovers."],
              "Boho footstool with a padded velvet seat {d} and a mango wood frame, 40 x 30 x 40 cm. Handmade in India."),
    "fr_FR": ("turquoise brodé de velours", "orange brodé de velours marocain",
              ["VELOURS BRODÉ : Assise rembourrée en velours doux {d} – une touche bohème pleine de couleur et de texture.",
               "STRUCTURE EN BOIS DE MANGUIER : Base robuste en manguier massif, stable et durable.",
               "40 x 30 x 40 CM : Format compact, idéal comme repose-pieds, petite assise ou tabouret d'appoint.",
               "POLYVALENT : Pour le salon, la chambre, l'entrée, le coin lecture ou l'espace yoga.",
               "FAIT MAIN EN INDE : Fabriqué par des artisans – une belle idée cadeau."],
              "Repose-pieds bohème avec assise rembourrée en velours {d} et structure en bois de manguier, 40 x 30 x 40 cm. Fait main en Inde."),
    "es_ES": ("turquesa con bordado de terciopelo", "naranja con bordado marroquí de terciopelo",
              ["TERCIOPELO BORDADO: Asiento acolchado de suave terciopelo {d} – un toque boho lleno de color y textura.",
               "ESTRUCTURA DE MADERA DE MANGO: Base robusta de madera maciza de mango, estable y duradera.",
               "40 x 30 x 40 CM: Tamaño compacto, ideal como reposapiés, asiento pequeño o taburete auxiliar.",
               "VERSÁTIL: Para salón, dormitorio, recibidor, rincón de lectura o espacio de yoga.",
               "HECHO A MANO EN INDIA: Elaborado por artesanos – un bonito regalo para amantes del estilo boho."],
              "Reposapiés boho con asiento acolchado de terciopelo {d} y estructura de madera de mango, 40 x 30 x 40 cm. Hecho a mano en India."),
    "nl_NL": ("in turquoise met fluwelen borduursel", "in oranje met Marokkaans fluwelen borduursel",
              ["GEBORDUURD FLUWEEL: Gestoffeerde zitting van zacht fluweel {d} – een boho-accent vol kleur en textuur.",
               "FRAME VAN MANGOHOUT: Stevig onderstel van massief mangohout, stabiel en duurzaam.",
               "40 x 30 x 40 CM: Compact formaat – ideaal als voetenbank, kleine zitplaats of bijzetkruk.",
               "VEELZIJDIG: Voor woonkamer, slaapkamer, hal, leeshoek of yogaruimte.",
               "HANDGEMAAKT IN INDIA: Gemaakt door ambachtslieden – een mooi cadeau voor boho-liefhebbers."],
              "Boho voetenbank met gestoffeerde zitting van fluweel {d} en frame van mangohout, 40 x 30 x 40 cm. Handgemaakt in India."),
    "pl_PL": ("w kolorze turkusowym z aksamitnym haftem", "w kolorze pomarańczowym z marokańskim aksamitnym haftem",
              ["HAFTOWANY AKSAMIT: Tapicerowane siedzisko z miękkiego aksamitu {d} – akcent boho pełen koloru i faktury.",
               "RAMA Z DREWNA MANGO: Solidna podstawa z litego drewna mango, stabilna i trwała.",
               "40 x 30 x 40 CM: Kompaktowy rozmiar – idealny jako podnóżek, małe siedzisko lub stołek.",
               "WSZECHSTRONNY: Do salonu, sypialni, przedpokoju, kącika do czytania lub jogi.",
               "RĘCZNIE ROBIONY W INDIACH: Wykonany przez rzemieślników – piękny prezent dla miłośników stylu boho."],
              "Podnóżek boho z tapicerowanym siedziskiem z aksamitu {d} i ramą z drewna mango, 40 x 30 x 40 cm. Ręcznie robiony w Indiach."),
}
T["fr_BE"] = T["fr_FR"]
STORES = {"DE": "de_DE", "UK": "en_GB", "FR": "fr_FR", "ES": "es_ES", "NL": "nl_NL", "BE": "fr_BE", "PL": "pl_PL"}


def main():
    cred, _ = creds("DE")
    kw = {} if "--apply" in sys.argv else {"mode": "VALIDATION_PREVIEW"}
    for sku, idx in (("Stool259", 0), ("Stool260", 1)):
        for mp, lang in STORES.items():
            m = getattr(Marketplaces, mp)
            mid = m.marketplace_id
            li = ListingsItems(credentials=cred, marketplace=m, timeout=120)
            item = li.get_listings_item(SELLER, sku, marketplaceIds=[mid], includedData=["attributes", "summaries"]).payload
            cur, ptype = item["attributes"], item["summaries"][0]["productType"]
            d = T[lang][idx]
            bullets = [b.format(d=d) for b in T[lang][2]]
            desc = T[lang][3].format(d=d)
            patches = []
            for k in ("bullet_point", "product_description"):
                other = [x for x in cur.get(k, []) if isinstance(x, dict) and x.get("language_tag") not in (None, lang)]
                if other:
                    patches.append({"op": "delete", "path": f"/attributes/{k}", "value": other})
            patches += [{"op": "replace", "path": "/attributes/bullet_point",
                         "value": [{"value": b, "language_tag": lang, "marketplace_id": mid} for b in bullets]},
                        {"op": "replace", "path": "/attributes/product_description",
                         "value": [{"value": desc, "language_tag": lang, "marketplace_id": mid}]}]
            r = li.patch_listings_item(SELLER, sku, marketplaceIds=[mid], body={"productType": ptype, "patches": patches},
                                       includedData=["issues"], **kw).payload
            print(sku, mp, r.get("status"), json.dumps([(i["code"], i["message"][:140]) for i in r.get("issues", [])
                                                        if i["severity"] == "ERROR"], ensure_ascii=False))
            time.sleep(0.3)


if __name__ == "__main__":
    main()
