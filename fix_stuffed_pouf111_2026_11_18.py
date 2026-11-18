"""Fix StuffedPouf111 (B0DLWVSB8S) in DE/UK/FR/IT/ES, per the user 2026-11-18
("fix B0DLWVSB8S listing, its stuffed pouf"). The product: a FILLED square
60x60x20 cm floor pouf, hand-woven wool & cotton patchwork stripes with fringe,
zipper on the back, handmade in India (photos). Old titles said "with filling"
and "filler not included" at once; FR/IT described a printed cotton cover; IT had
French bullets; stray it_IT item_name values caused 8541. This replaces title,
bullets, description, colour and size in each store's own language and deletes
item_name/bullet values in other languages.
Default VALIDATION_PREVIEW, --apply submits."""
import json
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

SKU = "StuffedPouf111"
SELLER = os.environ["AMAZON_SELLER_ID_EU"]
TEXT = {
    "DE": ("de_DE", "Mandala Life ART Gefüllter Pouf Sitzpouf 60x60x20 cm – Boho Bodenkissen eckig aus Wolle & Baumwolle, handgewebtes "
                    "Patchwork mit Fransen, Fußhocker Ottomane mit Füllung, handgemacht in Indien",
           ["MIT FÜLLUNG – SOFORT EINSATZBEREIT: Fertig gefüllter, formstabiler Sitzpouf – kein Befüllen nötig. Ideal als Sitzplatz, "
            "Fußhocker oder Beistelltisch.",
            "60 x 60 x 20 CM QUADRATISCH: Großzügiges Bodenkissen für Wohnzimmer, Schlafzimmer, Leseecke, Yoga- und Meditationsraum.",
            "WOLLE & BAUMWOLLE PATCHWORK: Handgewebte Streifen in warmen Natur- und Bunttönen mit verspielten Fransen – jedes Stück ein Unikat.",
            "REISSVERSCHLUSS AUF DER RÜCKSEITE: Der Bezug lässt sich abnehmen; Flecken punktuell reinigen.",
            "HANDGEMACHT IN INDIEN: Von Kunsthandwerkern in Rajasthan gefertigt – ein schönes Geschenk für Boho-Liebhaber."],
           "Gefüllter Boho-Pouf aus handgewebtem Wolle-Baumwolle-Patchwork mit Fransen, 60 x 60 x 20 cm, sofort einsatzbereit. "
           "Perfekt als Sitzpouf, Fußhocker oder Bodenkissen. Handgemacht in Rajasthan, Indien.",
           "Mehrfarbig", "60 x 60 x 20 cm"),
    "UK": ("en_GB", "Mandala Life ART Stuffed Floor Pouffe 60x60x20 cm – Filled Square Boho Pouf Ottoman in Wool & Cotton, Hand-Woven "
                    "Patchwork with Fringe, Footstool Floor Cushion, Handmade in India",
           ["FILLED & READY TO USE: Comes fully stuffed and holds its shape – no filling needed. Use it as a seat, footstool or side table.",
            "60 x 60 x 20 CM SQUARE: Generous floor cushion for the living room, bedroom, reading nook, yoga or meditation space.",
            "WOOL & COTTON PATCHWORK: Hand-woven stripes in warm natural and multicolour tones with playful fringe – every piece is unique.",
            "ZIP ON THE BACK: Removable cover; spot clean.",
            "HANDMADE IN INDIA: Crafted by artisans in Rajasthan – a lovely gift for boho lovers."],
           "Filled boho pouffe in hand-woven wool and cotton patchwork with fringe, 60 x 60 x 20 cm, ready to use. Perfect as a seat, "
           "footstool or floor cushion. Handmade in Rajasthan, India.",
           "Multicolour", "60 x 60 x 20 cm"),
    "FR": ("fr_FR", "Mandala Life ART Pouf rembourré 60x60x20 cm – Pouf carré bohème en laine et coton, patchwork tissé main à franges, "
                    "repose-pieds coussin de sol garni, fait main en Inde",
           ["REMBOURRÉ ET PRÊT À L'EMPLOI : Livré entièrement garni, il garde sa forme – aucun rembourrage nécessaire. Assise, repose-pieds "
            "ou table d'appoint.",
            "60 x 60 x 20 CM CARRÉ : Grand coussin de sol pour le salon, la chambre, le coin lecture, l'espace yoga ou méditation.",
            "PATCHWORK LAINE ET COTON : Bandes tissées à la main aux tons naturels et multicolores avec franges – chaque pièce est unique.",
            "FERMETURE ÉCLAIR AU DOS : Housse amovible ; nettoyage localisé.",
            "FAIT MAIN EN INDE : Fabriqué par des artisans du Rajasthan – une belle idée cadeau."],
           "Pouf bohème rembourré en patchwork de laine et coton tissé main à franges, 60 x 60 x 20 cm, prêt à l'emploi. Idéal comme "
           "assise, repose-pieds ou coussin de sol. Fait main au Rajasthan, Inde.",
           "Multicolore", "60 x 60 x 20 cm"),
    "IT": ("it_IT", "Mandala Life ART Pouf imbottito 60x60x20 cm – Pouf quadrato boho in lana e cotone, patchwork tessuto a mano con "
                    "frange, poggiapiedi cuscino da pavimento pieno, fatto a mano in India",
           ["IMBOTTITO E PRONTO ALL'USO: Arriva già riempito e mantiene la forma – nessuna imbottitura da aggiungere. Seduta, poggiapiedi "
            "o tavolino.",
            "60 x 60 x 20 CM QUADRATO: Ampio cuscino da pavimento per soggiorno, camera da letto, angolo lettura, yoga o meditazione.",
            "PATCHWORK DI LANA E COTONE: Strisce tessute a mano in toni naturali e multicolore con frange – ogni pezzo è unico.",
            "CERNIERA SUL RETRO: Fodera rimovibile; pulire localmente.",
            "FATTO A MANO IN INDIA: Realizzato da artigiani del Rajasthan – un bel regalo per gli amanti dello stile boho."],
           "Pouf boho imbottito in patchwork di lana e cotone tessuto a mano con frange, 60 x 60 x 20 cm, pronto all'uso. Perfetto come "
           "seduta, poggiapiedi o cuscino da pavimento. Fatto a mano in Rajasthan, India.",
           "Multicolore", "60 x 60 x 20 cm"),
    "ES": ("es_ES", "Mandala Life ART Puf relleno 60x60x20 cm – Puf cuadrado bohemio de lana y algodón, patchwork tejido a mano con flecos, "
                    "reposapiés cojín de suelo con relleno, hecho a mano en India",
           ["RELLENO Y LISTO PARA USAR: Llega completamente relleno y mantiene su forma – no hace falta rellenarlo. Asiento, reposapiés "
            "o mesa auxiliar.",
            "60 x 60 x 20 CM CUADRADO: Amplio cojín de suelo para salón, dormitorio, rincón de lectura, yoga o meditación.",
            "PATCHWORK DE LANA Y ALGODÓN: Franjas tejidas a mano en tonos naturales y multicolor con flecos – cada pieza es única.",
            "CREMALLERA EN LA PARTE TRASERA: Funda extraíble; limpieza localizada.",
            "HECHO A MANO EN INDIA: Elaborado por artesanos de Rajastán – un bonito regalo para amantes del estilo boho."],
           "Puf bohemio relleno de patchwork de lana y algodón tejido a mano con flecos, 60 x 60 x 20 cm, listo para usar. Perfecto como "
           "asiento, reposapiés o cojín de suelo. Hecho a mano en Rajastán, India.",
           "Multicolor", "60 x 60 x 20 cm"),
}


def main():
    cred, _ = creds("DE")
    only = [a for a in sys.argv[1:] if not a.startswith("--")] or list(TEXT)
    for mp in only:
        lang, title, bullets, desc, color, size = TEXT[mp]
        m = getattr(Marketplaces, mp)
        mid = m.marketplace_id
        li = ListingsItems(credentials=cred, marketplace=m, timeout=120)
        cur = li.get_listings_item(SELLER, SKU, marketplaceIds=[mid], includedData=["attributes"]).payload["attributes"]
        v = lambda val: [{"value": val, "language_tag": lang, "marketplace_id": mid}]
        patches = []
        for k in ("item_name", "bullet_point", "product_description", "color", "size"):
            other = [x for x in cur.get(k, []) if isinstance(x, dict) and x.get("language_tag") not in (None, lang)]
            if other:
                patches.append({"op": "delete", "path": f"/attributes/{k}", "value": other})
        patches += [
            {"op": "replace", "path": "/attributes/item_name", "value": v(title)},
            {"op": "replace", "path": "/attributes/bullet_point", "value": [{"value": b, "language_tag": lang, "marketplace_id": mid} for b in bullets]},
            {"op": "replace", "path": "/attributes/product_description", "value": v(desc)},
            {"op": "replace", "path": "/attributes/color", "value": v(color)},
            {"op": "replace", "path": "/attributes/size", "value": v(size)},
        ]
        kw = {} if "--apply" in sys.argv else {"mode": "VALIDATION_PREVIEW"}
        r = li.patch_listings_item(SELLER, SKU, marketplaceIds=[mid], body={"productType": "OTTOMAN", "patches": patches},
                                   includedData=["issues"], **kw).payload
        print(mp, len(title), r.get("status"),
              json.dumps([(i["code"], i["severity"], i["message"][:200]) for i in r.get("issues", [])], ensure_ascii=False))
        time.sleep(0.3)


if __name__ == "__main__":
    main()
