"""Fix StuffedPouf255 (B0DLWT21YM) in all EU stores, per the user 2026-11-18
("do same for StuffedPouf255"). The product (photos): a FILLED square 60x60x20 cm
floor pouf, orange cotton velvet with a turquoise/blue embroidered mandala on top
and sides, pom-pom trim, zipper on the back, handmade in India. Old titles called
it a pouf COVER, 60 cm diameter (round) or a printed cotton outdoor cushion; colour
said "square turquoise"; stray French it_IT title caused 8541. Same method as
fix_stuffed_pouf111_2026_11_18.py. Default VALIDATION_PREVIEW, --apply submits."""
import json
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

SKU = "StuffedPouf255"
SELLER = os.environ["AMAZON_SELLER_ID_EU"]
S = "60 x 60 x 20 cm"
TEXT = {
    "DE": ("de_DE", "Mandala Life ART Gefüllter Samt Pouf 60x60x20 cm Orange – Boho Sitzpouf eckig mit Mandala Stickerei und Pompons, "
                    "Bodenkissen Fußhocker Ottomane mit Füllung, handgemacht in Indien",
           ["MIT FÜLLUNG – SOFORT EINSATZBEREIT: Fertig gefüllter, formstabiler Sitzpouf – kein Befüllen nötig. Ideal als Sitzplatz, Fußhocker oder Beistelltisch.",
            "60 x 60 x 20 CM QUADRATISCH: Großzügiges Bodenkissen für Wohnzimmer, Schlafzimmer, Leseecke, Yoga- und Meditationsraum.",
            "SAMT MIT MANDALA-STICKEREI: Weicher orangefarbener Samt mit türkis-blauer Mandala-Stickerei oben und an den Seiten, rundum mit Pompons verziert.",
            "REISSVERSCHLUSS AUF DER RÜCKSEITE: Der Bezug lässt sich abnehmen; Flecken punktuell reinigen.",
            "HANDGEMACHT IN INDIEN: Von Kunsthandwerkern gefertigt – ein schönes Geschenk für Boho-Liebhaber."],
           "Gefüllter Boho-Pouf aus orangefarbenem Samt mit türkis-blauer Mandala-Stickerei und Pompons, 60 x 60 x 20 cm, sofort einsatzbereit. Perfekt als Sitzpouf, Fußhocker oder Bodenkissen. Handgemacht in Indien.",
           "Orange", S),
    "UK": ("en_GB", "Mandala Life ART Stuffed Velvet Pouffe 60x60x20 cm Orange – Filled Square Boho Pouf Ottoman with Mandala Embroidery "
                    "and Pom-Poms, Footstool Floor Cushion, Handmade in India",
           ["FILLED & READY TO USE: Comes fully stuffed and holds its shape – no filling needed. Use it as a seat, footstool or side table.",
            "60 x 60 x 20 CM SQUARE: Generous floor cushion for the living room, bedroom, reading nook, yoga or meditation space.",
            "VELVET WITH MANDALA EMBROIDERY: Soft orange velvet with a turquoise and blue embroidered mandala on top and sides, trimmed with pom-poms all around.",
            "ZIP ON THE BACK: Removable cover; spot clean.",
            "HANDMADE IN INDIA: Crafted by artisans – a lovely gift for boho lovers."],
           "Filled boho pouffe in orange velvet with turquoise and blue mandala embroidery and pom-poms, 60 x 60 x 20 cm, ready to use. Perfect as a seat, footstool or floor cushion. Handmade in India.",
           "Orange", S),
    "FR": ("fr_FR", "Mandala Life ART Pouf rembourré en velours 60x60x20 cm Orange – Pouf carré bohème brodé mandala à pompons, "
                    "repose-pieds coussin de sol garni, fait main en Inde",
           ["REMBOURRÉ ET PRÊT À L'EMPLOI : Livré entièrement garni, il garde sa forme – aucun rembourrage nécessaire. Assise, repose-pieds ou table d'appoint.",
            "60 x 60 x 20 CM CARRÉ : Grand coussin de sol pour le salon, la chambre, le coin lecture, l'espace yoga ou méditation.",
            "VELOURS BRODÉ MANDALA : Velours orange doux avec un mandala brodé turquoise et bleu sur le dessus et les côtés, bordé de pompons.",
            "FERMETURE ÉCLAIR AU DOS : Housse amovible ; nettoyage localisé.",
            "FAIT MAIN EN INDE : Fabriqué par des artisans – une belle idée cadeau."],
           "Pouf bohème rembourré en velours orange brodé d'un mandala turquoise et bleu, à pompons, 60 x 60 x 20 cm, prêt à l'emploi. Idéal comme assise, repose-pieds ou coussin de sol. Fait main en Inde.",
           "Orange", S),
    "BE": ("fr_BE", None, None, None, "Orange", S),
    "IT": ("it_IT", "Mandala Life ART Pouf imbottito in velluto 60x60x20 cm Arancione – Pouf quadrato boho con ricamo mandala e pompon, "
                    "poggiapiedi cuscino da pavimento pieno, fatto a mano in India",
           ["IMBOTTITO E PRONTO ALL'USO: Arriva già riempito e mantiene la forma – nessuna imbottitura da aggiungere. Seduta, poggiapiedi o tavolino.",
            "60 x 60 x 20 CM QUADRATO: Ampio cuscino da pavimento per soggiorno, camera da letto, angolo lettura, yoga o meditazione.",
            "VELLUTO RICAMATO MANDALA: Morbido velluto arancione con mandala ricamato turchese e blu sopra e sui lati, bordato di pompon.",
            "CERNIERA SUL RETRO: Fodera rimovibile; pulire localmente.",
            "FATTO A MANO IN INDIA: Realizzato da artigiani – un bel regalo per gli amanti dello stile boho."],
           "Pouf boho imbottito in velluto arancione con mandala ricamato turchese e blu e pompon, 60 x 60 x 20 cm, pronto all'uso. Perfetto come seduta, poggiapiedi o cuscino da pavimento. Fatto a mano in India.",
           "Arancione", S),
    "ES": ("es_ES", "Mandala Life ART Puf relleno de terciopelo 60x60x20 cm Naranja – Puf cuadrado bohemio con bordado mandala y pompones, "
                    "reposapiés cojín de suelo con relleno, hecho a mano en India",
           ["RELLENO Y LISTO PARA USAR: Llega completamente relleno y mantiene su forma – no hace falta rellenarlo. Asiento, reposapiés o mesa auxiliar.",
            "60 x 60 x 20 CM CUADRADO: Amplio cojín de suelo para salón, dormitorio, rincón de lectura, yoga o meditación.",
            "TERCIOPELO CON BORDADO MANDALA: Suave terciopelo naranja con un mandala bordado turquesa y azul arriba y en los lados, rematado con pompones.",
            "CREMALLERA EN LA PARTE TRASERA: Funda extraíble; limpieza localizada.",
            "HECHO A MANO EN INDIA: Elaborado por artesanos – un bonito regalo para amantes del estilo boho."],
           "Puf bohemio relleno de terciopelo naranja con mandala bordado turquesa y azul y pompones, 60 x 60 x 20 cm, listo para usar. Perfecto como asiento, reposapiés o cojín de suelo. Hecho a mano en India.",
           "Naranja", S),
    "NL": ("nl_NL", "Mandala Life ART Gevulde fluwelen poef 60x60x20 cm Oranje – Vierkante boho poef met mandala borduurwerk en pompons, "
                    "voetenbank vloerkussen met vulling, handgemaakt in India",
           ["GEVULD EN KLAAR VOOR GEBRUIK: Wordt volledig gevuld geleverd en houdt zijn vorm – geen vulling nodig. Zitplaats, voetenbank of bijzettafel.",
            "60 x 60 x 20 CM VIERKANT: Ruim vloerkussen voor woonkamer, slaapkamer, leeshoek, yoga- of meditatieruimte.",
            "FLUWEEL MET MANDALA BORDUURWERK: Zacht oranje fluweel met turquoise en blauw geborduurde mandala bovenop en aan de zijkanten, afgewerkt met pompons.",
            "RITS AAN DE ACHTERKANT: Afneembare hoes; plaatselijk reinigen.",
            "HANDGEMAAKT IN INDIA: Gemaakt door ambachtslieden – een mooi cadeau voor boho-liefhebbers."],
           "Gevulde boho poef van oranje fluweel met turquoise en blauw mandala borduurwerk en pompons, 60 x 60 x 20 cm, klaar voor gebruik. Perfect als zitplaats, voetenbank of vloerkussen. Handgemaakt in India.",
           "Oranje", S),
    "PL": ("pl_PL", "Mandala Life ART Wypełniona aksamitna pufa 60x60x20 cm Pomarańczowa – Kwadratowa pufa boho z haftem mandala i pomponami, "
                    "podnóżek poduszka podłogowa z wypełnieniem, ręcznie robiona w Indiach",
           ["WYPEŁNIONA I GOTOWA DO UŻYCIA: Dostarczana w pełni wypełniona, trzyma kształt – nie trzeba jej wypełniać. Siedzisko, podnóżek lub stolik.",
            "60 x 60 x 20 CM KWADRATOWA: Duża poduszka podłogowa do salonu, sypialni, kącika do czytania, jogi lub medytacji.",
            "AKSAMIT Z HAFTEM MANDALA: Miękki pomarańczowy aksamit z turkusowo-niebieską haftowaną mandalą na górze i bokach, wykończony pomponami.",
            "ZAMEK Z TYŁU: Zdejmowany pokrowiec; czyścić miejscowo.",
            "RĘCZNIE ROBIONA W INDIACH: Wykonana przez rzemieślników – piękny prezent dla miłośników stylu boho."],
           "Wypełniona pufa boho z pomarańczowego aksamitu z turkusowo-niebieskim haftem mandala i pomponami, 60 x 60 x 20 cm, gotowa do użycia. Idealna jako siedzisko, podnóżek lub poduszka podłogowa. Ręcznie robiona w Indiach.",
           "Pomarańczowy", S),
    "SE": ("sv_SE", "Mandala Life ART Fylld sammetspuff 60x60x20 cm Orange – Fyrkantig boho puff med mandalabroderi och pompoms, "
                    "fotpall golvkudde med fyllning, handgjord i Indien",
           ["FYLLD OCH KLAR ATT ANVÄNDA: Levereras helt fylld och behåller formen – ingen fyllning behövs. Sittplats, fotpall eller sidobord.",
            "60 x 60 x 20 CM FYRKANTIG: Rymlig golvkudde för vardagsrum, sovrum, läshörna, yoga eller meditation.",
            "SAMMET MED MANDALABRODERI: Mjuk orange sammet med turkos och blå broderad mandala ovanpå och på sidorna, kantad med pompoms.",
            "DRAGKEDJA PÅ BAKSIDAN: Avtagbart överdrag; fläckrengöring.",
            "HANDGJORD I INDIEN: Tillverkad av hantverkare – en fin present till boho-älskare."],
           "Fylld boho puff i orange sammet med turkos och blå mandalabroderi och pompoms, 60 x 60 x 20 cm, klar att använda. Perfekt som sittplats, fotpall eller golvkudde. Handgjord i Indien.",
           "Orange", S),
}
TEXT["BE"] = ("fr_BE",) + TEXT["FR"][1:]


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
