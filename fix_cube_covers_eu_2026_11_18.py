"""Cube ottoman covers in all EU stores, per the user 2026-11-18 ("yes all 3
59.99, update their title and bullet points as well, they are coming with liner,
50cm cube, cotton, cover only, I think it is more ottoman than pouf").
SKUs: CoverCubeTribalWB60A (B08427L5TB, black & white tribal on white),
CoverCubeTribalBW59A (B08427XRF6, black & white tribal on black),
CoverCubeGray74 (B08427LS8R, vintage grey).
- price: ALL offer EUR 59.99 (SE 658 kr / PL 265 zł = 59.99 at the BIL formula);
  an explicit user exception to the never-edit-BIL rule
- item_name / bullet_point / product_description rewritten in each store's language:
  cube ottoman cover 50x50x50 cm, cotton, comes with an inner liner, cover only.
Colour/size attributes are left as they are. Default VALIDATION_PREVIEW, --apply submits."""
import json
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

S = os.environ["AMAZON_SELLER_ID_EU"]
SKUS = ["CoverCubeTribalWB60A", "CoverCubeTribalBW59A", "CoverCubeGray74"]
PRICE = {"SE": ("SEK", 658.0), "PL": ("PLN", 265.0)}
STORES = {"DE": "de_DE", "FR": "fr_FR", "IT": "it_IT", "ES": "es_ES", "NL": "nl_NL", "BE": "fr_BE", "SE": "sv_SE", "PL": "pl_PL", "IE": "en_IE"}
DESIGN = {  # lang -> sku -> design words
    "de": {"CoverCubeTribalWB60A": "Tribal Muster Weiß Schwarz", "CoverCubeTribalBW59A": "Tribal Muster Schwarz Weiß", "CoverCubeGray74": "Vintage Grau"},
    "fr": {"CoverCubeTribalWB60A": "motif tribal blanc et noir", "CoverCubeTribalBW59A": "motif tribal noir et blanc", "CoverCubeGray74": "gris vintage"},
    "it": {"CoverCubeTribalWB60A": "motivo tribale bianco e nero", "CoverCubeTribalBW59A": "motivo tribale nero e bianco", "CoverCubeGray74": "grigio vintage"},
    "es": {"CoverCubeTribalWB60A": "estampado tribal blanco y negro", "CoverCubeTribalBW59A": "estampado tribal negro y blanco", "CoverCubeGray74": "gris vintage"},
    "nl": {"CoverCubeTribalWB60A": "tribal print wit zwart", "CoverCubeTribalBW59A": "tribal print zwart wit", "CoverCubeGray74": "vintage grijs"},
    "sv": {"CoverCubeTribalWB60A": "tribalmönster vit svart", "CoverCubeTribalBW59A": "tribalmönster svart vit", "CoverCubeGray74": "vintage grå"},
    "pl": {"CoverCubeTribalWB60A": "wzór plemienny biało-czarny", "CoverCubeTribalBW59A": "wzór plemienny czarno-biały", "CoverCubeGray74": "szary vintage"},
    "en": {"CoverCubeTribalWB60A": "Black & White Tribal on White", "CoverCubeTribalBW59A": "Black & White Tribal on Black", "CoverCubeGray74": "Vintage Grey"},
}
T = {  # lang -> (title, bullets, description) with {d} = design words
    "de": ("Mandala Life ART Würfel Ottoman Bezug 50x50x50 cm mit Innenbezug, {d} – Boho Hocker Bezug aus Baumwolle, Nur Bezug ohne Füllung",
           ["NUR BEZUG MIT INNENBEZUG – Geliefert wird der Außenbezug mit passendem Innenbezug (Liner), ohne Füllung. Einfach den Innenbezug füllen und in den Bezug stecken.",
            "WÜRFEL 50 x 50 x 50 CM – Großer quadratischer Ottoman / Hocker – ideal als Fußhocker, Sitzwürfel oder Beistelltisch.",
            "100% BAUMWOLLE – Bedruckter Baumwollstoff, {d}, mit Reißverschluss – weich, atmungsaktiv und pflegeleicht.",
            "VIELSEITIG – Passt ins Wohnzimmer, Schlafzimmer, Kinderzimmer oder auf die Terrasse und bringt Boho-Flair in jeden Raum.",
            "FÜLLTIPP – Mit Schaumstoffflocken, Bohnen, alten Kissen oder Textilresten befüllen. Handgefertigt in Indien – ein schönes Geschenk."],
           "Würfelförmiger Ottoman-Bezug 50 x 50 x 50 cm aus 100% Baumwolle, {d}, mit Innenbezug. Nur Bezug – Füllung nicht enthalten."),
    "fr": ("Mandala Life ART Housse de Pouf Ottoman Cube 50x50x50 cm avec Doublure Intérieure, {d} – Coton, Housse Seule sans Rembourrage",
           ["HOUSSE SEULE AVEC DOUBLURE – Livrée avec sa doublure intérieure, sans rembourrage. Remplissez la doublure puis glissez-la dans la housse.",
            "CUBE 50 x 50 x 50 CM – Grand ottoman / tabouret carré, idéal comme repose-pieds, assise d'appoint ou table basse.",
            "100% COTON – Tissu en coton imprimé, {d}, avec fermeture éclair – doux, respirant et facile d'entretien.",
            "POLYVALENT – Pour le salon, la chambre, la chambre d'enfant ou la terrasse ; apporte une touche bohème à chaque pièce.",
            "ASTUCE REMPLISSAGE – Mousse en flocons, billes, vieux coussins ou chutes de tissu. Fait main en Inde – une belle idée cadeau."],
           "Housse d'ottoman cube 50 x 50 x 50 cm en 100% coton, {d}, avec doublure intérieure. Housse seule – rembourrage non inclus."),
    "it": ("Mandala Life ART Fodera per Pouf Ottomano Cubo 50x50x50 cm con Fodera Interna, {d} – Cotone, Solo Fodera senza Imbottitura",
           ["SOLO FODERA CON FODERA INTERNA – Fornita con fodera interna, senza imbottitura. Riempi la fodera interna e inseriscila nella fodera esterna.",
            "CUBO 50 x 50 x 50 CM – Grande ottomano / sgabello quadrato, ideale come poggiapiedi, seduta o tavolino.",
            "100% COTONE – Tessuto di cotone stampato, {d}, con cerniera – morbido, traspirante e facile da curare.",
            "VERSATILE – Per soggiorno, camera da letto, cameretta o terrazza; porta un tocco boho in ogni ambiente.",
            "CONSIGLIO – Riempi con fiocchi di gommapiuma, palline, vecchi cuscini o ritagli di tessuto. Fatto a mano in India – un bel regalo."],
           "Fodera per ottomano cubo 50 x 50 x 50 cm in 100% cotone, {d}, con fodera interna. Solo fodera – imbottitura non inclusa."),
    "es": ("Mandala Life ART Funda de Puf Otomano Cubo 50x50x50 cm con Forro Interior, {d} – Algodón, Solo Funda sin Relleno",
           ["SOLO FUNDA CON FORRO INTERIOR – Se entrega con forro interior, sin relleno. Rellena el forro y colócalo dentro de la funda.",
            "CUBO 50 x 50 x 50 CM – Gran otomano / taburete cuadrado, ideal como reposapiés, asiento o mesa auxiliar.",
            "100% ALGODÓN – Tejido de algodón estampado, {d}, con cremallera – suave, transpirable y fácil de cuidar.",
            "VERSÁTIL – Para salón, dormitorio, habitación infantil o terraza; aporta un toque boho a cualquier espacio.",
            "CONSEJO DE RELLENO – Espuma triturada, bolitas, cojines viejos o retales de tela. Hecho a mano en India – un bonito regalo."],
           "Funda de otomano cubo 50 x 50 x 50 cm de 100% algodón, {d}, con forro interior. Solo funda – relleno no incluido."),
    "nl": ("Mandala Life ART Kubus Poef Ottoman Hoes 50x50x50 cm met Binnenhoes, {d} – Katoen, Alleen Hoes zonder Vulling",
           ["ALLEEN HOES MET BINNENHOES – Geleverd met binnenhoes (liner), zonder vulling. Vul de binnenhoes en stop hem in de hoes.",
            "KUBUS 50 x 50 x 50 CM – Grote vierkante ottoman / kruk, ideaal als voetenbank, zitplaats of bijzettafel.",
            "100% KATOEN – Bedrukte katoenen stof, {d}, met rits – zacht, ademend en makkelijk te onderhouden.",
            "VEELZIJDIG – Voor woonkamer, slaapkamer, kinderkamer of terras; brengt boho-sfeer in elke ruimte.",
            "VULTIP – Schuimvlokken, bolletjes, oude kussens of stofresten. Handgemaakt in India – een mooi cadeau."],
           "Kubusvormige ottoman-hoes 50 x 50 x 50 cm van 100% katoen, {d}, met binnenhoes. Alleen hoes – vulling niet inbegrepen."),
    "sv": ("Mandala Life ART Kubformad Puff Ottoman Överdrag 50x50x50 cm med Innerfodral, {d} – Bomull, Endast Överdrag utan Fyllning",
           ["ENDAST ÖVERDRAG MED INNERFODRAL – Levereras med innerfodral, utan fyllning. Fyll innerfodralet och lägg det i överdraget.",
            "KUB 50 x 50 x 50 CM – Stor fyrkantig ottoman / pall, perfekt som fotpall, sittplats eller sidobord.",
            "100% BOMULL – Tryckt bomullstyg, {d}, med dragkedja – mjukt, andas och lättskött.",
            "MÅNGSIDIG – För vardagsrum, sovrum, barnrum eller terrass; ger boho-känsla i varje rum.",
            "FYLLNINGSTIPS – Skumflingor, kulor, gamla kuddar eller tygrester. Handgjord i Indien – en fin present."],
           "Kubformat ottoman-överdrag 50 x 50 x 50 cm i 100% bomull, {d}, med innerfodral. Endast överdrag – fyllning ingår inte."),
    "pl": ("Mandala Life ART Pokrowiec na Pufę Otomanę Kostkę 50x50x50 cm z Wkładem Wewnętrznym, {d} – Bawełna, Sam Pokrowiec bez Wypełnienia",
           ["SAM POKROWIEC Z WKŁADEM – Dostarczany z wewnętrznym pokrowcem (linerem), bez wypełnienia. Wypełnij wkład i włóż go do pokrowca.",
            "KOSTKA 50 x 50 x 50 CM – Duża kwadratowa otomana / pufa, idealna jako podnóżek, siedzisko lub stolik.",
            "100% BAWEŁNA – Drukowana tkanina bawełniana, {d}, z zamkiem – miękka, przewiewna i łatwa w pielęgnacji.",
            "WSZECHSTRONNA – Do salonu, sypialni, pokoju dziecięcego lub na taras; dodaje klimatu boho każdemu wnętrzu.",
            "WSKAZÓWKA – Wypełnij płatkami pianki, kulkami, starymi poduszkami lub skrawkami tkanin. Ręcznie robiona w Indiach – piękny prezent."],
           "Pokrowiec na otomanę w kształcie kostki 50 x 50 x 50 cm ze 100% bawełny, {d}, z wkładem wewnętrznym. Sam pokrowiec – wypełnienie nie jest dołączone."),
    "en": ("Mandala Life ART Cube Ottoman Cover 50x50x50 cm with Inner Liner, {d} – Cotton Boho Footstool Cover, Cover Only, No Filling",
           ["COVER ONLY WITH INNER LINER – Comes with a matching inner liner, without filling. Fill the liner and slip it into the cover.",
            "CUBE 50 x 50 x 50 CM (20 IN) – Large square ottoman / footstool – ideal as a footrest, extra seat or side table.",
            "100% COTTON – Printed cotton fabric, {d}, with zip – soft, breathable and easy care.",
            "VERSATILE – For the living room, bedroom, kids' room or patio; brings a boho touch to any space.",
            "FILLING TIP – Use shredded foam, beans, old pillows or fabric scraps. Handmade in India – a lovely gift."],
           "Cube-shaped ottoman cover 50 x 50 x 50 cm in 100% cotton, {d}, with inner liner. Cover only – filling not included."),
}


def main():
    cred, _ = creds("DE")
    kw = {} if "--apply" in sys.argv else {"mode": "VALIDATION_PREVIEW"}
    only = [a for a in sys.argv[1:] if not a.startswith("--")] or list(STORES)
    for mp in only:
        lang = STORES[mp]
        short = lang[:2]
        m = getattr(Marketplaces, mp)
        mid = m.marketplace_id
        li = ListingsItems(credentials=cred, marketplace=m, timeout=120)
        cur_code, price = PRICE.get(mp, ("EUR", 59.99))
        for sku in SKUS:
            try:
                item = li.get_listings_item(S, sku, marketplaceIds=[mid], includedData=["attributes", "summaries"]).payload
            except Exception:
                print(mp, sku, "no listing")
                continue
            cur = item["attributes"]
            d = DESIGN[short][sku]
            title, bullets, desc = T[short]
            title = title.format(d=d)
            v = lambda val: [{"value": val, "language_tag": lang, "marketplace_id": mid}]
            patches = []
            for k in ("item_name", "bullet_point", "product_description"):
                other = [x for x in cur.get(k, []) if isinstance(x, dict) and x.get("language_tag") not in (None, lang)]
                if other:
                    patches.append({"op": "delete", "path": f"/attributes/{k}", "value": other})
            patches += [{"op": "replace", "path": "/attributes/item_name", "value": v(title)},
                        {"op": "replace", "path": "/attributes/bullet_point",
                         "value": [{"value": b.format(d=d), "language_tag": lang, "marketplace_id": mid} for b in bullets]},
                        {"op": "replace", "path": "/attributes/product_description", "value": v(desc.format(d=d))},
                        {"op": "replace", "path": "/attributes/purchasable_offer", "value": [
                            {"marketplace_id": mid, "currency": cur_code, "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": price}]}]}]}]
            ptype = (item.get("summaries") or [{}])[0].get("productType") or "OTTOMAN"
            r = li.patch_listings_item(S, sku, marketplaceIds=[mid], body={"productType": ptype, "patches": patches},
                                       includedData=["issues"], **kw).payload
            print(mp, sku, len(title), price, r.get("status"),
                  json.dumps([(i["code"], i["message"][:150]) for i in r.get("issues", []) if i["severity"] == "ERROR"], ensure_ascii=False))
            time.sleep(0.3)


if __name__ == "__main__":
    main()
