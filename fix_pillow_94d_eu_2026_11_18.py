"""Re-activate CoverPillowRoundYellow94D (round 60 cm floor cushion cover with
fringe, "94" turquoise/pink Persian print, ASIN B0933HJFF9) in DE + FR, per the
user 2026-11-18 ("create listings in eu for 94d ... the listing already existed
with pictures and text, 60 flat round with fringe, no uk").
The old listing items exist but are blocked: brand/manufacturer carry an it_IT
language tag (error 100720) and the de_DE/fr_FR texts are Italian -> 13013
"product not in catalog". This replaces those attributes with correct-language
values (German text adapted from CoverPillowPlue56A). Price = the other round
60 cm pillow covers on DE (39.99, + B2B tiers); FR price comes from BIL.
Default = VALIDATION_PREVIEW, --apply submits."""
import json
import os
import sys

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

APPLY = "--apply" in sys.argv
SKU = "CoverPillowRoundYellow94D"
SELLER = os.environ["AMAZON_SELLER_ID_EU"]

DE_TITLE = ("Mandala Life ART Runder Bodenkissenbezug 60 cm mit Fransen, Türkis-Rosa Orient-Muster – Nur Bezug, ohne Füllung, "
            "Böhmischer Baumwoll-Kissenbezug für Meditation, Yoga und Boho-Sitzecke")
DE_BULLETS = [
    "RUND 60 CM MIT FRANSEN – Flacher runder Bodenkissenbezug (ca. 60 cm Durchmesser) mit verspielten türkisfarbenen Fransen "
    "rundherum und orientalischem Medaillon-Muster in Türkis, Rosa und Creme.",
    "NUR BEZUG – Wird ohne Füllung geliefert. Mit Reißverschluss; füllen Sie ihn mit einem runden Kissen-Inlett, "
    "Sitzsack- oder Hobbyfüllung.",
    "100% BAUMWOLLE – Weicher, atmungsaktiver Baumwollstoff, strapazierfähig und pflegeleicht für den täglichen Gebrauch.",
    "AUTHENTISCHE HANDWERKERQUALITÄT – Von Handwerkern in Rajasthan, Indien, handgefertigt – mit einer Detailgenauigkeit, "
    "die Massenware nicht erreicht.",
    "BOHO CHIC DEKO – Perfekt als Bodenkissen, Meditationskissen oder Sitzkissen im Wohnzimmer, Schlafzimmer, Yoga-Raum "
    "oder in der Kinderspielecke. Ein schönes Geschenk zum Einzug.",
]
DE_DESC = ("Verleihen Sie Ihrem Zuhause böhmischen Charme mit diesem runden Bodenkissenbezug mit Fransen. Aus 100% Baumwolle "
           "gefertigt, mit orientalischem Medaillon-Muster in Türkis und Rosa und einer türkisfarbenen Fransenkante – perfekt als "
           "Bodenkissen, Meditationskissen oder dekoratives Akzentstück. Lieferung ohne Füllung.")
FR_TITLE = ("Mandala Life ART Housse de coussin de sol ronde 60 cm à franges, motif oriental turquoise et rose – Housse seule, "
            "sans rembourrage, coussin bohème en coton pour méditation, yoga et coin salon")

FR_BULLETS = [
    "RONDE 60 CM À FRANGES – Housse de coussin de sol ronde et plate (env. 60 cm de diamètre) bordée de franges turquoise, "
    "motif médaillon oriental turquoise, rose et crème.",
    "HOUSSE SEULE – Livrée sans rembourrage, avec fermeture éclair ; à garnir d'un coussin rond, de billes ou de rembourrage.",
    "100% COTON – Tissu doux et respirant, résistant et facile d'entretien pour un usage quotidien.",
    "ARTISANAT AUTHENTIQUE – Fabriquée à la main par des artisans du Rajasthan, en Inde.",
    "DÉCO BOHÈME – Idéale comme coussin de sol, coussin de méditation ou pouf d'appoint au salon, dans la chambre, l'espace "
    "yoga ou la chambre d'enfant. Une belle idée cadeau.",
]


def build(mp, lang):
    mid = getattr(Marketplaces, mp).marketplace_id
    v = lambda val: [{"value": val, "language_tag": lang, "marketplace_id": mid}]
    patches = [
        {"op": "replace", "path": "/attributes/brand", "value": v("Mandala Life ART")},
        {"op": "replace", "path": "/attributes/manufacturer", "value": v("Mandala Life ART")},
    ]
    if mp == "DE":
        patches += [
            {"op": "replace", "path": "/attributes/item_name", "value": v(DE_TITLE)},
            {"op": "replace", "path": "/attributes/bullet_point", "value": [{"value": b, "language_tag": lang, "marketplace_id": mid} for b in DE_BULLETS]},
            {"op": "replace", "path": "/attributes/product_description", "value": v(DE_DESC)},
            {"op": "replace", "path": "/attributes/color", "value": v("Türkis mit Aufdruck")},
            {"op": "replace", "path": "/attributes/material", "value": v("Baumwolle")},
            {"op": "replace", "path": "/attributes/fabric_type", "value": v("100% Baumwolle")},
            {"op": "replace", "path": "/attributes/list_price", "value": [{"value_with_tax": 39.99, "currency": "EUR", "marketplace_id": mid}]},
            {"op": "replace", "path": "/attributes/purchasable_offer", "value": [
                {"currency": "EUR", "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": 39.99}]}], "marketplace_id": mid},
                {"currency": "EUR", "audience": "B2B", "our_price": [{"schedule": [{"value_with_tax": 39.99}]}],
                 "quantity_discount_plan": [{"schedule": [{"discount_type": "percent", "levels": [
                     {"lower_bound": 3, "value": 10.0}, {"lower_bound": 5, "value": 12.0}, {"lower_bound": 10, "value": 15.0}]}]}],
                 "marketplace_id": mid}]},
        ]
    else:
        patches += [{"op": "replace", "path": "/attributes/item_name", "value": v(FR_TITLE)},
                    {"op": "replace", "path": "/attributes/color", "value": v("Turquoise avec Imprimé")},
                    {"op": "replace", "path": "/attributes/bullet_point", "value": [{"value": b, "language_tag": lang, "marketplace_id": mid} for b in FR_BULLETS]}]
    return mid, patches


def main():
    cred, _ = creds("DE")
    for mp, lang in (("DE", "de_DE"), ("FR", "fr_FR")):
        li = ListingsItems(credentials=cred, marketplace=getattr(Marketplaces, mp), timeout=120)
        mid, patches = build(mp, lang)
        kw = {} if APPLY else {"mode": "VALIDATION_PREVIEW"}
        r = li.patch_listings_item(SELLER, SKU, marketplaceIds=[mid], body={"productType": "DECORATIVE_PILLOW_COVER", "patches": patches},
                                   includedData=["issues"], **kw).payload
        print(mp, r.get("status"), json.dumps([(i["code"], i["severity"], i["message"][:160]) for i in r.get("issues", [])], ensure_ascii=False))


if __name__ == "__main__":
    main()
