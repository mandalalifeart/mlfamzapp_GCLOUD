"""PAREO_SOL (Flower of Life cotton pareos) bullets in FR / IT / ES, per the user
2026-11-18 ("yes" to adding the wall-hanging use). US/DE already have the
sacred-geometry + tapestry/wall-hanging bullets; FR/IT/ES had generic pareo
bullets that call the fabric rayon (SOL is cotton) and no wall-hanging use.
Replaced with translations of the US/DE bullets. BE takes the FR text (fr_BE).
Default VALIDATION_PREVIEW, --apply submits."""
import json
import os
import sys
import time

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

SKUS = ["PAREO_SOL_536", "PAREO_SOL_537", "PAREO_SOL_538", "PAREO_SOL_551", "PAREO_SOL_552", "PAREO_SOL_553"]
FR = ["MOTIF GÉOMÉTRIE SACRÉE – Un magnifique motif Fleur de Vie / Graine de Vie inspiré de la géométrie sacrée. Un look mandala "
      "bohème unique, parfait comme paréo de plage, sarong ou cache-maillot.",
      "PARFAIT EN TENTURE MURALE – Apporte une touche bohème et spirituelle à toute pièce : chambre, salle de yoga, coin méditation "
      "ou décoration murale au motif géométrique chargé de sens.",
      "COTON DOUX ET RESPIRANT – Imprimé à la main sur un coton léger et agréable, notre paréo allie confort et élégance au quotidien.",
      "PERSONNALISEZ VOTRE LOOK – Robe, jupe, cache-maillot ou foulard : quelques nœuds suffisent pour passer de la plage au restaurant.",
      "UNE IDÉE CADEAU – Un paréo imprimé à la main est un cadeau original et utile pour les amoureux de la plage, du yoga et du style bohème."]
IT = ["DISEGNO GEOMETRIA SACRA – Un bellissimo motivo Fiore della Vita / Seme della Vita ispirato alla geometria sacra. Un look mandala "
      "boho unico, perfetto come pareo da spiaggia, sarong o copricostume.",
      "PERFETTO COME ARAZZO DA PARETE – Dona un tocco boho e spirituale a ogni ambiente: camera da letto, sala yoga, angolo meditazione "
      "o decorazione da parete con un motivo geometrico ricco di significato.",
      "COTONE MORBIDO E TRASPIRANTE – Stampato a mano su cotone leggero e piacevole, il nostro pareo unisce comfort ed eleganza.",
      "PERSONALIZZA IL TUO LOOK – Abito, gonna, copricostume o foulard: bastano pochi nodi per passare dalla spiaggia al ristorante.",
      "UN'IDEA REGALO – Un pareo stampato a mano è un regalo originale e utile per chi ama il mare, lo yoga e lo stile boho."]
ES = ["DISEÑO DE GEOMETRÍA SAGRADA – Un precioso motivo Flor de la Vida / Semilla de la Vida inspirado en la geometría sagrada. Un look "
      "mandala boho único, perfecto como pareo de playa, sarong o cubre bikini.",
      "PERFECTO COMO TAPIZ DE PARED – Aporta un toque boho y espiritual a cualquier espacio: dormitorio, sala de yoga, rincón de "
      "meditación o decoración de pared con un diseño geométrico lleno de significado.",
      "ALGODÓN SUAVE Y TRANSPIRABLE – Estampado a mano en algodón ligero y agradable, nuestro pareo une comodidad y elegancia.",
      "PERSONALIZA TU LOOK – Vestido, falda, cubre bikini o pañuelo: con unos nudos pasas de la playa al restaurante.",
      "UNA IDEA DE REGALO – Un pareo estampado a mano es un regalo original y útil para amantes de la playa, el yoga y el estilo boho."]
STORES = {"FR": ("fr_FR", FR), "BE": ("fr_BE", FR), "IT": ("it_IT", IT), "ES": ("es_ES", ES)}


def main():
    cred, _ = creds("DE")
    kw = {} if "--apply" in sys.argv else {"mode": "VALIDATION_PREVIEW"}
    for mp, (lang, bullets) in STORES.items():
        m = getattr(Marketplaces, mp)
        mid = m.marketplace_id
        li = ListingsItems(credentials=cred, marketplace=m, timeout=120)
        for sku in SKUS:
            try:
                item = li.get_listings_item(os.environ["AMAZON_SELLER_ID_EU"], sku, marketplaceIds=[mid],
                                            includedData=["summaries"]).payload
            except Exception:
                print(mp, sku, "no listing")
                continue
            ptype = item["summaries"][0]["productType"] if item.get("summaries") else "SWIMWEAR"
            patch = [{"op": "replace", "path": "/attributes/bullet_point",
                      "value": [{"value": b, "language_tag": lang, "marketplace_id": mid} for b in bullets]}]
            r = li.patch_listings_item(os.environ["AMAZON_SELLER_ID_EU"], sku, marketplaceIds=[mid],
                                       body={"productType": ptype, "patches": patch}, includedData=["issues"], **kw).payload
            print(mp, sku, r.get("status"), json.dumps([(i["code"], i["message"][:120]) for i in r.get("issues", [])
                                                         if i["severity"] == "ERROR"], ensure_ascii=False))
            time.sleep(0.3)


if __name__ == "__main__":
    main()
