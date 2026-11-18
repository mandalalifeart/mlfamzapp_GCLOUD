"""GENERIC ad group in each "<CC> SP SACRED GEOMETRY" campaign, per the user
2026-11-18 ("maybe we should also add generic sacred geometry keywords?").
Bare generic terms (flower of life / sacred geometry / seed of life in each
language) as EXACT match - phrase would also catch necklaces, books, stencils -
at a lower bid than the specific BEACH/WALL groups. All 6 PAREO_SOL as ads.
Default preview, --apply creates."""
import json
import sys

from AsinAnalytics import pb_authenticate
from AdsGrowth import Api, _media

APPLY = "--apply" in sys.argv
SKUS = ["PAREO_SOL_536", "PAREO_SOL_537", "PAREO_SOL_538", "PAREO_SOL_551", "PAREO_SOL_552", "PAREO_SOL_553"]
SPEC = {  # cc: (profile, campaign, bid, keywords)
    "US": ("1649312585287580", "40751050266311", 0.25, ["flower of life", "sacred geometry", "seed of life", "sacred geometry decor",
                                                        "flower of life decor", "sacred geometry art", "flower of life art"]),
    "DE": ("1262925395560957", "50660004836416", 0.20, ["blume des lebens", "heilige geometrie", "lebensblume", "saat des lebens",
                                                        "flower of life", "blume des lebens deko"]),
    "FR": ("1167111969385560", "13163843926591", 0.20, ["fleur de vie", "géométrie sacrée", "graine de vie", "fleur de vie déco"]),
    "IT": ("2967253577783675", "129312192427492", 0.20, ["fiore della vita", "geometria sacra", "seme della vita"]),
    "ES": ("548812940689391", "47372813187271", 0.20, ["flor de la vida", "geometría sagrada", "semilla de la vida"]),
    "NL": ("964809683265675", "103848502057931", 0.20, ["levensbloem", "bloem des levens", "heilige geometrie", "flower of life"]),
}

api = Api(pb_authenticate())
for cc, (prof, cid, bid, kws) in SPEC.items():
    print(cc, cid, bid, kws)
    if not APPLY:
        continue
    ag = api.call(prof, "POST", "/sp/adGroups", {"adGroups": [{"campaignId": cid, "name": "GENERIC", "state": "ENABLED",
                                                              "defaultBid": bid}]}, _media("spAdGroup"))["adGroups"]["success"][0]["adGroupId"]
    r = api.call(prof, "POST", "/sp/productAds", {"productAds": [{"campaignId": cid, "adGroupId": ag, "sku": s, "state": "ENABLED"}
                                                                 for s in SKUS]}, _media("spProductAd"))
    k = api.call(prof, "POST", "/sp/keywords", {"keywords": [{"campaignId": cid, "adGroupId": ag, "keywordText": t, "matchType": "EXACT",
                                                              "state": "ENABLED", "bid": bid} for t in kws]}, _media("spKeyword"))
    print("   ad group", ag, "ads", len(r["productAds"].get("success") or []), "keywords", len(k["keywords"].get("success") or []),
          json.dumps(k["keywords"].get("error") or [])[:300])
