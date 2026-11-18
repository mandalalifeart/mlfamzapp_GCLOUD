"""Low-budget SP keyword campaigns "<CC> SP SACRED GEOMETRY" for the PAREO_SOL
Flower of Life / Seed of Life pareos, per the user 2026-11-18 ("create a low
budget campaign in all mp with sacred geometry/flower of life/seed of life
keywords, both for beach and as wall hanging"). One campaign per store with
SOL stock: US, DE, FR, IT, ES, NL (UK skipped - SOL not buyable there).
Two ad groups: BEACH (sarong/pareo/cover-up words) and WALL (tapestry / wall
hanging words), phrase match, all 6 SOL SKUs in each. Keywords: real buyer
searches from our ad search-term data where they exist (DE "blume des lebens
tuch/sarong/wandbehang", US "sacred geometry sarong", FR "pareo fleur de vie",
IT "arazzo fiore della vita") + the same patterns in each language.
Pareo portfolio, dynamic bids down only. Default preview, --apply creates."""
import json
import sys
from datetime import date

from AsinAnalytics import pb_authenticate
from AdsBidWriter import record_bid_change
from AdsGrowth import Api, _media

APPLY = "--apply" in sys.argv
SKUS = ["PAREO_SOL_536", "PAREO_SOL_537", "PAREO_SOL_538", "PAREO_SOL_551", "PAREO_SOL_552", "PAREO_SOL_553"]
EN_BEACH = ["flower of life sarong", "sacred geometry sarong", "flower of life pareo", "seed of life sarong",
            "sacred geometry beach cover up", "flower of life beach", "sacred geometry pareo", "flower of life cover up"]
EN_WALL = ["flower of life tapestry", "sacred geometry tapestry", "seed of life tapestry", "flower of life wall hanging",
           "sacred geometry wall hanging", "flower of life wall decor", "sacred geometry wall decor", "flower of life wall art"]
SPEC = {  # cc: (profile, portfolio, budget, bid, beach keywords, wall keywords)
    "US": ("1649312585287580", "50831881940379", 5.0, 0.35, EN_BEACH, EN_WALL),
    "DE": ("1262925395560957", "135627869766289", 3.0, 0.30,
           ["blume des lebens tuch", "blume des lebens sarong", "blume des lebens pareo", "lebensblume sarong",
            "lebensblume tuch", "blume des lebens strandtuch", "heilige geometrie tuch", "saat des lebens tuch"],
           ["blume des lebens wandbehang", "blume des lebens wandtuch", "wandbehang blume des lebens", "blume des lebens wandteppich",
            "heilige geometrie wandbehang", "lebensblume wandbehang", "blume des lebens deko", "saat des lebens wandbehang"]),
    "FR": ("1167111969385560", "89048727013662", 3.0, 0.30,
           ["pareo fleur de vie", "fleur de vie paréo", "fleur de vie sarong", "fleur de vie foulard", "géométrie sacrée paréo",
            "fleur de vie tissu", "graine de vie paréo"],
           ["fleur de vie tenture", "tenture fleur de vie", "géométrie sacrée tenture", "fleur de vie tenture murale",
            "fleur de vie décoration murale", "graine de vie tenture"]),
    "IT": ("2967253577783675", "2931779830380", 3.0, 0.30,
           ["fiore della vita pareo", "pareo fiore della vita", "fiore della vita telo mare", "geometria sacra pareo",
            "fiore della vita foulard", "seme della vita pareo"],
           ["arazzo fiore della vita", "fiore della vita arazzo", "geometria sacra arazzo", "fiore della vita decorazione parete",
            "fiore della vita telo", "seme della vita arazzo"]),
    "ES": ("548812940689391", "240967298599921", 3.0, 0.30,
           ["flor de la vida pareo", "pareo flor de la vida", "flor de la vida pañuelo", "geometría sagrada pareo",
            "flor de la vida toalla playa", "semilla de la vida pareo"],
           ["flor de la vida tapiz", "tapiz flor de la vida", "geometría sagrada tapiz", "flor de la vida decoración pared",
            "flor de la vida tela", "semilla de la vida tapiz"]),
    "NL": ("964809683265675", "108471786507495", 3.0, 0.30,
           ["levensbloem sarong", "bloem des levens sarong", "flower of life sarong", "levensbloem pareo", "bloem des levens doek"],
           ["levensbloem wandkleed", "bloem des levens wandkleed", "flower of life wandkleed", "heilige geometrie wandkleed",
            "levensbloem wanddecoratie"]),
}
NEGATIVES = ["necklace", "pendant", "ring", "earring", "sticker", "stencil", "coin", "felt"]  # jewellery / craft, EN only


def main():
    pb = pb_authenticate()
    api = Api(pb)
    only = [a for a in sys.argv[1:] if not a.startswith("--")] or list(SPEC)
    for cc in only:
        prof, port, budget, bid, beach, wall = SPEC[cc]
        name = f"{cc} SP SACRED GEOMETRY"
        print(cc, name, f"budget {budget}/day, bid {bid}", f"BEACH {len(beach)} kw, WALL {len(wall)} kw")
        if not APPLY:
            print("   beach:", beach, "\n   wall:", wall)
            continue
        res = api.call(prof, "POST", "/sp/campaigns", {"campaigns": [{
            "name": name, "targetingType": "MANUAL", "state": "ENABLED", "startDate": date.today().isoformat(),
            "portfolioId": port, "budget": {"budgetType": "DAILY", "budget": budget},
            "dynamicBidding": {"strategy": "LEGACY_FOR_SALES"}}]}, _media("spCampaign"))
        ok = (res.get("campaigns") or {}).get("success") or []
        if not ok:
            print("   campaign error", json.dumps(res)[:400])
            continue
        cid = ok[0]["campaignId"]
        for agname, kws in (("BEACH", beach), ("WALL", wall)):
            r = api.call(prof, "POST", "/sp/adGroups", {"adGroups": [{"campaignId": cid, "name": agname, "state": "ENABLED",
                                                                     "defaultBid": bid}]}, _media("spAdGroup"))
            ag = r["adGroups"]["success"][0]["adGroupId"]
            r = api.call(prof, "POST", "/sp/productAds", {"productAds": [{"campaignId": cid, "adGroupId": ag, "sku": s,
                                                                         "state": "ENABLED"} for s in SKUS]}, _media("spProductAd"))
            na = len(r["productAds"].get("success") or [])
            r = api.call(prof, "POST", "/sp/keywords", {"keywords": [{"campaignId": cid, "adGroupId": ag, "keywordText": k,
                                                                     "matchType": "PHRASE", "state": "ENABLED", "bid": bid}
                                                                    for k in kws]}, _media("spKeyword"))
            nk = len(r["keywords"].get("success") or [])
            errs = r["keywords"].get("error") or []
            print(f"   {agname}: ad group {ag}, {na} ads, {nk} keywords", ("errors " + json.dumps(errs)[:300]) if errs else "")
        if cc in ("US",):
            r = api.call(prof, "POST", "/sp/campaignNegativeKeywords", {"campaignNegativeKeywords": [
                {"campaignId": cid, "keywordText": n, "matchType": "NEGATIVE_PHRASE", "state": "ENABLED"} for n in NEGATIVES]},
                _media("spCampaignNegativeKeyword"))
            print("   negatives", len((r.get("campaignNegativeKeywords") or {}).get("success") or []))
        record_bid_change(pb, {"target_id": str(cid), "campaign_id": str(cid), "campaign_name": name, "country_code": cc,
                               "profile_id": prof, "ad_product": "SPONSORED_PRODUCTS", "action": "new_campaign",
                               "changed_at": date.today().isoformat() + "T00:00:00Z", "status": "applied", "new_bid": bid,
                               "reason": "Sacred geometry / Flower of Life campaign for PAREO_SOL (user 2026-11-18)"})
        print("   campaign", cid)


if __name__ == "__main__":
    main()
