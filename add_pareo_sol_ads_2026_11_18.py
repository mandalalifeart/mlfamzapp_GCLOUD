"""Add the 6 PAREO_SOL pareos as SP product ads to existing pareo campaigns in
US/DE/FR/IT/ES and re-enable PAREO_SOL_551 in DE PAREO AUTO, per the user
2026-11-18 ("ok, enable 551 as well, add to usa, de, fr, it, es"; existing
campaigns, no bid changes). Each product goes into every ENABLED ad group of the
listed campaigns; existing ads are left alone (paused ones are only re-enabled
for 551 in DE). Logged in ads_bid_change_log.
Default preview, --apply writes."""
import sys
from datetime import datetime, timezone

from AsinAnalytics import pb_authenticate
from AdsBidWriter import record_bid_change
from AdsGrowth import Api, _media

APPLY = "--apply" in sys.argv
SKUS = ["PAREO_SOL_536", "PAREO_SOL_537", "PAREO_SOL_538", "PAREO_SOL_551", "PAREO_SOL_552", "PAREO_SOL_553"]
CAMPAIGNS = {  # country: (profile, [(campaign_id, name)])
    "US": ("1649312585287580", [("254727612333677", "PAREO SP AUTO"), ("71114848140594", "PAREO SP EXACT")]),
    "DE": ("1262925395560957", [("424008217270471", "DE PAREO AUTO"), ("62690509900207", "PAREO SP EXACT")]),
    "FR": ("1167111969385560", [("11766355879387", "PAREO SP EXACT"), ("69967469355752", "PAREO SP BROAD")]),
    "IT": ("2967253577783675", [("294296571098131", "IT SP PAREO AUTO"), ("197679621602724", "IT SP PAREO BROAD")]),
    "ES": ("548812940689391", [("127639647461163", "ES PAREO PHRASE")]),
}
# ad group per campaign (SOL = cotton "Flower of Life" pareos): EXACT campaigns are split by theme -> the
# cotton group only; single-group campaigns -> that group; US AUTO -> the general Sarong-Auto group.
AG_NAME = {"254727612333677": "Sarong-Auto", "71114848140594": "Cotton", "62690509900207": "COTTON", "11766355879387": "COTON"}
REENABLE = {("DE", "PAREO_SOL_551")}
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
REASON = "PAREO_SOL ads 2026-11-18 (user: add to us/de/fr/it/es, enable 551)"


def main():
    pb = pb_authenticate()
    api = Api(pb)
    for cc, (prof, camps) in CAMPAIGNS.items():
        for cid, name in camps:
            ags = [a for a in api.call(prof, "POST", "/sp/adGroups/list", {"campaignIdFilter": {"include": [cid]},
                                                                            "stateFilter": {"include": ["ENABLED"]}},
                                       _media("spAdGroup")).get("adGroups", [])
                   if cid not in AG_NAME or a["name"] == AG_NAME[cid]]
            ads = api.call(prof, "POST", "/sp/productAds/list", {"campaignIdFilter": {"include": [cid]}, "maxResults": 1000},
                           _media("spProductAd")).get("productAds", [])
            have = {(str(a["adGroupId"]), a.get("sku")): a for a in ads}
            new, enable = [], []
            for ag in ags:
                for sku in SKUS:
                    a = have.get((str(ag["adGroupId"]), sku))
                    if not a:
                        new.append({"campaignId": cid, "adGroupId": ag["adGroupId"], "sku": sku, "state": "ENABLED"})
                    elif a["state"] == "PAUSED" and (cc, sku) in REENABLE:
                        enable.append({"adId": a["adId"], "state": "ENABLED"})
            print(cc, name, f"{len(ags)} ad group(s)", "new:", [n["sku"][-3:] for n in new], "re-enable:", len(enable))
            if not APPLY:
                continue
            if new:
                r = api.call(prof, "POST", "/sp/productAds", {"productAds": new}, _media("spProductAd"))
                ok = (r.get("productAds") or {}).get("success", [])
                err = (r.get("productAds") or {}).get("error", [])
                print("   created", len(ok), "errors", [str(e)[:160] for e in err])
                for s in ok:
                    pa = s.get("productAd") or {}
                    record_bid_change(pb, {"target_id": str(pa.get("adId") or s.get("adId")), "campaign_id": cid,
                                           "reason": f"{REASON}: new product ad {pa.get('sku', '')} in {name}", "status": "applied",
                                           "changed_at": NOW, "country_code": cc, "profile_id": prof, "ad_product": "SPONSORED_PRODUCTS", "campaign_name": name, "action": "product_ad", "amazon_response": str(s)[:500]})
            if enable:
                r = api.call(prof, "PUT", "/sp/productAds", {"productAds": enable}, _media("spProductAd"))
                print("   re-enabled", r)
                for e in enable:
                    record_bid_change(pb, {"target_id": str(e["adId"]), "campaign_id": cid,
                                           "reason": f"{REASON}: re-enabled PAREO_SOL_551 in {name}", "status": "applied",
                                           "changed_at": NOW, "country_code": cc, "profile_id": prof, "ad_product": "SPONSORED_PRODUCTS", "campaign_name": name, "action": "product_ad", "amazon_response": str(r)[:500]})


if __name__ == "__main__":
    main()
