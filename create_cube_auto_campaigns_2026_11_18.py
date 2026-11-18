"""Small SP AUTO campaigns for the 3 cube (stool) covers in DE / FR / IT, per the
user 2026-11-18 ("create a small auto campaign with low bid in de/fr/it").
CoverCubeTribalWB60A, CoverCubeTribalBW59A, CoverCubeGray74 - all sell slowly
in the EU (10 units Aug-Nov). €3/day, default bid €0.20, home-decor (POUF)
portfolio, dynamic bids down only. Default preview, --apply creates."""
import json
import sys
from datetime import date

from AsinAnalytics import pb_authenticate
from AdsBidWriter import record_bid_change
from AdsGrowth import Api, _media

APPLY = "--apply" in sys.argv
SKUS = ["CoverCubeTribalWB60A", "CoverCubeTribalBW59A", "CoverCubeGray74"]
SPEC = {"DE": ("1262925395560957", "262687214324301"), "FR": ("1167111969385560", "162607980740874"),
        "IT": ("2967253577783675", "253823631302372")}
BUDGET, BID = 3.0, 0.20

pb = pb_authenticate()
api = Api(pb)
for cc, (prof, port) in SPEC.items():
    name = f"{cc} CUBE COVER AUTO"
    print(cc, name, BUDGET, BID, SKUS)
    if not APPLY:
        continue
    res = api.call(prof, "POST", "/sp/campaigns", {"campaigns": [{
        "name": name, "targetingType": "AUTO", "state": "ENABLED", "startDate": date.today().isoformat(), "portfolioId": port,
        "budget": {"budgetType": "DAILY", "budget": BUDGET}, "dynamicBidding": {"strategy": "LEGACY_FOR_SALES"}}]}, _media("spCampaign"))
    ok = (res.get("campaigns") or {}).get("success") or []
    if not ok:
        print("   error", json.dumps(res)[:300])
        continue
    cid = ok[0]["campaignId"]
    ag = api.call(prof, "POST", "/sp/adGroups", {"adGroups": [{"campaignId": cid, "name": "CUBE", "state": "ENABLED", "defaultBid": BID}]},
                  _media("spAdGroup"))["adGroups"]["success"][0]["adGroupId"]
    r = api.call(prof, "POST", "/sp/productAds", {"productAds": [{"campaignId": cid, "adGroupId": ag, "sku": s, "state": "ENABLED"} for s in SKUS]},
                 _media("spProductAd"))
    print("   campaign", cid, "ad group", ag, "ads", len(r["productAds"].get("success") or []), json.dumps(r["productAds"].get("error") or [])[:200])
    record_bid_change(pb, {"target_id": str(cid), "campaign_id": str(cid), "campaign_name": name, "country_code": cc, "profile_id": prof,
                           "ad_product": "SPONSORED_PRODUCTS", "action": "new_campaign", "new_bid": BID, "status": "applied",
                           "changed_at": date.today().isoformat() + "T00:00:00Z", "reason": "Cube cover AUTO campaign (user 2026-11-18)"})
