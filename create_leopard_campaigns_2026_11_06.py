"""One-off: leopard pouf cover test campaigns (CoverPoufVLeo540 "Jungle Fusion"
tiger+leopard, CoverPoufVLeo541 "Savanna Leo"), approved by the user on
Telegram 2026-11-06 ("ok"). Both had ~1,100 visits in Oct (all from the velvet
family page) and 0 sales, no ads. Exact keywords from real buyer searches
(Brand Analytics SQP for these ASINs + our search-term data), 30-day test,
stop rule: $25 / EUR 25 spend with 0 orders -> pause. Watched in change_watch.
Dry run unless --apply.  Usage: create_leopard_campaigns_2026_11_06.py us|de [--apply]"""
import json
import sys
from datetime import date

import requests

from AdsBidWriter import find_profile_connection, pb_authenticate, refresh_access_token

SPECS = {
    "us": {"profile": "1649312585287580", "portfolio": "252083709299975", "name": "USAC LEOPARD EXACT",
           "budget": 4.0, "bid": 0.35,
           "keywords": ["animal print ottoman", "leopard ottoman", "leopard print ottoman", "leopard pouf",
                        "leopard print ottoman cover round", "round ottoman cover leopard", "cheetah print ottoman",
                        "tiger print ottoman", "leopard print pouf"]},
    "de": {"profile": "1262925395560957", "portfolio": "262687214324301", "name": "DE LEOPARD EXACT",
           "budget": 3.0, "bid": 0.30,
           "keywords": ["pouf leopardenmuster", "ottoman leopard", "leoparden pouf", "leopardenmuster pouf",
                        "pouf zebramuster", "tiger pouf", "pouf leopard"]},
}
SKUS = ["CoverPoufVLeo540", "CoverPoufVLeo541"]
region = sys.argv[1]
S = SPECS[region]
APPLY = "--apply" in sys.argv

pb = pb_authenticate()
conn = find_profile_connection(pb, S["profile"])
tok = refresh_access_token(conn["profile_key"], conn["refresh_token"])


def call(path, media, body):
    ct = f"application/vnd.{media}.v3+json"
    r = requests.post(conn["base_url"] + path, json=body, timeout=60, headers={
        "Authorization": f"Bearer {tok}", "Amazon-Advertising-API-ClientId": conn["client_id"],
        "Amazon-Advertising-API-Scope": S["profile"], "Content-Type": ct, "Accept": ct})
    if r.status_code >= 400:
        raise RuntimeError(f"{path}: HTTP {r.status_code} {r.text[:500]}")
    return r.json()


def ids(data, key, field):
    res = data.get(key) or {}
    if res.get("error"):
        print("  errors:", json.dumps(res["error"])[:600])
    return [s[field] for s in res.get("success") or []]


print(json.dumps(S, ensure_ascii=False, indent=1), SKUS)
if not APPLY:
    sys.exit(0)
cid = ids(call("/sp/campaigns", "spCampaign", {"campaigns": [{
    "name": S["name"], "targetingType": "MANUAL", "state": "ENABLED", "startDate": date.today().isoformat(),
    "portfolioId": S["portfolio"], "budget": {"budgetType": "DAILY", "budget": S["budget"]},
    "dynamicBidding": {"strategy": "LEGACY_FOR_SALES"}}]}), "campaigns", "campaignId")[0]
ag = ids(call("/sp/adGroups", "spAdGroup", {"adGroups": [{"campaignId": cid, "name": "LEOPARD", "state": "ENABLED",
                                                          "defaultBid": S["bid"]}]}), "adGroups", "adGroupId")[0]
ads = ids(call("/sp/productAds", "spProductAd", {"productAds": [
    {"campaignId": cid, "adGroupId": ag, "sku": s, "state": "ENABLED"} for s in SKUS]}), "productAds", "adId")
kw = ids(call("/sp/keywords", "spKeyword", {"keywords": [
    {"campaignId": cid, "adGroupId": ag, "keywordText": t, "matchType": "EXACT", "state": "ENABLED", "bid": S["bid"]}
    for t in S["keywords"]]}), "keywords", "keywordId")
print(S["name"], "campaign", cid, "ad group", ag, "ads", len(ads), "keywords", len(kw))

# watch it in the Monday changes review
from JobState import get_job_state, set_job_state
watch = json.loads(get_job_state(pb, "change_watch") or "[]")
watch.append({"label": f"{S['name']} (leopard test, stop at 25 spend / 0 orders)", "since": date.today().isoformat(),
              "campaign_ids": [cid]})
set_job_state(pb, "change_watch", json.dumps(watch))
print("added to change_watch")
