"""October 2026 ads review, proposals A-G (approved by the user on /checks and
"yes" in chat 2026-11-02). Preview by default, --apply. Every change is logged
in ads_bid_change_log; temporary product-ad pauses go to job_state
product_ad_pauses (restored by ProductAdPauses on unpause_on).

A. SB video: pause the 4 with 0 sales; budget / 2 and keyword bids -30% on the rest
B. DE: POUF AUTO bids -20%, DE POUF GOLDEN NUGGETS -25%, POUF_BRAND_VIN (SB) -15%;
   pause Geom107 in DE POUF GOLDEN NUGGETS (PBlue56 already paused today)
C. Low stock until the next shipment: pause RectPurple103 product ads (US + EU)
   and keep RoundBlue93A (US) paused until the US shipment
D. USAC SP Broad budget 30 -> 40; top of search +20 points on SP Broad + EXACT WINNERS
E. FR PAREO SP EXACT bids -30%
F. Pause UK Pareo Broad (winter)
G. DE SP "rest of search" placement -> 0; FR POUF SP EXACT bids -15%"""
import json
import sys
from datetime import date

import requests

from AdsBidWriter import (find_profile_connection, pb_authenticate, record_bid_change, refresh_access_token,
                          update_sb_keyword_bid)
from AsinAnalytics import pb_list_all
from JobState import get_job_state, set_job_state

APPLY = "--apply" in sys.argv  # APPLIED 2026-11-02 - do not re-run with --apply (bids would be cut again)
REASON = "October 2026 ads review (A-G), approved by the user on /checks 2026-11-02"
SPC, SBC = "application/vnd.spCampaign.v3+json", "application/vnd.sbcampaignresource.v4+json"
SPK, SPT = "application/vnd.spKeyword.v3+json", "application/vnd.spTargetingClause.v3+json"
SPA = "application/vnd.spProductAd.v3+json"
US, DE, FR, UK, ES, IT = ("1649312585287580", "1262925395560957", "1167111969385560", "429682557125413",
                          "548812940689391", "2967253577783675")
CC = {US: "US", DE: "DE", FR: "FR", UK: "UK", ES: "ES", IT: "IT"}
SB_MIN = {US: 0.25}  # others 0.10

SB_PAUSE = [(FR, "147111071039981", "FR SB VIDEO V251"), (US, "182621016363839", "USAC SB VIDEO MAGIC535"),
            (ES, "93487459373842", "ES SB VIDEO MAGIC535"), (DE, "56364763868664", "DE SB BRAND VIDEO VELVET")]
SB_HALVE = [(US, "137239449977751", "USAC SB BRAND VIDEO VELVET"), (US, "201574856862290", "USAC SB VIDEO V251"),
            (US, "143263766617036", "USAC SB VIDEO YELLOW525"), (DE, "5323465598894", "DE SB VIDEO MAGIC535"),
            (DE, "108815601633868", "DE SB VIDEO V251"), (DE, "39585994260668", "DE SB VIDEO YELLOW525"),
            (UK, "30539585802735", "UK SB VIDEO MAGIC535"), (UK, "235771212465468", "UK SB VIDEO V251"),
            (FR, "30540676954626", "FR SB VIDEO MAGIC535"), (ES, "279473019702798", "ES SB VIDEO V251"),
            (IT, "206174119420142", "IT SB VIDEO V251")]
SP_BID_CUTS = [(DE, "96446638963188", "POUF AUTO", 0.20), (DE, "264169843138957", "DE POUF GOLDEN NUGGETS", 0.25),
               (FR, "11766355879387", "PAREO SP EXACT", 0.30), (FR, "140399729367287", "POUF SP EXACT", 0.15)]
SB_BID_CUTS = [(DE, "410117005436685", "POUF_BRAND_VIN", 0.15)]
PAUSE_SP = [(UK, "155020454150206", "Pareo Broad")]
BUDGETS = [(US, "36387990770187", "USAC SP Broad", 40)]
TOP_PLUS = [(US, "36387990770187", "USAC SP Broad", 20), (US, "169360030382165", "USAC SP EXACT WINNERS", 20)]
# product ads to pause until the region's next shipment: (sku, profiles, only campaign id or None)
PRODUCT_PAUSES = [("CoverPoufGeom107", [DE], "264169843138957", "2026-11-16"),
                  ("CoverPoufRectPurple103", [US], None, "2027-02-01"),
                  ("CoverPoufRectPurple103", [DE, FR, IT, ES, UK], None, "2027-04-01")]
EXTEND = [("CoverPoufRoundBlue93A", "US", "2027-02-01")]  # already paused today until 11-16 -> US shipment

pb = pb_authenticate()
_conns = {}
done, failed = [], []


def ctx(profile):
    if profile not in _conns:
        c = find_profile_connection(pb, profile)
        _conns[profile] = (c, refresh_access_token(c["profile_key"], c["refresh_token"]))
    return _conns[profile]


def call(profile, method, path, body=None, media="application/json", params=None):
    c, tok = ctx(profile)
    h = {"Authorization": f"Bearer {tok}", "Amazon-Advertising-API-ClientId": c["client_id"],
         "Amazon-Advertising-API-Scope": profile}
    if media:
        h.update({"Content-Type": media, "Accept": media})
    r = requests.request(method, c["base_url"] + path, json=body, params=params, timeout=90, headers=h)
    if r.status_code >= 400:
        raise RuntimeError(f"{path}: HTTP {r.status_code} {r.text[:400]}")
    return r.json() if r.content else {}


def ok(res, key):
    part = res.get(key) or {}
    if part.get("error"):
        raise RuntimeError(json.dumps(part["error"])[:500])
    return part.get("success")


def log(action, prof, camp, name, tid, text, old, new, product, resp):
    record_bid_change(pb, {"action": action, "ad_product": product, "campaign_id": camp, "campaign_name": name,
                           "changed_at": date.today().isoformat(), "country_code": CC[prof], "profile_id": prof,
                           "target_id": str(tid), "target_text": text, "old_bid": old, "new_bid": new,
                           "reason": REASON, "status": "applied", "amazon_response": json.dumps(resp)[:3000]})


def step(label, fn):
    try:
        msg = fn()
        if msg:
            print(("OK  " if APPLY else "PRE ") + label + ": " + msg)
            done.append(f"{label}: {msg}")
    except Exception as exc:
        print("ERR " + label + ": " + str(exc)[:300])
        failed.append(f"{label}: {str(exc)[:200]}")


def sb_campaign(prof, cid):
    c = call(prof, "POST", "/sb/v4/campaigns/list", {"campaignIdFilter": {"include": [cid]}}, SBC).get("campaigns", [])
    if not c:
        raise RuntimeError("campaign not found")
    return c[0]


def sb_set(prof, cid, name, state=None, budget=None):
    c = sb_campaign(prof, cid)
    item = {"campaignId": cid}
    if state:
        item["state"] = state
    if budget is not None:
        item["budget"] = budget
    if APPLY:
        res = call(prof, "PUT", "/sb/v4/campaigns", {"campaigns": [item]}, SBC)
        ok(res, "campaigns")
        log("pause_campaign" if state else "budget", prof, cid, name, cid, name, c.get("budget"),
            0 if state else budget, "SPONSORED_BRANDS", res)
    return f"{c['state']} budget {c.get('budget')} -> " + (state or f"budget {budget}")


def sb_keyword_cut(prof, cid, name, pct):
    kws = call(prof, "GET", "/sb/keywords", params={"campaignIdFilter": cid, "count": 1000}, media=None)
    out = []
    for k in kws if isinstance(kws, list) else []:
        if k.get("state") != "enabled":
            continue
        old = k.get("bid")
        if not old:
            continue
        new = max(round(old * (1 - pct), 2), SB_MIN.get(prof, 0.10))
        if new >= old:
            out.append(f"{k['keywordText']} {old} (at minimum)")
            continue
        out.append(f"{k['keywordText']} {old}->{new}")
        if APPLY:
            c, tok = ctx(prof)
            res = update_sb_keyword_bid(c["base_url"], tok, c["client_id"], prof, k["keywordId"], cid, k["adGroupId"], new)
            log("bid_change", prof, cid, name, k["keywordId"], k["keywordText"], old, new, "SPONSORED_BRANDS", res)
    return f"{len(out)} keyword(s): " + "; ".join(out[:12])


def sp_bid_cut(prof, cid, name, pct):
    kws = call(prof, "POST", "/sp/keywords/list", {"campaignIdFilter": {"include": [cid]},
                                                    "stateFilter": {"include": ["ENABLED"]}}, SPK).get("keywords", [])
    tgs = call(prof, "POST", "/sp/targets/list", {"campaignIdFilter": {"include": [cid]},
                                                   "stateFilter": {"include": ["ENABLED"]}}, SPT).get("targetingClauses", [])
    ags = {a["adGroupId"]: a.get("defaultBid") for a in call(prof, "POST", "/sp/adGroups/list", {
        "campaignIdFilter": {"include": [cid]}}, "application/vnd.spAdGroup.v3+json").get("adGroups", [])}
    n, ex = 0, []
    for kind, items, media, idf, key in (("keyword", kws, SPK, "keywordId", "keywords"),
                                        ("target", tgs, SPT, "targetId", "targetingClauses")):
        for it in items:
            old = it.get("bid") or ags.get(it["adGroupId"])
            if not old:
                continue
            new = max(round(old * (1 - pct), 2), 0.05 if prof != US else 0.10)
            if new >= old:
                continue
            text = it.get("keywordText") or json.dumps(it.get("expression"))[:60]
            n += 1
            if len(ex) < 6:
                ex.append(f"{text[:30]} {old}->{new}")
            if APPLY:
                res = call(prof, "PUT", "/sp/" + ("keywords" if kind == "keyword" else "targets"),
                           {key: [{idf: it[idf], "bid": new}]}, media)
                ok(res, key)
                log("bid_change", prof, cid, name, it[idf], text, old, new, "SPONSORED_PRODUCTS", res)
    return f"{n} bid(s) -{pct * 100:.0f}%: " + "; ".join(ex) + (" …" if n > len(ex) else "")


def sp_campaign(prof, cid):
    c = call(prof, "POST", "/sp/campaigns/list", {"campaignIdFilter": {"include": [cid]}}, SPC).get("campaigns", [])
    if not c:
        raise RuntimeError("campaign not found")
    return c[0]


def sp_set(prof, cid, name, state=None, budget=None):
    c = sp_campaign(prof, cid)
    item = {"campaignId": cid}
    if state:
        item["state"] = state
    if budget is not None:
        item["budget"] = {"budget": budget, "budgetType": "DAILY"}
    if APPLY:
        res = call(prof, "PUT", "/sp/campaigns", {"campaigns": [item]}, SPC)
        ok(res, "campaigns")
        log("pause_campaign" if state else "budget", prof, cid, name, cid, name, c["budget"]["budget"],
            0 if state else budget, "SPONSORED_PRODUCTS", res)
    return f"{c['state']} budget {c['budget']['budget']} -> " + (state or f"budget {budget}")


def placement(prof, cid, name, change_fn):
    c = sp_campaign(prof, cid)
    db = c.get("dynamicBidding") or {}
    old = {p["placement"]: p["percentage"] for p in db.get("placementBidding") or []}
    new = change_fn(dict(old))
    if new == old:
        return None
    if APPLY:
        res = call(prof, "PUT", "/sp/campaigns", {"campaigns": [{"campaignId": cid, "dynamicBidding": {
            "strategy": db.get("strategy"),
            "placementBidding": [{"placement": k, "percentage": v} for k, v in new.items() if v]}}]}, SPC)
        ok(res, "campaigns")
        log("placement", prof, cid, name, cid, json.dumps(new), 0, 0, "SPONSORED_PRODUCTS", res)
    return f"{old} -> {new}"


def product_pauses():
    mapping = {g["sku"]: g["asin"] for g in pb_list_all(pb, "asin_group_mapping", 'asin != ""', fields="sku,asin")}
    pauses = json.loads(get_job_state(pb, "product_ad_pauses") or "[]")
    msgs = []
    for sku, profs, only, until in PRODUCT_PAUSES:
        asin = mapping[sku]
        for prof in profs:
            ads = call(prof, "POST", "/sp/productAds/list", {"stateFilter": {"include": ["ENABLED"]},
                                                              **({"campaignIdFilter": {"include": [only]}} if only else {})},
                       SPA).get("productAds", [])
            ads = [a for a in ads if a.get("asin") == asin or a.get("sku") == sku]
            enabled = {c["campaign_id"]: c["campaign_name"] for c in pb_list_all(
                pb, "ads_campaigns", f'profile_id = "{prof}" && campaign_status = "ENABLED"', fields="campaign_id,campaign_name")}
            for a in ads:
                cname = enabled.get(str(a["campaignId"]))
                if not cname:
                    continue
                msgs.append(f"{CC[prof]} {cname} {sku} until {until}")
                if APPLY:
                    res = call(prof, "PUT", "/sp/productAds", {"productAds": [{"adId": a["adId"], "state": "PAUSED"}]}, SPA)
                    ok(res, "productAds")
                    log("product_ad_state", prof, str(a["campaignId"]), cname, f"productad:{a['adId']}",
                        f"product ad {sku} -> PAUSED", 0, 0, "SPONSORED_PRODUCTS", res)
                    key = f"{prof}|{a['campaignId']}|{a['adGroupId']}|{asin}"
                    pauses = [p for p in pauses if p["key"] != key]
                    pauses.append({"key": key, "profile_id": prof, "campaign_id": str(a["campaignId"]), "campaign": cname,
                                   "ad_group_id": str(a["adGroupId"]), "asin": asin, "sku": sku, "country": CC[prof],
                                   "history": "good", "ad_id": str(a["adId"]), "paused_on": date.today().isoformat(),
                                   "unpause_on": until, "status": "active"})
    for sku, cc, until in EXTEND:
        for p in pauses:
            if p["sku"] == sku and p["country"] == cc and p["status"] == "active":
                msgs.append(f"{cc} {p['campaign']} {sku}: keep paused until {until} (was {p['unpause_on']})")
                p["unpause_on"] = until
    if APPLY:
        set_job_state(pb, "product_ad_pauses", json.dumps(pauses))
    return "; ".join(msgs)


for prof, cid, name in SB_PAUSE:
    step(f"A pause {CC[prof]} {name}", lambda: sb_set(prof, cid, name, state="PAUSED"))
for prof, cid, name in SB_HALVE:
    b = sb_campaign(prof, cid).get("budget") or 0
    step(f"A budget/2 {CC[prof]} {name}", lambda: sb_set(prof, cid, name, budget=max(round(b / 2, 2), 1)))
    step(f"A bids -30% {CC[prof]} {name}", lambda: sb_keyword_cut(prof, cid, name, 0.30))
for prof, cid, name, pct in SP_BID_CUTS:
    step(f"{'B' if prof == DE else 'E/G'} {CC[prof]} {name}", lambda: sp_bid_cut(prof, cid, name, pct))
for prof, cid, name, pct in SB_BID_CUTS:
    step(f"B {CC[prof]} {name} (SB)", lambda: sb_keyword_cut(prof, cid, name, pct))
step("B/C product ads", product_pauses)
for prof, cid, name, val in BUDGETS:
    step(f"D {CC[prof]} {name}", lambda: sp_set(prof, cid, name, budget=val))
for prof, cid, name, plus in TOP_PLUS:
    step(f"D top of search {CC[prof]} {name}",
         lambda: placement(prof, cid, name, lambda d: {**d, "PLACEMENT_TOP": d.get("PLACEMENT_TOP", 0) + plus}))
for prof, cid, name in PAUSE_SP:
    step(f"F pause {CC[prof]} {name}", lambda: sp_set(prof, cid, name, state="PAUSED"))
for c in pb_list_all(pb, "ads_campaigns", f'profile_id = "{DE}" && campaign_status = "ENABLED" && ad_product = "SPONSORED_PRODUCTS"',
                     fields="campaign_id,campaign_name"):
    step(f"G rest of search -> 0 DE {c['campaign_name']}",
         lambda: placement(DE, c["campaign_id"], c["campaign_name"],
                           lambda d: {k: v for k, v in d.items() if k != "PLACEMENT_REST_OF_SEARCH"}))
print("\nAPPLIED" if APPLY else "\nPREVIEW", "ok", len(done), "failed", len(failed))
