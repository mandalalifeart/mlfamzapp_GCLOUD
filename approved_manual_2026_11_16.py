"""Approved "done by Claude" proposals of the weekly search-term harvests of
2026-11-09 (report 0hz64wk2u6m0285) and 2026-11-16 (4mmqp482s7qh4wl), user
"make all approved adsreview changes" 2026-11-16. The two weeks overlap (same
terms) - each term is done once; 11-16 wording wins (e.g. DE boho deko = lower).
Kinds: lower (-30%, skipped if changed in the last 14 days), neg (SB negative
exact in all ad groups), exact (new SP exact keyword in the PHRASE campaign
where the term sold: ES/NL have no pareo EXACT campaign).
Preview by default, --apply to write; proposals set to "applied" afterwards."""
import json
import sys
from datetime import date

from AdsBidWriter import record_bid_change, write_bid_or_state
from AdsGrowth import Api, _media, _recently_changed
from AsinAnalytics import pb_authenticate, pb_list_all
from UpdateSkuSalesMonth import pb_batch

APPLY = "--apply" in sys.argv
R1, R2 = "0hz64wk2u6m0285", "4mmqp482s7qh4wl"
LOWER = 0.30
# (proposal keys [(report, id)], country, campaign, term, kind, extra)
ITEMS = [
    ([(R1, 1), (R2, 1)], "ES", "ES PAREO PHRASE", "pareos playa mujer", "exact", {"ag": "114405589198179", "bid": 0.45}),
    ([(R1, 5), (R2, 2)], "NL", "NL SP PAREO PHRASE", "sarong", "exact", {"ag": "2111362342746", "bid": 0.43}),
    ([(R1, 8)], "UK", "UK POUF KW", "meditation cushion", "lower", None),
    ([(R2, 5)], "UK", "UK POUF KW", "meditation cushions", "lower", None),
    ([(R2, 5)], "UK", "UK POUF KW", "pouffes for living room", "lower", None),
    ([(R2, 6)], "UK", "PAREO SP EXACT", "sarong", "lower", None),
    ([(R1, 9), (R2, 7)], "IT", "IT SB POUF PHRASE", "cuscini boho", "neg", None),
    ([(R1, 9), (R2, 7)], "IT", "IT SB POUF PHRASE", "cuscini decorativi per divano", "neg", None),
    ([(R1, 9), (R2, 7)], "IT", "IT SB POUF PHRASE", "pouf orientale", "neg", None),
    ([(R1, 10), (R2, 8)], "IT", "IT SB VIDEO V251", "pouf", "lower", None),
    ([(R1, 14), (R2, 11)], "FR", "FR 2024 PAREO VIDEO", "pareo femme plage", "neg", None),
    ([(R1, 17), (R2, 14)], "DE", "DE POUF PHRASE", "bodenkissen zum sitzen", "lower", None),
    ([(R1, 17), (R2, 14)], "DE", "DE POUF PHRASE", "boho deko", "lower", None),
    ([(R1, 20), (R2, 15)], "DE", "PAREO SP EXACT", "pareo baumwolle", "lower", None),
    ([(R1, 21), (R2, 16)], "DE", "POUF_BRAND_VIN", "afrikanischer hocker", "neg", None),
    ([(R1, 21), (R2, 16)], "DE", "POUF_BRAND_VIN", "bodenkissen", "neg", None),
    ([(R1, 21)], "DE", "POUF_BRAND_VIN", "bodenkissen groß bohemian", "neg", None),
    ([(R1, 21)], "DE", "POUF_BRAND_VIN", "marokkanischer hocker", "neg", None),
    ([(R1, 21), (R2, 16)], "DE", "POUF_BRAND_VIN", "boho deko", "lower", None),
    ([(R1, 21), (R2, 16)], "DE", "POUF_BRAND_VIN", "meditationskissen", "neg", None),
    ([(R1, 22), (R2, 17)], "DE", "DE SB VIDEO V251", "pouf", "lower", None),
    ([(R1, 22), (R2, 17)], "DE", "DE SB VIDEO V251", "bodenkissen", "lower", None),
    ([(R1, 25)], "US", "USAC SP EXACT WINNERS", "meditation pillows for sitting on floor", "lower", None),
    ([(R1, 25)], "US", "USAC SP EXACT WINNERS", "pouf ottomans for seating", "lower", None),
    ([(R1, 25), (R2, 23)], "US", "USAC SP EXACT WINNERS", "poufs", "lower", None),
    ([(R1, 25), (R2, 23)], "US", "USAC SP EXACT WINNERS", "boho ottoman", "lower", None),
    ([(R2, 23)], "US", "USAC SP EXACT WINNERS", "meditation cushion", "lower", None),
    ([(R1, 29), (R2, 25)], "US", "USAC SB BRAND VIDEO VELVET", "floor pillow", "lower", None),
    ([(R1, 29), (R2, 25)], "US", "USAC SB BRAND VIDEO VELVET", "ottoman", "lower", None),
    ([(R1, 30), (R2, 26)], "US", "USAC SB POUF", "moroccan pouf", "neg", None),
    ([(R1, 30), (R2, 26)], "US", "USAC SB POUF", "ottoman", "neg", None),
    ([(R1, 30), (R2, 26)], "US", "USAC SB POUF", "pouf ottoman", "lower", None),
    ([(R1, 31), (R2, 27)], "US", "USAC SB VIDEO YELLOW525", "ottoman", "lower", None),
    ([(R1, 32), (R2, 28)], "US", "USAC SB VIDEO V251", "pouf", "lower", None),
    ([(R1, 33), (R2, 29)], "UK", "UK SB VIDEO V251", "pouffe", "lower", None),
]


def campaign(pb, api, cc, name):
    rows = [r for r in pb_list_all(pb, "ads_campaigns", f'campaign_name = "{name}"')
            if (r.get("country_code") or "").upper() in (cc, "GB" if cc == "UK" else cc)]
    for r in sorted(rows, key=lambda r: r.get("campaign_status") != "ENABLED"):
        try:
            api.ctx(r["profile_id"])
            return r
        except Exception:
            continue
    raise RuntimeError(f"campaign not found: {cc} {name}")


def keywords(api, c):
    prof, cid = c["profile_id"], c["campaign_id"]
    if c["ad_product"] == "SPONSORED_PRODUCTS":
        flt = {"campaignIdFilter": {"include": [cid]}, "stateFilter": {"include": ["ENABLED"]}, "maxResults": 1000}
        return [{"id": str(k["keywordId"]), "text": k["keywordText"].lower(), "match": k["matchType"].lower(),
                 "bid": k.get("bid"), "ag": str(k["adGroupId"])}
                for k in api.call(prof, "POST", "/sp/keywords/list", flt, _media("spKeyword")).get("keywords", [])]
    return [{"id": str(k["keywordId"]), "text": k["keywordText"].lower(), "match": k["matchType"].lower(),
             "bid": k.get("bid"), "ag": str(k["adGroupId"])}
            for k in api.call(prof, "GET", "/sb/keywords", params={"campaignIdFilter": cid, "count": 1000}) or []
            if k.get("state") == "enabled"]


def main():
    pb = pb_authenticate()
    api = Api(pb)
    changed = _recently_changed(pb, date.today())
    results = {}
    for keys, cc, name, term, kind, extra in ITEMS:
        try:
            c = campaign(pb, api, cc, name)
            kws = keywords(api, c)
            if kind == "lower":
                hits = [k for k in kws if k["text"] == term and k["match"] == "exact"] or [k for k in kws if k["text"] == term]
                msg = f"{term}: no enabled keyword found"
                for k in hits:
                    if k["id"] in changed:
                        msg = f"{term}: skipped (bid changed in the last 14 days)"
                        continue
                    floor = (0.25 if cc == "US" else 0.10) if c["ad_product"] == "SPONSORED_BRANDS" else 0.02
                    new = round(k["bid"] * (1 - LOWER), 2)
                    if new < floor:
                        msg = f"{term}: bid {k['bid']} already near the minimum - left"
                        continue
                    msg = f"{term} ({k['match']}) {k['bid']}→{new}"
                    if APPLY:
                        conn, tok = api.ctx(c["profile_id"])
                        resp = write_bid_or_state(conn, tok, c["profile_id"], c["ad_product"], True, k["id"],
                                                  c["campaign_id"], k["ag"], new)
                        record_bid_change(pb, {"action": "bid_change", "ad_product": c["ad_product"], "campaign_id": c["campaign_id"],
                                               "campaign_name": name, "changed_at": date.today().isoformat(), "country_code": cc,
                                               "profile_id": c["profile_id"], "target_id": k["id"], "target_text": term,
                                               "old_bid": k["bid"], "new_bid": new, "status": "applied",
                                               "reason": f"Search term harvest 2026-11-16 #{keys[-1][1]}: search term = keyword, clicks and no orders, -30%",
                                               "amazon_response": json.dumps(resp)[:2000]})
            elif kind == "neg":
                ags = sorted({k["ag"] for k in kws})
                msg = f"SB negative exact '{term}' in {len(ags)} ad group(s)"
                if APPLY:
                    resp = api.call(c["profile_id"], "POST", "/sb/negativeKeywords",
                                    [{"campaignId": int(c["campaign_id"]), "adGroupId": int(ag), "keywordText": term,
                                      "matchType": "negativeExact"} for ag in ags])
                    bad = [x for x in resp if x.get("code") != "SUCCESS"] if isinstance(resp, list) else [resp]
                    if bad:
                        dup = all("DUPLICATE" in json.dumps(b).upper() for b in bad)
                        msg += " - already there" if dup else f" - {json.dumps(bad)[:150]}"
            else:  # new SP exact keyword
                exists = [k for k in kws if k["text"] == term and k["match"] == "exact"]
                msg = f"new exact '{term}' bid {extra['bid']} in {name}"
                if exists:
                    msg = f"exact '{term}' already exists in {name}"
                elif APPLY:
                    resp = api.call(c["profile_id"], "POST", "/sp/keywords", {"keywords": [
                        {"campaignId": c["campaign_id"], "adGroupId": extra["ag"], "keywordText": term, "matchType": "EXACT",
                         "state": "ENABLED", "bid": extra["bid"]}]}, _media("spKeyword"))
                    ok = resp.get("keywords", {}).get("success") or []
                    if ok:
                        record_bid_change(pb, {"action": "new_keyword", "ad_product": "SPONSORED_PRODUCTS", "campaign_id": c["campaign_id"],
                                               "campaign_name": name, "changed_at": date.today().isoformat(), "country_code": cc,
                                               "profile_id": c["profile_id"], "target_id": str(ok[0].get("keywordId")), "target_text": term,
                                               "old_bid": 0, "new_bid": extra["bid"], "status": "applied",
                                               "reason": f"Search term harvest 2026-11-16 #{keys[-1][1]}: new exact keyword from converting search term",
                                               "amazon_response": json.dumps(resp)[:2000]})
                    else:
                        msg += f" - {json.dumps(resp.get('keywords', {}).get('error'))[:200]}"
        except Exception as exc:
            msg = f"{term}: error {str(exc)[:150]}"
        print(f"{keys} {cc} {name}: {msg}")
        for key in keys:
            results.setdefault(key, []).append(msg)
    if APPLY:
        for rep in (R1, R2):
            rec = pb_list_all(pb, "check_reports", f'id = "{rep}"')[0]
            props = rec["proposals"]
            for p in props:
                if p.get("status") == "approved" and (rep, p["id"]) in results:
                    p["status"] = "applied"
                    p["result"] = ("(also in the 2026-11-16 harvest, done once) " if rep == R1 else "") + "; ".join(results[(rep, p["id"])])
            pb_batch(pb, [{"method": "PATCH", "url": f"/api/collections/check_reports/records/{rep}", "body": {"proposals": props}}])


if __name__ == "__main__":
    main()
