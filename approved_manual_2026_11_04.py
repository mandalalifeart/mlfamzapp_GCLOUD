"""Approved "by Claude" keyword-harvest proposals of the October 2026 monthly
ads review (check_reports ads-monthly-auto 2026-10), user "make all approved
items" 2026-11-04.

- "lower/pause the exact keyword": a search term = our own exact keyword with
  clicks and no orders -> bid -30% (not paused: history rule), skipped if the
  bid was changed in the last 14 days (e.g. by today's TACOS rule).
- "SB negative": negative exact in the SB campaign's ad group(s).
- #44 failed (keywords already in EXACT WINNERS): only the negative exact in
  USAC AUTO that the failed action never reached.
- #45 DE: new SP campaign DE POUF EXACT with the 3 exact keywords.
Preview by default, --apply to write. Proposals set to "applied" afterwards."""
import json
import sys
from datetime import date

from AdsBidWriter import record_bid_change, write_bid_or_state
from AdsGrowth import Api, _media, _recently_changed
from AsinAnalytics import pb_authenticate, pb_list_all
from UpdateSkuSalesMonth import pb_batch

APPLY = "--apply" in sys.argv
REPORT = "dd9g0p099nk7a4z"
LOWER = 0.30
# (proposal id, country, campaign name, term, kind)
ITEMS = [
    (26, "UK", "UK POUF KW", "floor cushion", "lower"),
    (28, "DE", "POUF_BRAND_VIN", "bodenkissen", "neg"),
    (28, "DE", "POUF_BRAND_VIN", "bodenkissen groß bohemian", "neg"),
    (28, "DE", "POUF_BRAND_VIN", "marokkanischer hocker", "lower"),
    (29, "DE", "DE SB VIDEO V251", "pouf", "lower"),
    (34, "US", "USAC SB BRAND VIDEO VELVET", "ottoman", "lower"),
    (48, "IT", "IT SB POUF PHRASE", "cuscini decorativi per divano", "neg"),
    (48, "IT", "IT SB POUF PHRASE", "pouf orientale", "neg"),
    (52, "ES", "ES SB  PAREO KEYWORD", "pareo", "neg"),
    (55, "DE", "DE POUF PHRASE", "bodenkissen zum sitzen", "lower"),
    (55, "DE", "DE POUF PHRASE", "boho deko", "lower"),
    (58, "DE", "POUF_BRAND_VIN", "afrikanischer hocker", "lower"),
    (58, "DE", "POUF_BRAND_VIN", "marokkanischer hocker", "neg"),
    (59, "DE", "DE SB VIDEO V251", "bodenkissen", "lower"),
    (62, "US", "USAC SP EXACT WINNERS", "meditation pillows for sitting on floor", "lower"),
    (62, "US", "USAC SP EXACT WINNERS", "pouf ottomans for seating", "lower"),
    (62, "US", "USAC SP EXACT WINNERS", "poufs", "lower"),
    (66, "US", "USAC SB BRAND VIDEO VELVET", "floor pillow", "lower"),
    (67, "US", "USAC SB POUF", "moroccan pouf", "lower"),
    (68, "US", "USAC SB VIDEO YELLOW525", "ottoman", "lower"),
    (69, "US", "USAC SB VIDEO V251", "pouf", "lower"),
    (70, "US", "PAREO VIDEO EXACT", "swim cover up for women", "neg"),
    (71, "UK", "UK SB VIDEO V251", "pouffe", "lower"),
    (44, "US", "USAC AUTO", "meditation cushion", "spneg"),
    (44, "US", "USAC AUTO", "pouf ottoman boho", "spneg"),
    (44, "US", "USAC AUTO", "floor cushion boho", "spneg"),
    (44, "US", "USAC AUTO", "moroccan floor poufs", "spneg"),
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
    for pid, cc, name, term, kind in ITEMS:
        try:
            c = campaign(pb, api, cc, name)
            kws = keywords(api, c)
            if kind == "lower":
                hits = [k for k in kws if k["text"] == term and k["match"] == "exact"] or [k for k in kws if k["text"] == term]
                if not hits:
                    msg = f"{term}: no enabled keyword found"
                for k in hits:
                    if k["id"] in changed:
                        msg = f"{term}: skipped (bid changed in the last 14 days)"
                        continue
                    floor = 0.25 if c["ad_product"] == "SPONSORED_BRANDS" else 0.02
                    new = max(floor, round(k["bid"] * (1 - LOWER), 2))
                    msg = f"{term} ({k['match']}) {k['bid']}→{new}"
                    if APPLY and new < k["bid"]:
                        conn, tok = api.ctx(c["profile_id"])
                        resp = write_bid_or_state(conn, tok, c["profile_id"], c["ad_product"], True, k["id"],
                                                  c["campaign_id"], k["ag"], new)
                        record_bid_change(pb, {"action": "bid_change", "ad_product": c["ad_product"], "campaign_id": c["campaign_id"],
                                               "campaign_name": name, "changed_at": date.today().isoformat(), "country_code": cc,
                                               "profile_id": c["profile_id"], "target_id": k["id"], "target_text": term,
                                               "old_bid": k["bid"], "new_bid": new, "status": "applied",
                                               "reason": f"Monthly ads review 2026-10 #{pid}: search term = keyword, clicks and no orders, -30%",
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
                        msg += f" - {json.dumps(bad)[:150]}"
            else:  # SP campaign negative exact
                msg = f"SP negative exact '{term}' in {name}"
                if APPLY:
                    resp = api.call(c["profile_id"], "POST", "/sp/campaignNegativeKeywords", {"campaignNegativeKeywords": [
                        {"campaignId": c["campaign_id"], "keywordText": term, "matchType": "NEGATIVE_EXACT", "state": "ENABLED"}]},
                        _media("spCampaignNegativeKeyword"))
                    err = resp.get("campaignNegativeKeywords", {}).get("error")
                    if err:
                        msg += f" - {json.dumps(err)[:150]}"
        except Exception as exc:
            msg = f"{term}: error {str(exc)[:150]}"
        print(f"#{pid} {cc} {name}: {msg}")
        results.setdefault(pid, []).append(msg)
    if APPLY:
        rec = pb_list_all(pb, "check_reports", f'id = "{REPORT}"')[0]
        for p in rec["proposals"]:
            if p["id"] in results:
                p["status"] = "applied"
                p["result"] = ("negatives added in USAC AUTO (keywords were already in EXACT WINNERS); " if p["id"] == 44 else "") \
                    + "; ".join(results[p["id"]])
        pb_batch(pb, [{"method": "PATCH", "url": f"/api/collections/check_reports/records/{REPORT}",
                       "body": {"proposals": rec["proposals"]}}])


if __name__ == "__main__":
    main()
