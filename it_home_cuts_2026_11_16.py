"""Italy home decor temporary cuts, approved by the user 2026-11-16 ("yes").
IT SB POUF PHRASE was good Aug-Sep, but since 2 Nov 760 clicks / EUR 156 / 1 order;
IT SP POUF AUTO "substitutes" 466 clicks / EUR 73 / 0 orders. Good history ->
temporary cuts, re-check 2026-11-30 (job_state bid_reviews + change_watch).
Preview by default, --apply to write."""
import json
import sys
from datetime import date

from AdsBidWriter import record_bid_change, write_bid_or_state
from AdsGrowth import Api, _recently_changed
from AsinAnalytics import pb_authenticate
from approved_manual_2026_11_16 import campaign, keywords
from JobState import get_job_state, set_job_state

APPLY = "--apply" in sys.argv
LABEL = "IT home decor temporary cuts 2026-11-16"
SB_BUDGET_NEW = 5.0
LOWER = 0.30
SB_TERMS = {("cuscino divano", "phrase"), ("cuscino divano", "exact"), ("cuscino arredo poltrona", "phrase"),
            ("cuscino da pavimento", "exact"), ("cuscino da pavimento", "phrase"), ("cuscino rotondo giallo", "exact"),
            ("puff orientale", "phrase"), ("cuscini da arredo", "phrase"), ("pouf poggiapiedi", "exact"),
            ("cuscino", "phrase"), ("pouf", "exact")}
AUTO_TARGET = "183483042348990"   # IT SP POUF AUTO "substitutes"
AUTO_NEW = 0.10


def log(pb, c, name, tid, text, old, new, resp, action="bid_change"):
    record_bid_change(pb, {"action": action, "ad_product": c["ad_product"], "campaign_id": c["campaign_id"],
                           "campaign_name": name, "changed_at": date.today().isoformat(), "country_code": "IT",
                           "profile_id": c["profile_id"], "target_id": tid, "target_text": text, "old_bid": old,
                           "new_bid": new, "status": "applied",
                           "reason": f"{LABEL}: conversion collapsed since 2 Nov (good Aug-Sep history) - temporary, re-check 2026-11-30",
                           "amazon_response": json.dumps(resp)[:2000]})


def main():
    pb = pb_authenticate(); api = Api(pb)
    changed = _recently_changed(pb, date.today())
    # 1. SB budget
    sb = campaign(pb, api, "IT", "IT SB POUF PHRASE")
    cur = api.call(sb["profile_id"], "POST", "/sb/v4/campaigns/list", {"campaignIdFilter": {"include": [sb["campaign_id"]]}},
                   "application/vnd.sbcampaignresource.v4+json")
    old_budget = (cur.get("campaigns") or [{}])[0].get("budget")
    print(f"IT SB POUF PHRASE budget {old_budget} -> {SB_BUDGET_NEW}")
    if APPLY:
        res = api.call(sb["profile_id"], "PUT", "/sb/v4/campaigns", {"campaigns": [{"campaignId": sb["campaign_id"], "budget": SB_BUDGET_NEW}]},
                       "application/vnd.sbcampaignresource.v4+json")
        err = (res.get("campaigns") or {}).get("error") if isinstance(res.get("campaigns"), dict) else None
        print("   ", "ERROR " + json.dumps(err)[:200] if err else "ok")
        if not err:
            log(pb, sb, "IT SB POUF PHRASE", sb["campaign_id"], "IT SB POUF PHRASE", old_budget, SB_BUDGET_NEW, res, "budget")
    # 2a. undo the 2026-11-04 TACOS-rule +20% raises (#17) in this campaign -> old bid
    from AsinAnalytics import pb_list_all
    raised = {}
    for r in pb_list_all(pb, "ads_bid_change_log", f'campaign_id = "{sb["campaign_id"]}" && changed_at >= "2026-11-04" && changed_at < "2026-11-05"'):
        if "TACOS bid rule 2026-10 #17" in (r.get("reason") or "") and (r.get("new_bid") or 0) > (r.get("old_bid") or 0):
            raised[r["target_id"]] = r["old_bid"]
    kws = keywords(api, sb)
    done = set()
    for k in kws:
        if k["id"] in raised and k["bid"] > raised[k["id"]]:
            new = raised[k["id"]]
            if (k["text"], k["match"]) in SB_TERMS:
                new = min(new, round(k["bid"] * (1 - LOWER), 2))
            new = max(new, 0.10)
            print(f"  undo raise {k['text']} ({k['match']}) {k['bid']} -> {new}")
            done.add(k["id"])
            if APPLY:
                conn, tok = api.ctx(sb["profile_id"])
                resp = write_bid_or_state(conn, tok, sb["profile_id"], sb["ad_product"], True, k["id"], sb["campaign_id"], k["ag"], new)
                log(pb, sb, "IT SB POUF PHRASE", k["id"], k["text"], k["bid"], new, resp)
    # 2b. SB keyword bids -30%
    for k in kws:
        if k["id"] in done:
            continue
        if (k["text"], k["match"]) not in SB_TERMS:
            continue
        if k["id"] in changed:
            print(f"  {k['text']} ({k['match']}): skipped, changed in the last 14 days"); continue
        new = round(k["bid"] * (1 - LOWER), 2)
        if new < 0.10:
            print(f"  {k['text']} ({k['match']}): {k['bid']} would go below the SB minimum - left"); continue
        print(f"  {k['text']} ({k['match']}) {k['bid']} -> {new}")
        if APPLY:
            conn, tok = api.ctx(sb["profile_id"])
            resp = write_bid_or_state(conn, tok, sb["profile_id"], sb["ad_product"], True, k["id"], sb["campaign_id"], k["ag"], new)
            log(pb, sb, "IT SB POUF PHRASE", k["id"], k["text"], k["bid"], new, resp)
    # 3. SP auto substitutes
    sp = campaign(pb, api, "IT", "IT SP POUF AUTO")
    tg = api.call(sp["profile_id"], "POST", "/sp/targets/list", {"targetIdFilter": {"include": [AUTO_TARGET]}},
                  "application/vnd.sptargetingClause.v3+json").get("targetingClauses", [])
    if tg:
        t = tg[0]; old = t.get("bid")
        print(f"IT SP POUF AUTO substitutes {old} -> {AUTO_NEW} (its 4-Nov raise 0.13->0.15 undone + cut, user OK)")
        if APPLY:
            conn, tok = api.ctx(sp["profile_id"])
            resp = write_bid_or_state(conn, tok, sp["profile_id"], sp["ad_product"], False, AUTO_TARGET, sp["campaign_id"], t["adGroupId"], AUTO_NEW)
            log(pb, sp, "IT SP POUF AUTO", AUTO_TARGET, "substitutes", old, AUTO_NEW, resp)
    else:
        print("IT SP POUF AUTO substitutes target not found")
    # 4. re-check 2026-11-30
    if APPLY:
        reviews = json.loads(get_job_state(pb, "bid_reviews") or "[]")
        reviews.append({"date": "2026-11-30", "label": LABEL + " - restore if conversion is back?",
                        "campaign_ids": [sb["campaign_id"], sp["campaign_id"]],
                        "args": {"country_code": "IT", "lookback_days": 14, "attribution_lag_days": 2, "min_spend": 2}, "done": False})
        set_job_state(pb, "bid_reviews", json.dumps(reviews))
        watch = json.loads(get_job_state(pb, "change_watch") or "[]")
        watch.append({"label": LABEL, "since": date.today().isoformat(), "campaign_ids": [sb["campaign_id"], sp["campaign_id"]]})
        set_job_state(pb, "change_watch", json.dumps(watch))
        print("bid review 2026-11-30 + change_watch added")


if __name__ == "__main__":
    main()
