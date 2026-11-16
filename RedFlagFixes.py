"""Fix proposals for 🔴 campaigns in the weekly ads review (per the user
2026-11-16: "yes" to "every 🔴 campaign gets a ready proposal with a checkbox").

A campaign is 🔴 when last week (7 days) spent >= 15 (x SCALE) with no sales or
ACOS >= 70% (WeeklyChangesReview.flags). Rule (history before any cut, memory
amzbot-ads-history-before-pause):
- good history = >= 3 orders and ACOS <= the campaign's ACOS limit in the 60 days
  ending 14 days ago -> TEMPORARY bid cut -30% on its keywords/targets with
  >= 10 clicks and 0 orders in the last 14 days (not changed in 14 days, not
  below Amazon's minimum bid);
- no good history -> budget -50% (not below 1 x SCALE, not changed in 14 days).
Proposals go to check_reports (job "red-flag-fixes", weekly, shown on
/ads-reviews); ApplyApprovedProposals applies the approved ones (set_bids /
set_budgets). Never pauses, never raises."""
import json
from collections import defaultdict
from datetime import date, timedelta

from AdsGrowth import SCALE, Api, _campaigns, _dedupe, _recently_changed, acos_target
from AsinAnalytics import pb_list_all
from TargetTacos import MIN_BID

JOB = "red-flag-fixes"
LABEL = "🔴 Red flag fixes"
CUT = 0.30
BUDGET_CUT = 0.50
MIN_CLICKS = 10


def _agg(rows, key):
    out = defaultdict(lambda: {"clicks": 0, "orders": 0, "spend": 0.0, "sales": 0.0})
    for r in rows:
        a = out[key(r)]
        for k in ("clicks", "orders", "spend", "sales"):
            a[k] += r.get(k) or 0
    return out


def build(pb, today, red_ids, api=None):
    """red_ids: campaign ids flagged 🔴 last week. -> (lines, proposals)."""
    api = api or Api(pb)
    camps = _campaigns(pb)
    changed = _recently_changed(pb, today)
    h_end, h_start = today - timedelta(days=14), today - timedelta(days=74)
    k_start = today - timedelta(days=14)
    ids = [c for c in red_ids if c in camps]
    if not ids:
        return [], []
    flt = " || ".join(f'campaign_id = "{c}"' for c in ids)
    camp_rows = _dedupe(pb_list_all(pb, "ads_campaign_stats", f'({flt}) && date >= "{h_start}" && date < "{h_end}"',
                                    fields="profile_id,campaign_id,date,clicks,orders,spend,sales"),
                        lambda r: (r["campaign_id"], r["date"]))
    hist = _agg(camp_rows, lambda r: r["campaign_id"])
    kw_rows = _dedupe(pb_list_all(pb, "ads_keyword_stats", f'({flt}) && date >= "{k_start}" && date < "{today}"',
                                  fields="profile_id,campaign_id,target_id,target_text,match_type,date,clicks,orders,spend,sales"),
                      lambda r: (r["campaign_id"], r["target_id"], r["date"]))
    kws = _agg(kw_rows, lambda r: (r["campaign_id"], r["target_id"]))
    texts = {(r["campaign_id"], r["target_id"]): (r.get("target_text") or "", r.get("match_type") or "") for r in kw_rows}
    budgets = {c["campaign_id"]: c for c in pb_list_all(pb, "ads_campaigns", flt, fields="campaign_id,profile_id,budget")}
    lines, proposals = [f"\n🔴 Fix proposals for red-flag campaigns (approve on /ads-reviews): good history → bids -{CUT:.0%} on "
                        f"keywords with ≥{MIN_CLICKS} clicks and 0 orders in 14 days (temporary); no good history → budget -{BUDGET_CUT:.0%}"], []
    for cid in ids:
        c = camps[cid]
        cc = (c.get("country_code") or "").upper()
        h = hist.get(cid, {"orders": 0, "spend": 0, "sales": 0})
        limit = acos_target(pb, cc, c["seg"], c["ad_product"])
        h_acos = 100 * h["spend"] / h["sales"] if h["sales"] else None
        good = h["orders"] >= 3 and h_acos is not None and h_acos <= limit
        title_base = f"{cc} {c['campaign_name']}"
        hist_txt = f"60 days before: {h['orders']:.0f} orders, ACOS {'-' if h_acos is None else f'{h_acos:.0f}%'} (limit {limit:.0f}%)"
        if good:
            try:
                live = api.live_targets(c["profile_id"], cid, c["ad_product"])
            except Exception as exc:
                lines.append(f"• {title_base}: live bids failed ({exc})"); continue
            floor = MIN_BID[c["ad_product"]].get(cc, 0.10 if c["ad_product"] == "SPONSORED_BRANDS" else 0.02)
            acts, rows = [], []
            for (k_cid, tid), a in sorted(kws.items(), key=lambda kv: -kv[1]["spend"]):
                if k_cid != cid or a["orders"] or a["clicks"] < MIN_CLICKS or tid in changed:
                    continue
                lv = live.get(tid)
                if not lv or not lv.get("bid"):
                    continue
                new = round(lv["bid"] * (1 - CUT), 2)
                if new < floor:
                    continue
                text, match = texts.get((cid, tid), ("", ""))
                acts.append({"profile_id": c["profile_id"], "ad_product": c["ad_product"], "is_keyword": lv["is_keyword"],
                             "target_id": tid, "campaign_id": cid, "ad_group_id": lv["ad_group_id"], "campaign": c["campaign_name"],
                             "text": text, "country": cc, "old_bid": lv["bid"], "new_bid": new,
                             "reason": f"red flag fix (temporary, good history): {a['clicks']:.0f} clicks, 0 orders in 14 days"})
                rows.append([text[:60], (match or "").lower(), round(a["clicks"]), round(a["spend"], 2), lv["bid"], new])
            if acts:
                proposals.append({"status": "proposed", "section": "Red flag fixes", "title": f"{title_base}: bids -{CUT:.0%} on {len(acts)} keyword(s)",
                                  "why": f"🔴 last week; {hist_txt} → temporary cut, not a pause",
                                  "columns": ["Keyword / target", "Match", "Clicks 14d", "Spend 14d", "Bid now", "New bid"],
                                  "rows": rows, "rowIds": [x["target_id"] for x in acts],
                                  "detail": "; ".join(f"{x['text'][:40]} {x['old_bid']}→{x['new_bid']}" for x in acts[:10]),
                                  "action": {"type": "set_bids", "items": acts}})
                lines.append(f"• {title_base}: bids -{CUT:.0%} on {len(acts)} keyword(s) ({hist_txt})")
            else:
                lines.append(f"• {title_base}: good history but no keyword to cut (all changed in 14 days or few clicks) ({hist_txt})")
        else:
            b = budgets.get(cid) or {}
            old = b.get("budget")
            if not old:
                lines.append(f"• {title_base}: no budget found"); continue
            if cid in changed:
                lines.append(f"• {title_base}: budget changed in the last 14 days - left ({hist_txt})"); continue
            new = max(round(old * (1 - BUDGET_CUT), 2), 1.0 * SCALE.get(cc, 1))
            if new >= old:
                lines.append(f"• {title_base}: budget {old:g} already at the minimum ({hist_txt})"); continue
            item = {"profile_id": b.get("profile_id") or c["profile_id"], "campaign_id": cid, "campaign": c["campaign_name"],
                    "ad_product": c["ad_product"], "country": cc, "old": old, "new": new,
                    "rule": "red flag fix", "why": f"🔴 last week, {hist_txt}"}
            proposals.append({"status": "proposed", "section": "Red flag fixes", "title": f"{title_base}: budget {old:g} → {new:g}",
                              "why": f"🔴 last week; {hist_txt} → budget -{BUDGET_CUT:.0%}",
                              "detail": item["why"], "action": {"type": "set_budgets", "items": [item]}})
            lines.append(f"• {title_base}: budget {old:g} → {new:g} ({hist_txt})")
    return lines, proposals


def save(pb, today, text, proposals):
    from CheckReports import save_report
    save_report("weekly", today.isoformat(), JOB, LABEL, text, token=pb, proposals=proposals)
