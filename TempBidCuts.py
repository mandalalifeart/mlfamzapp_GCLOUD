"""Temporary bid cuts for good targets that suddenly "go crazy" (2026-10-24, per
the user: "if a good product/keyword goes crazy, then lower bid or pause for
short time, restore it and check again").

Part of the Monday review (WeeklyChangesReview) - it only PROPOSES, never
applies by itself; the user approves on Telegram and Claude runs this file.

1. Spike detection (spike_section): a keyword/target with a GOOD history -
   >= 3 orders and ACOS <= 40% in the 60 days ending 14 days ago (matured
   attribution) - whose last 7 days look crazy: spend >= 15 (scaled for
   SEK/PLN/MXN/CAD) and >= 2x its usual weekly spend, and no sales or ACOS
   >= max(60%, 2x its history ACOS). Skipped: targets we changed in the last
   14 days (let our own changes settle), active temporary cuts, non-ENABLED
   campaigns, Amazon theme targets, SD, pareo in winter mode.
   Proposal = temporary bid cut -25% (floor SP 0.10 / SB 0.25), restore in 14
   days. Proposals are kept in job_state "temp_cut_proposals".
2. Follow-up (followup_section) over job_state "temp_cuts":
   - active cut, 14 days passed -> propose restoring the old bid (shows the
     spike week vs the time since the cut);
   - restored, 14 days later -> re-check: crazy again (same rule) = propose a
     permanent cut; otherwise closed as OK.

CLI (run by Claude after the user's OK):
  python TempBidCuts.py apply all|<target_id,...>    apply proposed temporary cuts
  python TempBidCuts.py restore due|<target_id,...>  restore old bids
  python TempBidCuts.py permanent <target_id,...>    keep the cut (close the entry)
Every write is logged in ads_bid_change_log.
"""
import json
import sys
from collections import defaultdict
from datetime import date, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state, set_job_state

CUR = {"US": "$", "UK": "£", "CA": "C$", "MX": "MX$", "SE": "kr ", "PL": "zł "}
SCALE = {"SE": 11, "PL": 4.3, "MX": 20, "CA": 1.4}
PAREO_WORDS = ("PAREO", "SARONG", "BEACH")
PAREO_RESUME = "2027-02-20"
KEYWORD_TYPES = {"BROAD", "PHRASE", "EXACT"}
CUT_PCT = 25
RESTORE_DAYS = 14
RECHECK_DAYS = 14
MIN_BID = {"SPONSORED_PRODUCTS": 0.10, "SPONSORED_BRANDS": 0.25}


def _agg(rows):
    t = {"spend": 0.0, "sales": 0.0, "orders": 0, "clicks": 0}
    for r in rows:
        for k in t:
            t[k] += r.get(k) or 0
    return t


def _acos(t):
    return 100 * t["spend"] / t["sales"] if t["sales"] else None


def _fmt(t, cur):
    a = _acos(t)
    return (f"{cur}{t['spend']:.0f} spend / {cur}{t['sales']:.0f} sales, {t['orders']} orders, "
            f"ACOS {'∞' if a is None and t['spend'] else '-' if a is None else f'{a:.0f}%'}")


def _load_targets(pb, start, end):
    rows = pb_list_all(pb, "ads_keyword_stats", f'date >= "{start}" && date < "{end}"',
                       fields="profile_id,target_id,target_text,target_type,match_type,ad_product,campaign_id,"
                              "campaign_name,ad_group_id,country_code,date,spend,sales,orders,clicks,bid")
    best = {}
    for r in rows:
        k = (r["target_id"], r["date"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    by_t = defaultdict(list)
    for r in best.values():
        by_t[r["target_id"]].append(r)
    return by_t


def is_crazy(hist, recent, cc):
    """Good history + crazy last 7 days (see module doc)."""
    if hist["orders"] < 3:
        return False
    h_acos = _acos(hist)
    if h_acos is None or h_acos > 40:
        return False
    weekly = hist["spend"] / 60 * 7
    if recent["spend"] < 15 * SCALE.get(cc, 1) or recent["spend"] < 2 * weekly:
        return False
    r_acos = _acos(recent)
    return r_acos is None or r_acos >= max(60, 2 * h_acos)


def spike_section(pb, today, save=True):
    h0 = (today - timedelta(days=74)).isoformat()
    h1 = (today - timedelta(days=14)).isoformat()
    r0 = (today - timedelta(days=7)).isoformat()
    end = today.isoformat()
    by_t = _load_targets(pb, h0, end)
    camps = {c["campaign_id"]: c for c in pb_list_all(pb, "ads_campaigns", 'campaign_id != ""',
                                                       fields="campaign_id,campaign_status,portfolio_id")}
    ports = {p["portfolio_id"]: p["name"] for p in pb_list_all(pb, "ads_portfolios", 'portfolio_id != ""',
                                                               fields="portfolio_id,name")}
    recent_changes = {c["target_id"] for c in pb_list_all(
        pb, "ads_bid_change_log", f'status = "applied" && changed_at >= "{h1}"', fields="target_id")}
    active = {c["target_id"] for c in json.loads(get_job_state(pb, "temp_cuts") or "[]") if c.get("status") == "active"}
    winter = end < PAREO_RESUME
    out = []
    for tid, rows in by_t.items():
        last = max(rows, key=lambda r: r["date"])
        if last.get("ad_product") not in MIN_BID or last.get("target_type") == "THEME":
            continue
        if tid in recent_changes or tid in active:
            continue
        c = camps.get(last["campaign_id"])
        if not c or c.get("campaign_status") != "ENABLED":
            continue
        text = f"{ports.get(c.get('portfolio_id'), '')} {last['campaign_name']}".upper()
        if winter and any(w in text for w in PAREO_WORDS):
            continue
        hist = _agg([r for r in rows if h0 <= r["date"] < h1])
        recent = _agg([r for r in rows if r["date"] >= r0])
        if not is_crazy(hist, recent, last["country_code"]):
            continue
        bid = next((r.get("bid") for r in sorted(rows, key=lambda r: r["date"], reverse=True) if r.get("bid")), None)
        if not bid:
            continue
        new = max(MIN_BID[last["ad_product"]], round(bid * (100 - CUT_PCT) / 100, 2))
        if new >= bid:
            continue
        out.append({"target_id": tid, "profile_id": last["profile_id"], "campaign_id": last["campaign_id"],
                    "campaign": last["campaign_name"], "ad_group_id": last.get("ad_group_id") or "",
                    "ad_product": last["ad_product"], "is_keyword": last.get("target_type") in KEYWORD_TYPES,
                    "text": f"{last.get('target_text') or tid} ({(last.get('match_type') or '').lower()[:6]})",
                    "country": last["country_code"], "old_bid": bid, "new_bid": new,
                    "hist": hist, "recent": recent})
    out.sort(key=lambda x: -x["recent"]["spend"])
    if save:
        set_job_state(pb, "temp_cut_proposals", json.dumps({"date": end, "items": out}))
    lines = [f"\n🌡 Good targets that went crazy last week (good 60-day history, last 7 days ≥2x usual spend and no "
             f"sales / ACOS ≥2x): {len(out)}"]
    for i, x in enumerate(out[:15], 1):
        cur = CUR.get(x["country"], "€")
        h = _acos(x["hist"])
        lines.append(f"{i}. {x['country']} {x['campaign']} | {x['text']}\n"
                     f"   usual: {h:.0f}% ACOS, ~{cur}{x['hist']['spend'] / 60 * 7:.0f}/week · "
                     f"last 7d: {_fmt(x['recent'], cur)}\n"
                     f"   ➜ temporary cut {cur}{x['old_bid']:.2f} → {cur}{x['new_bid']:.2f} (-{CUT_PCT}%), "
                     f"restore in {RESTORE_DAYS} days")
    if out:
        lines.append("Reply \"apply temp cuts\" (or the numbers) to apply. Last 7 days' sales can still grow "
                     "(7-day attribution).")
    else:
        lines.append("none 👍")
    return lines, out


def followup_section(pb, today):
    cuts = json.loads(get_job_state(pb, "temp_cuts") or "[]")
    end = today.isoformat()
    due_restore = [c for c in cuts if c.get("status") == "active" and c["restore_on"] <= end]
    due_check = [c for c in cuts if c.get("status") == "restored" and c.get("recheck_on", "9999") <= end]
    if not due_restore and not due_check:
        return [], {"restore": [], "recheck": []}
    h0 = (today - timedelta(days=90)).isoformat()
    by_t = _load_targets(pb, h0, end)
    lines = []
    if due_restore:
        lines.append(f"\n⏪ Temporary cuts due to restore: {len(due_restore)}")
        for c in due_restore:
            rows = by_t.get(c["target_id"], [])
            since = _agg([r for r in rows if r["date"] >= c["cut_date"]])
            cur = CUR.get(c["country"], "€")
            lines.append(f"• {c['country']} {c['campaign']} | {c['text']}: cut {c['cut_date']} "
                         f"{cur}{c['old_bid']:.2f}→{cur}{c['new_bid']:.2f}\n"
                         f"   spike week: {_fmt(c['spike'], cur)}\n   since cut: {_fmt(since, cur)}\n"
                         f"   ➜ restore {cur}{c['old_bid']:.2f}, re-check in {RECHECK_DAYS} days")
        lines.append("Reply \"restore temp cuts\" to restore.")
    verdicts = []
    if due_check:
        lines.append(f"\n🔁 Re-check after restore: {len(due_check)}")
        for c in due_check:
            rows = by_t.get(c["target_id"], [])
            h1 = (date.fromisoformat(c["cut_date"]) - timedelta(days=7)).isoformat()
            hist = _agg([r for r in rows if (date.fromisoformat(h1) - timedelta(days=60)).isoformat() <= r["date"] < h1])
            after = _agg([r for r in rows if r["date"] >= c["restored_on"]])
            last7 = _agg([r for r in rows if r["date"] >= (today - timedelta(days=7)).isoformat()])
            again = is_crazy(hist, last7, c["country"])
            cur = CUR.get(c["country"], "€")
            verdicts.append({"target_id": c["target_id"], "again": again})
            lines.append(f"• {c['country']} {c['campaign']} | {c['text']}: since restore {_fmt(after, cur)} → "
                         + ("🔴 crazy again - propose a PERMANENT cut to "
                            f"{cur}{c['new_bid']:.2f} (reply \"permanent\")" if again else "🟢 back to normal, closed"))
            if not again:
                c["status"] = "closed_ok"
        set_job_state(pb, "temp_cuts", json.dumps(cuts))
    return lines, {"restore": [c["target_id"] for c in due_restore], "recheck": verdicts}


# ---------------- CLI (run after the user's OK) ----------------

def _write(pb, item, bid, reason):
    from AdsBidWriter import find_profile_connection, record_bid_change, refresh_access_token, write_bid_or_state
    conn = find_profile_connection(pb, item["profile_id"])
    tok = refresh_access_token(conn["profile_key"], conn["refresh_token"])
    resp = write_bid_or_state(conn, tok, item["profile_id"], item["ad_product"], item["is_keyword"], item["target_id"],
                              item["campaign_id"], item["ad_group_id"], bid)
    record_bid_change(pb, {"action": "bid", "ad_product": item["ad_product"], "campaign_id": item["campaign_id"],
                           "campaign_name": item["campaign"], "changed_at": date.today().isoformat(),
                           "country_code": item["country"], "profile_id": item["profile_id"],
                           "target_id": item["target_id"], "target_text": item["text"],
                           "old_bid": item["new_bid"] if bid == item["old_bid"] else item["old_bid"], "new_bid": bid,
                           "reason": reason, "status": "applied", "amazon_response": json.dumps(resp)[:5000]})
    return resp


def main(argv):
    from AdsBidWriter import pb_authenticate as ads_pb_auth
    pb = ads_pb_auth()
    cmd, which = argv[1], (argv[2] if len(argv) > 2 else "")
    ids = None if which in ("all", "due", "") else set(which.split(","))
    cuts = json.loads(get_job_state(pb, "temp_cuts") or "[]")
    today = date.today()
    if cmd == "apply":
        prop = json.loads(get_job_state(pb, "temp_cut_proposals") or "{}")
        for x in prop.get("items", []):
            if ids and x["target_id"] not in ids:
                continue
            if any(c["target_id"] == x["target_id"] and c["status"] == "active" for c in cuts):
                continue
            print("cut", x["country"], x["campaign"], x["text"], x["old_bid"], "->", x["new_bid"])
            _write(pb, x, x["new_bid"], f"Temporary cut -{CUT_PCT}%: good history, crazy week to {prop['date']} "
                                        f"(user rule 2026-10-24), restore in {RESTORE_DAYS} days")
            cuts.append({**{k: x[k] for k in ("target_id", "profile_id", "campaign_id", "campaign", "ad_group_id",
                                              "ad_product", "is_keyword", "text", "country", "old_bid", "new_bid")},
                         "spike": x["recent"], "cut_date": today.isoformat(),
                         "restore_on": (today + timedelta(days=RESTORE_DAYS)).isoformat(), "status": "active"})
    elif cmd == "restore":
        for c in cuts:
            if c["status"] != "active" or (ids and c["target_id"] not in ids) or (not ids and c["restore_on"] > today.isoformat()):
                continue
            print("restore", c["country"], c["campaign"], c["text"], c["new_bid"], "->", c["old_bid"])
            _write(pb, c, c["old_bid"], f"Restore after temporary cut of {c['cut_date']} (user rule 2026-10-24), "
                                        f"re-check in {RECHECK_DAYS} days")
            c.update(status="restored", restored_on=today.isoformat(),
                     recheck_on=(today + timedelta(days=RECHECK_DAYS)).isoformat())
    elif cmd == "permanent":
        for c in cuts:
            if c["status"] == "restored" and (not ids or c["target_id"] in ids):
                print("permanent cut", c["country"], c["campaign"], c["text"], c["old_bid"], "->", c["new_bid"])
                _write(pb, c, c["new_bid"], "Permanent cut: went crazy again after the restore (user rule 2026-10-24)")
                c["status"] = "permanent"
    else:
        sys.exit(__doc__)
    set_job_state(pb, "temp_cuts", json.dumps(cuts))


if __name__ == "__main__":
    main(sys.argv)
