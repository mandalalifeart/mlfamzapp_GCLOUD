"""Products per campaign that get many clicks but sell badly (2026-11-02, per
the user, formula OK'd): every 2nd Monday (even ISO weeks, same as the
search-term harvest) inside the weekly changes review. Proposals only - the
user approves on Telegram and Claude runs this file.

Find: every SP product ad (campaign x ad group x ASIN, profile-deduped
ads_advertised_product_stats), last 14 days ending yesterday. Flagged when
clicks >= 20 and (0 orders, or ACOS > 2x the department target: home decor
35% -> 70%, pareo 30% -> 60%).
History = the same product ad in the 90 days before that window:
  good (>= 3 orders and ACOS <= target + 10)  -> pause 14 days, then unpause
  bad  (ACOS > 60%, or spend >= 20 and 0 orders) -> pause 60 days
  else (new / not enough data)                -> left alone this round
After an unpause the ad is re-checked 14 days later: flagged again ->
a good one gets the 60-day pause, a bad one goes to the ignore list (stays
paused, never proposed again). Skipped: non-ENABLED campaigns, ads in the
ignore list or already paused by us, pareo campaigns in winter mode.
State: job_state product_ad_pause_proposals / product_ad_pauses /
product_ad_ignore. Writes go through AdsBidWriter's product-ad lookup
(client-side ASIN match) and are logged in ads_bid_change_log.

CLI (after the user's OK):
  python ProductAdPauses.py preview                build and save proposals now
  python ProductAdPauses.py pause all|<n,...>      pause proposals (numbers as listed)
  python ProductAdPauses.py unpause due|<key,...>  unpause ads whose pause ended
  python ProductAdPauses.py ignore <key,...>       keep paused for good
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
TARGET = {"pareo": 30, "home": 35}
MIN_CLICKS = 20
WINDOW = 14
HISTORY = 90
PAUSE_DAYS = {"good": 14, "bad": 60}
RECHECK_DAYS = 14


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
    return (f"{t['clicks']} clicks, {cur}{t['spend']:.0f} spend, {t['orders']} orders, "
            f"ACOS {'∞' if a is None and t['spend'] else '-' if a is None else f'{a:.0f}%'}")


def _key(r):
    return f"{r['campaign_id']}|{r['ad_group_id']}|{r['asin']}"


def _load(pb, start, end):
    rows = pb_list_all(pb, "ads_advertised_product_stats",
                       f'ad_product = "SPONSORED_PRODUCTS" && date >= "{start}" && date < "{end}"',
                       fields="profile_id,campaign_id,campaign_name,ad_group_id,ad_group_name,asin,sku,country_code,"
                              "date,clicks,spend,sales,orders")
    best = {}
    for r in rows:
        if not r.get("asin") or not r.get("ad_group_id"):
            continue
        k = (_key(r), r["date"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    by = defaultdict(list)
    for r in best.values():
        by[_key(r)].append(r)
    return by


def _dept(pb):
    camps = {c["campaign_id"]: c for c in pb_list_all(pb, "ads_campaigns", 'campaign_id != ""',
                                                       fields="campaign_id,campaign_name,campaign_status,portfolio_id")}
    ports = {p["portfolio_id"]: p["name"] for p in pb_list_all(pb, "ads_portfolios", 'portfolio_id != ""',
                                                               fields="portfolio_id,name")}

    def dept(cid, name):
        c = camps.get(cid) or {}
        text = f"{ports.get(c.get('portfolio_id'), '')} {name}".upper()
        return "pareo" if any(w in text for w in PAREO_WORDS) else "home"
    return camps, dept


def flagged(recent, dept):
    if recent["clicks"] < MIN_CLICKS:
        return False
    a = _acos(recent)
    return recent["orders"] == 0 or a is None or a > 2 * TARGET[dept]


def history_class(hist, dept, cc):
    a = _acos(hist)
    if hist["orders"] >= 3 and a is not None and a <= TARGET[dept] + 10:
        return "good"
    if (a is not None and a > 60) or (hist["orders"] == 0 and hist["spend"] >= 20 * SCALE.get(cc, 1)):
        return "bad"
    return "new"


def _windows(today):
    end = (today - timedelta(days=1)).isoformat()  # data through yesterday
    w0 = (today - timedelta(days=1 + WINDOW)).isoformat()
    h0 = (today - timedelta(days=1 + WINDOW + HISTORY)).isoformat()
    return h0, w0, end


def pause_section(pb, today, save=True):
    h0, w0, end = _windows(today)
    by = _load(pb, h0, (today).isoformat())
    camps, dept_of = _dept(pb)
    pauses = json.loads(get_job_state(pb, "product_ad_pauses") or "[]")
    ignore = set(json.loads(get_job_state(pb, "product_ad_ignore") or "[]"))
    active = {p["key"] for p in pauses if p["status"] == "active"}
    winter = today.isoformat() < PAREO_RESUME
    out, new_count = [], 0
    for k, rows in by.items():
        last = max(rows, key=lambda r: r["date"])
        c = camps.get(last["campaign_id"])
        if not c or c.get("campaign_status") != "ENABLED" or k in ignore or k in active:
            continue
        dept = dept_of(last["campaign_id"], last["campaign_name"])
        if winter and dept == "pareo":
            continue
        recent = _agg([r for r in rows if w0 <= r["date"] <= end])
        if not flagged(recent, dept):
            continue
        hist = _agg([r for r in rows if h0 <= r["date"] < w0])
        cls = history_class(hist, dept, last["country_code"])
        if cls == "new":
            new_count += 1
            continue
        out.append({"key": k, "profile_id": last["profile_id"], "campaign_id": last["campaign_id"],
                    "campaign": last["campaign_name"], "ad_group_id": last["ad_group_id"],
                    "ad_group": last.get("ad_group_name") or "", "asin": last["asin"], "sku": last.get("sku") or "",
                    "country": last["country_code"], "dept": dept, "history": cls, "recent": recent, "hist": hist,
                    "pause_days": PAUSE_DAYS[cls]})
    out.sort(key=lambda x: -x["recent"]["spend"])
    if save:
        set_job_state(pb, "product_ad_pause_proposals", json.dumps({"date": today.isoformat(), "items": out}))
    lines = [f"\n⏸ Products with many clicks and bad sales (last {WINDOW} days to {end}, ≥{MIN_CLICKS} clicks, "
             f"0 orders or ACOS > 2x target): {len(out)}"]
    for i, x in enumerate(out[:30], 1):
        cur = CUR.get(x["country"], "€")
        icon = "🟢 good history" if x["history"] == "good" else "🔴 bad history"
        lines.append(f"{i}. {x['country']} {x['campaign']} | {x['sku'] or x['asin']}\n"
                     f"   last {WINDOW}d: {_fmt(x['recent'], cur)}\n"
                     f"   90d before: {_fmt(x['hist'], cur)} → {icon}\n"
                     f"   ➜ pause {x['pause_days']} days")
    if out:
        lines.append("Reply \"pause products\" (or the numbers) to pause. Last days' sales can still grow "
                     "(7-day attribution).")
    else:
        lines.append("none 👍")
    if new_count:
        lines.append(f"({new_count} more flagged with too little history - left alone, checked again next round)")
    return lines, out


def followup_section(pb, today):
    pauses = json.loads(get_job_state(pb, "product_ad_pauses") or "[]")
    t = today.isoformat()
    due_unpause = [p for p in pauses if p["status"] == "active" and p["unpause_on"] <= t]
    due_check = [p for p in pauses if p["status"] == "unpaused" and p.get("recheck_on", "9999") <= t]
    if not due_unpause and not due_check:
        return [], {"unpause": [], "recheck": []}
    lines = []
    if due_unpause:
        lines.append(f"\n▶️ Paused products due to come back: {len(due_unpause)}")
        for p in due_unpause:
            lines.append(f"• {p['country']} {p['campaign']} | {p['sku'] or p['asin']} "
                         f"({p['history']} history, paused {p['paused_on']})")
        lines.append("Reply \"unpause products\" to unpause; re-check 14 days later.")
    verdicts = []
    if due_check:
        h0, w0, end = _windows(today)
        by = _load(pb, w0, t)
        _, dept_of = _dept(pb)
        ignore = json.loads(get_job_state(pb, "product_ad_ignore") or "[]")
        lines.append(f"\n🔁 Re-check after unpause: {len(due_check)}")
        for p in due_check:
            recent = _agg([r for r in by.get(p["key"], []) if r["date"] >= p["unpaused_on"]])
            again = flagged(recent, dept_of(p["campaign_id"], p["campaign"]))
            cur = CUR.get(p["country"], "€")
            if not again:
                p["status"] = "closed_ok"
                verdict = "🟢 fine now, closed"
            elif p["history"] == "good":
                p["status"] = "repause_proposed"
                verdict = "🔴 bad again → propose a 60-day pause (reply \"pause products\")"
            else:
                p["status"] = "ignore_proposed"
                verdict = "🔴 bad again → propose the ignore list (stays paused; reply \"ignore\")"
            verdicts.append({"key": p["key"], "again": again})
            lines.append(f"• {p['country']} {p['campaign']} | {p['sku'] or p['asin']}: since unpause "
                         f"{_fmt(recent, cur)} → {verdict}")
        set_job_state(pb, "product_ad_pauses", json.dumps(pauses))
        set_job_state(pb, "product_ad_ignore", json.dumps(ignore))
    return lines, {"unpause": [p["key"] for p in due_unpause], "recheck": verdicts}


# ---------------- CLI (run after the user's OK) ----------------

def _set_state(pb, item, state, reason):
    from AdsBidWriter import (find_profile_connection, find_sp_product_ad_id, record_bid_change,
                              refresh_access_token, update_sp_product_ad_state)
    conn = find_profile_connection(pb, item["profile_id"])
    tok = refresh_access_token(conn["profile_key"], conn["refresh_token"])
    ad_id = item.get("ad_id") or find_sp_product_ad_id(conn["base_url"], tok, conn["client_id"], item["profile_id"],
                                                       item["campaign_id"], item["ad_group_id"], item["asin"])
    resp = update_sp_product_ad_state(conn["base_url"], tok, conn["client_id"], item["profile_id"], ad_id, state)
    record_bid_change(pb, {"action": "product_ad_state", "ad_product": "SPONSORED_PRODUCTS",
                           "campaign_id": item["campaign_id"], "campaign_name": item["campaign"],
                           "changed_at": date.today().isoformat(), "country_code": item["country"],
                           "profile_id": item["profile_id"], "target_id": f"productad:{ad_id}",
                           "target_text": f"product ad {item['sku'] or item['asin']} -> {state}",
                           "old_bid": 0, "new_bid": 0, "reason": reason, "status": "applied",
                           "amazon_response": json.dumps(resp)[:5000]})
    return ad_id


def main(argv):
    pb = pb_authenticate()
    cmd, which = argv[1], (argv[2] if len(argv) > 2 else "")
    pauses = json.loads(get_job_state(pb, "product_ad_pauses") or "[]")
    ignore = json.loads(get_job_state(pb, "product_ad_ignore") or "[]")
    today = date.today()
    if cmd == "preview":  # build + save proposals now (outside the Monday run)
        lines, _ = pause_section(pb, today, save=True)
        print("\n".join(lines))
        return
    if cmd == "pause":
        prop = json.loads(get_job_state(pb, "product_ad_pause_proposals") or "{}")
        items = list(prop.get("items", []))
        # re-pause proposals from re-checks (good history that went bad again)
        items += [{**p, "history": "bad", "pause_days": PAUSE_DAYS["bad"]} for p in pauses
                  if p["status"] == "repause_proposed"]
        nums = None if which in ("all", "") else {int(n) for n in which.split(",")}
        for i, x in enumerate(items, 1):
            if nums and i not in nums:
                continue
            days = x["pause_days"]
            print("pause", x["country"], x["campaign"], x["sku"] or x["asin"], f"{days}d")
            ad_id = _set_state(pb, x, "PAUSED", f"Product pause {days}d: many clicks, bad sales, {x['history']} "
                                                f"history (user rule 2026-11-02)")
            pauses = [p for p in pauses if p["key"] != x["key"]]
            pauses.append({**{k: x[k] for k in ("key", "profile_id", "campaign_id", "campaign", "ad_group_id", "asin",
                                                "sku", "country", "history")},
                           "ad_id": ad_id, "paused_on": today.isoformat(),
                           "unpause_on": (today + timedelta(days=days)).isoformat(), "status": "active"})
    elif cmd == "unpause":
        keys = None if which in ("due", "") else set(which.split(","))
        for p in pauses:
            if p["status"] != "active" or (keys and p["key"] not in keys) or \
                    (not keys and p["unpause_on"] > today.isoformat()):
                continue
            print("unpause", p["country"], p["campaign"], p["sku"] or p["asin"])
            _set_state(pb, p, "ENABLED", f"Unpause after product pause of {p['paused_on']} (user rule 2026-11-02)")
            p.update(status="unpaused", unpaused_on=today.isoformat(),
                     recheck_on=(today + timedelta(days=RECHECK_DAYS)).isoformat())
    elif cmd == "ignore":
        keys = set(which.split(",")) if which not in ("", "proposed") else \
            {p["key"] for p in pauses if p["status"] == "ignore_proposed"}
        for p in pauses:
            if p["key"] in keys:
                if p["status"] != "active":
                    _set_state(pb, p, "PAUSED", "Ignore list: bad again after unpause (user rule 2026-11-02)")
                p["status"] = "ignored"
                print("ignore", p["country"], p["campaign"], p["sku"] or p["asin"])
        ignore = sorted(set(ignore) | keys)
    else:
        sys.exit(__doc__)
    set_job_state(pb, "product_ad_pauses", json.dumps(pauses))
    set_job_state(pb, "product_ad_ignore", json.dumps(ignore))


if __name__ == "__main__":
    main(sys.argv)
