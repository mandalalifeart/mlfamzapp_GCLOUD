"""Weekly "is anything going crazy" review of our Ads changes (2026-10-08, per
the user: "review all our changes on weekly bases to see nothing is going
crazy").

Section 1 - watched changes: groups of campaigns we created/changed, from
job_state key "change_watch" = [{"label", "since", "campaign_ids"}]. Per
campaign: last 7 days vs the 7 days before (or since launch), budget use, flags.
Section 2 - account-wide alerts: any other ENABLED campaign that got a red or
orange flag.

Flags (last 7 days, local currency):
  🔴 spend >= 15 and (no sales or ACOS >= 70%)
  🟠 spend >= 15 and >= 2x the previous 7 days (previous >= 3)
  🟡 spent >= 90% of the daily budget on 4+ of 7 days (budget-capped)
  ⚪ watched campaign live 5+ days with 0 impressions (not serving)
Section 3 - TACOS per marketplace (2026-10-18, per the user): ad spend / total
sales from country_sales, last full month + month to date, vs TACOS_TARGET
(USA/DE 20%, others 25%). TACOS has no attribution lag, unlike ACOS.
Section 4 - home decor vs pareo per marketplace (2026-10-18, per the user):
ACOS/TACOS per segment, last full month + month to date. Ad spend/sales from
ads_campaign_stats, segment by portfolio/campaign name (PAREO/SARONG/BEACH =
pareo, else home decor); total sales per ASIN from asin_traffic_monthly
(Business Reports) mapped through asin_group_mapping (PAREO* groups = pareo,
IGNORE skipped, unmapped ASINs left out).
Section 5 - lost exposure (2026-10-18, per the user: keywords that sold
before and now get no impressions): a target with >= 3 orders in the 60 days
ending 14 days ago whose last 7 days have < 25% of its usual weekly
impressions (usual weekly >= 50). Skipped: targets we paused ourselves (last
logged change old == new), non-ENABLED campaigns, and pareo until
PAREO_RESUME (winter mode). Shows the bid then/now and our logged changes.
Section 6 - temporary cuts (2026-10-24, per the user): good targets that went
crazy last week get a proposed -25% cut + restore/re-check follow-ups, see
TempBidCuts.py (proposals only; applied after the user's OK).
Non-dry runs are also saved to ads_reviews for the /ads-reviews page.
Reads only stored ads_campaign_stats / ads_campaigns / country_sales (morning cron pulls).
Args: dry=1, today=YYYY-MM-DD, group=<label substring>, since_launch=1.
"""
import json
import os
from collections import defaultdict
from datetime import date, timedelta

import requests

from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state
from NotificationRouting import notify
from AdsReviews import save_review

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
TELEGRAM_BOT_TOKEN = (os.environ.get("UPDATE_TELEGRAM_BOT_TOKEN") or os.environ.get("MCF_TELEGRAM_BOT_TOKEN", ""))
TELEGRAM_CHAT_ID = (os.environ.get("UPDATE_TELEGRAM_CHAT_ID") or os.environ.get("MCF_TELEGRAM_CHAT_ID", ""))
JOB_NAME = "weekly-changes-review"
CUR = {"US": "$", "UK": "£", "CA": "C$", "MX": "MX$", "SE": "kr ", "PL": "zł "}
# money thresholds are in EUR-ish units; scale for low-value currencies
SCALE = {"SE": 11, "PL": 4.3, "MX": 20, "CA": 1.4}
METRICS = ("impressions", "clicks", "spend", "sales", "orders")
TACOS_MARKETS = ("usa", "de", "uk", "fr", "it", "es", "nl", "se", "pl")
TACOS_TARGET = {"usa": 20, "de": 20}  # others: 25
SEGMENT_MARKETS = (("us", "USA"), ("de", "DE"), ("uk", "UK"), ("fr", "FR"), ("it", "IT"), ("es", "ES"))
PAREO_WORDS = ("PAREO", "SARONG", "BEACH")
PAREO_RESUME = "2027-02-20"  # pareo winter mode until the season review


def _json(body, status=200):
    return json.dumps(body), status, {"Access-Control-Allow-Origin": ALLOWED_ORIGIN, "Content-Type": "application/json"}


def _acos(t):
    return 100 * t["spend"] / t["sales"] if t["sales"] else None


def _fmt(t, cur):
    acos = _acos(t)
    a = f"{acos:.0f}%" if acos is not None else ("∞" if t["spend"] else "–")
    return f"{cur}{t['spend']:.2f} spend · {t['clicks']} clicks · {t['orders']} orders · {cur}{t['sales']:.2f} sales · ACOS {a}"


def load(pb, today, since_min):
    rows = pb_list_all(pb, "ads_campaign_stats", f'date >= "{since_min}" && date < "{today.isoformat()}"',
                       fields="campaign_id,campaign_name,country_code,profile_id,date," + ",".join(METRICS))
    best = {}  # same campaign can sit under two profiles - pick one, never sum
    for r in rows:
        k = (r["campaign_id"], r["date"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    by_camp = defaultdict(list)
    for r in best.values():
        by_camp[r["campaign_id"]].append(r)
    camps = {}
    for c in pb_list_all(pb, "ads_campaigns", 'campaign_status = "ENABLED" || campaign_status = "PAUSED"',
                         fields="campaign_id,campaign_name,country_code,campaign_status,budget"):
        camps.setdefault(c["campaign_id"], c)
    return by_camp, camps


def window(rows, start, end):
    t = dict.fromkeys(METRICS, 0)
    t["spend"] = t["sales"] = 0.0
    days = {}
    for r in rows:
        if start <= r["date"] < end:
            for m in METRICS:
                t[m] += r.get(m) or 0
            days[r["date"]] = days.get(r["date"], 0) + (r.get("spend") or 0)
    return t, days


def flags(cur7, prev7, days, budget, live_days, watched, cc=""):
    out = []
    k = SCALE.get(cc, 1)
    acos = _acos(cur7)
    if cur7["spend"] >= 15 * k and (not cur7["sales"] or acos >= 70):
        out.append("🔴 high spend, " + ("no sales" if not cur7["sales"] else f"ACOS {acos:.0f}%"))
    if cur7["spend"] >= 15 * k and prev7["spend"] >= 3 * k and cur7["spend"] >= 2 * prev7["spend"]:
        out.append(f"🟠 spend x{cur7['spend'] / prev7['spend']:.1f} vs last week")
    if budget and sum(1 for v in days.values() if v >= 0.9 * budget) >= 4:
        good = acos is not None and acos < 40
        out.append("🟡 budget-capped" + (" (good ACOS - raise budget?)" if good else ""))
    if watched and live_days >= 5 and cur7["impressions"] == 0:
        out.append("⚪ 0 impressions - not serving")
    return out


def tacos_section(pb, today):
    """Ad spend / total sales per marketplace (own currency, so the ratio is
    safe): last full month and month to date. The combined "eu" row is skipped."""
    cur_m = (today.year, today.month)
    prev_m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    rows = pb_list_all(pb, "country_sales",
                       f'((year = {cur_m[0]} && month = {cur_m[1]}) || (year = {prev_m[0]} && month = {prev_m[1]}))',
                       fields="marketplace,year,month,sales,ppc_spend")
    tot = defaultdict(lambda: [0.0, 0.0])
    for r in rows:
        t = tot[(r["marketplace"], (r["year"], r["month"]))]
        t[0] += r.get("sales") or 0
        t[1] += r.get("ppc_spend") or 0
    def cell(mp, ym):
        sales, spend = tot.get((mp, ym), (0, 0))
        return (spend / sales * 100) if sales else None
    lines = [f"\n📊 TACOS (ad spend / all sales) - {date(*prev_m, 1):%b} | {date(*cur_m, 1):%b} to date · target USA/DE 20%, others 25%"]
    data = []
    for mp in TACOS_MARKETS:
        a, b = cell(mp, prev_m), cell(mp, cur_m)
        if a is None and b is None:
            continue
        data.append({"market": mp.upper(), "prev": a, "cur": b, "target": TACOS_TARGET.get(mp, 25)})
        target = TACOS_TARGET.get(mp, 25)
        worst = max(x for x in (a, b) if x is not None)
        mark = "🔴" if worst > target * 1.25 else ("🟠" if worst > target else "🟢")
        fmt = lambda x: "-" if x is None else f"{x:.0f}%"
        lines.append(f"{mark} {mp.upper()}: {fmt(a)} | {fmt(b)}")
    return lines, data


def _months(today):
    cur_m = (today.year, today.month)
    prev_m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    return prev_m, cur_m


def segment_section(pb, today):
    """ACOS/TACOS for home decor vs pareo per marketplace, last month + MTD."""
    prev_m, cur_m = _months(today)
    start = date(*prev_m, 1).isoformat()
    groups = {m["asin"]: m["group"] for m in pb_list_all(pb, "asin_group_mapping", 'asin != ""', fields="asin,group")}
    ports = {p["portfolio_id"]: p["name"] for p in pb_list_all(pb, "ads_portfolios", 'portfolio_id != ""', fields="portfolio_id,name")}
    camp_port = {c["campaign_id"]: ports.get(c.get("portfolio_id"), "")
                 for c in pb_list_all(pb, "ads_campaigns", 'campaign_id != ""', fields="campaign_id,portfolio_id")}
    best = {}
    for r in pb_list_all(pb, "ads_campaign_stats", f'date >= "{start}" && date < "{today.isoformat()}"',
                         fields="campaign_id,campaign_name,country_code,profile_id,date,spend,sales"):
        k = (r["campaign_id"], r["date"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    ads = defaultdict(lambda: [0.0, 0.0])
    for r in best.values():
        text = f"{camp_port.get(r['campaign_id'], '')} {r['campaign_name']}".upper()
        seg = "pareo" if any(w in text for w in PAREO_WORDS) else "home"
        ym = (int(r["date"][:4]), int(r["date"][5:7]))
        a = ads[(r["country_code"].lower(), ym, seg)]
        a[0] += r.get("spend") or 0
        a[1] += r.get("sales") or 0
    totals = defaultdict(float)
    for r in pb_list_all(pb, "asin_traffic_monthly",
                         f'((year = {cur_m[0]} && month = {cur_m[1]}) || (year = {prev_m[0]} && month = {prev_m[1]}))',
                         fields="asin,marketplace,year,month,sales"):
        g = (groups.get(r["asin"]) or "").upper()
        if not g or g.startswith("IGNORE"):
            continue
        totals[(r["marketplace"], (r["year"], r["month"]), "pareo" if "PAREO" in g else "home")] += r.get("sales") or 0
    pct = lambda x: "-" if x is None else f"{x:.0f}%"  # noqa: E731
    lines = [f"\n🏷️ Home decor vs pareo - ACOS/TACOS {date(*prev_m, 1):%b} → {date(*cur_m, 1):%b} to date"]
    data = []
    for mp, label in SEGMENT_MARKETS:
        parts = []
        for seg, name in (("home", "home decor"), ("pareo", "pareo")):
            row = {"market": label, "segment": name}
            for key, ym in (("prev", prev_m), ("cur", cur_m)):
                spend, ad_sales = ads[(mp, ym, seg)]
                total = totals[(mp, ym, seg)]
                row[key] = {"spend": round(spend, 2), "adSales": round(ad_sales, 2), "sales": round(total, 2),
                            "acos": spend / ad_sales * 100 if ad_sales else None,
                            "tacos": spend / total * 100 if total else None}
            if not (row["prev"]["spend"] or row["cur"]["spend"] or row["prev"]["sales"] or row["cur"]["sales"]):
                continue
            data.append(row)
            parts.append(f"{name} {pct(row['prev']['acos'])}/{pct(row['prev']['tacos'])} → "
                         f"{pct(row['cur']['acos'])}/{pct(row['cur']['tacos'])}")
        if parts:
            lines.append(f"{label}: " + " | ".join(parts))
    lines.append("(ACOS/TACOS; this month's ACOS drops as late ad sales arrive)")
    return lines, data


def lost_exposure_section(pb, today):
    b0 = (today - timedelta(days=74)).isoformat()
    b1 = (today - timedelta(days=14)).isoformat()
    r0 = (today - timedelta(days=7)).isoformat()
    end = today.isoformat()
    rows = pb_list_all(pb, "ads_keyword_stats", f'date >= "{b0}" && date < "{end}"',
                       fields="profile_id,target_id,target_text,match_type,campaign_id,campaign_name,country_code,"
                              "currency_code,date,impressions,spend,sales,orders,bid")
    best = {}
    for r in rows:
        k = (r["target_id"], r["date"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    agg = defaultdict(lambda: {"bimp": 0, "borders": 0, "bsales": 0.0, "bspend": 0.0, "rimp": 0,
                               "bid_then": (None, ""), "bid_now": (None, "")})
    meta = {}
    for r in best.values():
        a = agg[r["target_id"]]
        meta[r["target_id"]] = r
        if b0 <= r["date"] < b1:
            a["bimp"] += r.get("impressions") or 0
            a["borders"] += r.get("orders") or 0
            a["bsales"] += r.get("sales") or 0
            a["bspend"] += r.get("spend") or 0
            if r.get("bid") and r["date"] >= a["bid_then"][1]:
                a["bid_then"] = (r["bid"], r["date"])
        elif r["date"] >= r0:
            a["rimp"] += r.get("impressions") or 0
            if r.get("bid") and r["date"] >= a["bid_now"][1]:
                a["bid_now"] = (r["bid"], r["date"])
    camps = {c["campaign_id"]: c for c in pb_list_all(pb, "ads_campaigns", 'campaign_id != ""',
                                                       fields="campaign_id,campaign_status,portfolio_id")}
    ports = {p["portfolio_id"]: p["name"] for p in pb_list_all(pb, "ads_portfolios", 'portfolio_id != ""', fields="portfolio_id,name")}
    changes = defaultdict(list)
    for c in pb_list_all(pb, "ads_bid_change_log", f'status = "applied" && changed_at >= "{b0}"',
                         fields="target_id,old_bid,new_bid,changed_at", sort="changed_at"):
        changes[c["target_id"]].append(c)
    winter = today.isoformat() < PAREO_RESUME
    out = []
    for tid, a in agg.items():
        weekly = a["bimp"] / (60 / 7)
        if a["borders"] < 3 or weekly < 50 or a["rimp"] >= 0.25 * weekly:
            continue
        m = meta[tid]
        c = camps.get(m["campaign_id"])
        if c and c.get("campaign_status") != "ENABLED":
            continue
        ch = changes.get(tid, [])
        if ch and ch[-1].get("old_bid") == ch[-1].get("new_bid"):
            continue  # we paused it ourselves
        text = f"{ports.get((c or {}).get('portfolio_id'), '')} {m['campaign_name']} {m.get('target_text') or ''}".upper()
        if winter and any(w in text for w in PAREO_WORDS):
            continue
        out.append({"country": m["country_code"], "campaign": m["campaign_name"], "target": m.get("target_text") or tid,
                    "match": m.get("match_type") or "", "orders": a["borders"], "sales": round(a["bsales"], 2),
                    "acos": a["bspend"] / a["bsales"] * 100 if a["bsales"] else None,
                    "weeklyImpr": round(weekly), "lastWeekImpr": a["rimp"],
                    "bidThen": a["bid_then"][0], "bidNow": a["bid_now"][0],
                    "changes": [f"{x['changed_at'][:10]} {x['old_bid']}→{x['new_bid']}" for x in ch]})
    out.sort(key=lambda x: -x["sales"])
    lines = [f"\n📉 Lost exposure - sold before (3+ orders {b0} – {b1}), now <25% of usual impressions: {len(out)}"
             + (" (pareo skipped until " + PAREO_RESUME + ")" if winter else "")]
    for x in out[:15]:
        cur = CUR.get(x["country"], "€")
        bid = lambda v: "paused/none" if v is None else f"{cur}{v:.2f}"  # noqa: E731
        lines.append(f"• {x['country']} {x['campaign']} | {x['target']} ({x['match'].lower()[:6]}): "
                     f"{x['orders']} orders, impr/wk {x['weeklyImpr']:,} → {x['lastWeekImpr']:,}, "
                     f"bid {bid(x['bidThen'])} → {bid(x['bidNow'])}"
                     + (f", our changes {', '.join(x['changes'])}" if x["changes"] else ", no change by us"))
    if not out:
        lines.append("none 👍")
    return lines, out


def build_report(today=None, group=None, since_launch=False, save_proposals=False):
    today = today or date.today()
    pb = pb_authenticate()
    watch = json.loads(get_job_state(pb, "change_watch") or "[]")
    if group:
        watch = [w for w in watch if group.lower() in w["label"].lower()]
    w_start, p_start = (today - timedelta(days=7)).isoformat(), (today - timedelta(days=14)).isoformat()
    earliest = min([p_start] + [w["since"] for w in watch]) if since_launch else p_start
    by_camp, camps = load(pb, today, earliest)
    end = today.isoformat()
    lines = [f"🔎 Weekly ads changes review — {w_start} to {(today - timedelta(days=1)).isoformat()}"]
    if since_launch:
        lines[0] = f"🔎 Ads review since launch — up to {(today - timedelta(days=1)).isoformat()}"
    watched_ids, n_flags = set(), 0
    data = {"from": w_start, "to": (today - timedelta(days=1)).isoformat()}
    for w in watch:
        lines.append(f"\n▶ {w['label']} (since {w['since']})")
        for cid in w["campaign_ids"]:
            cid = str(cid)
            watched_ids.add(cid)
            rows = by_camp.get(cid, [])
            c = camps.get(cid, {})
            name = c.get("campaign_name") or (rows[0]["campaign_name"] if rows else cid)
            cc = c.get("country_code") or (rows[0]["country_code"] if rows else "")
            cur = CUR.get(cc, "€")
            start = w["since"] if since_launch else max(w_start, w["since"])
            cur7, days = window(rows, start, end)
            prev7, _ = window(rows, p_start, w_start)
            live = (today - date.fromisoformat(w["since"])).days
            budget = c.get("budget")
            fl = flags(cur7, prev7, days, budget, live, True, cc) if c.get("campaign_status", "ENABLED") == "ENABLED" else []
            n_flags += len(fl)
            status = "" if c.get("campaign_status", "ENABLED") == "ENABLED" else f" [{c['campaign_status']}]"
            bud = f" · budget {cur}{budget:g}/day" if budget else ""
            lines.append(f"• {cc} {name}{status}{bud}\n  {_fmt(cur7, cur)}")
            if not since_launch and (prev7["spend"] or prev7["sales"]):
                lines.append(f"  prev week: {_fmt(prev7, cur)}")
            lines += [f"  {f}" for f in fl]
    alerts = []
    for cid, rows in by_camp.items():
        c = camps.get(cid)
        if cid in watched_ids or not c or c["campaign_status"] != "ENABLED":
            continue
        cur7, days = window(rows, w_start, end)
        prev7, _ = window(rows, p_start, w_start)
        fl = [f for f in flags(cur7, prev7, days, c.get("budget"), 99, False, c["country_code"]) if f[0] in "🔴🟠"]
        if fl:
            alerts.append((cur7["spend"], f"• {c['country_code']} {c['campaign_name']}: {_fmt(cur7, CUR.get(c['country_code'], '€'))}\n  " + " | ".join(fl)))
    if not since_launch:
        lines.append(f"\n⚠️ Other campaigns with red/orange flags: {len(alerts)}")
        lines += [a for _, a in sorted(alerts, reverse=True)[:15]]
        if not alerts:
            lines.append("none - nothing going crazy 👍")
        tacos_lines, data["tacos"] = tacos_section(pb, today)
        seg_lines, data["segments"] = segment_section(pb, today)
        lost_lines, data["lostExposure"] = lost_exposure_section(pb, today)
        lines += tacos_lines + seg_lines + lost_lines
        # Temporary cuts for good targets that went crazy (2026-10-24, per the user)
        from TempBidCuts import followup_section, spike_section
        try:
            spike_lines, data["tempCutProposals"] = spike_section(pb, today, save=save_proposals)
            follow_lines, data["tempCutFollowup"] = followup_section(pb, today) if save_proposals else ([], {})
            lines += spike_lines + follow_lines
        except Exception as exc:
            lines.append(f"\n🌡 temporary-cut check failed: {exc}")
    lines.append(f"\nFlags on watched changes: {n_flags}")
    data["flags"] = n_flags
    return "\n".join(lines), data


def _send(text):
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) > 3800:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    chunks.append(cur)
    for ch in chunks:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": ch}, timeout=30).raise_for_status()


def SendWeeklyChangesReview(request):
    try:
        args = getattr(request, "args", {}) or {}
        today = date.fromisoformat(args["today"]) if args.get("today") else None
        text, data = build_report(today, args.get("group"), args.get("since_launch") == "1",
                                  save_proposals=args.get("dry") != "1")
        if args.get("dry") != "1":
            _send(text)
            if not args.get("group") and args.get("since_launch") != "1":
                save_review(pb_authenticate(), JOB_NAME, (today or date.today()).isoformat(), text, data)
        return _json({"text": text, "data": data})
    except Exception as exc:
        notify(JOB_NAME, "amzbot", f"🚨 Weekly changes review failed: {exc}", is_error=True)
        return _json({"error": str(exc), "type": exc.__class__.__name__}, 500)
