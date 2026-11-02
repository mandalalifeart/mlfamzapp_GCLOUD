"""Automatic monthly ads review (per the user 2026-11-02: "cut losses and make
improvements as fast as possible after the month ends").

Runs on the 2nd at 10:00 for the month that just ended (cron), saved to the
/checks page (check_reports monthly, job ads-monthly-review) with a list of
proposals, and sent to @baba_update_bot with the page link. The user approves
proposals with checkboxes on the page (SetCheckProposal); ApplyApprovedProposals
(cron every 10 min) runs the approved ones that have an executable action and
reports on Telegram. Proposals without an action are done by Claude in chat.

Sections:
 1. Per marketplace: ad spend, ACOS, TACOS - last month vs the month before
    and the same month last year (country_sales, own currency)
 2. Home decor vs pareo per marketplace: spend, ACOS, TACOS (campaign stats
    split by portfolio/name; all sales per ASIN from asin_traffic_monthly)
 3. Ad types per marketplace: SP / SB / SB video / SD spend + ACOS
 4. Products losing money (ProductAdPauses.profit_section on the month) ->
    one proposal per product: pause its SP product ads (action)
 5. Keyword clean-up: keywords / product targets with >= 60 days of data,
    spend >= 10 (x SCALE) in the last 60 days and no order in all stored
    history, campaign ENABLED, not paused by us before -> one proposal per
    campaign: pause them (action)
 6. Products with FBA stock >= 5 and no ad impressions in that marketplace in
    the last 14 days -> one proposal per marketplace (manual: add to campaigns)
Placements are not in it (the placement report takes 30+ min - asked for by hand)."""
import json
from collections import defaultdict
from datetime import date, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all

SCALE = {"SE": 11, "PL": 4.3, "MX": 20, "CA": 1.4}
CUR = {"US": "$", "UK": "£", "CA": "C$", "MX": "MX$", "SE": "kr ", "PL": "zł "}
MARKETS = [("usa", "US"), ("de", "DE"), ("uk", "UK"), ("fr", "FR"), ("it", "IT"), ("es", "ES"), ("nl", "NL"),
           ("se", "SE"), ("pl", "PL"), ("ca", "CA"), ("mex", "MX")]
TRAFFIC_MP = {"us": "US", "uk": "UK", "de": "DE", "fr": "FR", "it": "IT", "es": "ES", "nl": "NL", "se": "SE", "pl": "PL"}
PAREO_WORDS = ("PAREO", "SARONG", "BEACH")
PAGE = "https://mlfamzappfire.web.app/checks?kind=monthly&period={}"
JOB = "ads-monthly-auto"


def _ym(d):
    return d.year, d.month


def _prev(y, m, k=1):
    for _ in range(k):
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return y, m


def _pct(a, b):
    return a / b * 100 if b else None


def _f(x):
    return "-" if x is None else f"{x:.0f}%"


def _camp_stats(pb, start, end):
    """profile-deduped ads_campaign_stats rows in [start, end]."""
    best = {}
    for r in pb_list_all(pb, "ads_campaign_stats", f'date >= "{start}" && date <= "{end}"',
                         fields="campaign_id,campaign_name,country_code,profile_id,date,spend,sales,orders,ad_product"):
        k = (r["campaign_id"], r["date"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    return list(best.values())


def tacos_block(pb, y, m):
    months = [(y, m), _prev(y, m), (y - 1, m)]
    flt = " || ".join(f"(year = {a} && month = {b})" for a, b in months)
    tot = defaultdict(lambda: [0.0, 0.0, 0.0])
    for r in pb_list_all(pb, "country_sales", f"({flt})", fields="marketplace,year,month,sales,ppc_spend,ppc_sales"):
        t = tot[(r["marketplace"], (r["year"], r["month"]))]
        t[0] += r.get("sales") or 0
        t[1] += r.get("ppc_spend") or 0
        t[2] += r.get("ppc_sales") or 0
    lines = [f"1) Per marketplace - {y}-{m:02d} (vs {months[1][0]}-{months[1][1]:02d} | {y - 1}-{m:02d}), own currency"]
    trows = []
    for mp, cc in MARKETS:
        sales, spend, adsales = tot.get((mp, (y, m)), (0, 0, 0))
        if not (sales or spend):
            continue
        p = tot.get((mp, months[1]), (0, 0, 0))
        ly = tot.get((mp, months[2]), (0, 0, 0))
        tac = _pct(spend, sales)
        target = 20 if cc in ("US", "DE") else 25
        mark = "🔴" if tac and tac > target * 1.25 else "🟠" if tac and tac > target else "🟢"
        trows.append([f"{mark} {cc}", round(sales), round(spend), _f(_pct(spend, adsales)), _f(tac), _f(_pct(p[1], p[0])),
                      _f(_pct(ly[1], ly[0])), f"{target}%"])
        lines.append(f"{mark} {cc}: sales {sales:,.0f}, ads {spend:,.0f}, ACOS {_f(_pct(spend, adsales))}, TACOS {_f(tac)} "
                     f"(before {_f(_pct(p[1], p[0]))} | last year {_f(_pct(ly[1], ly[0]))}; target {target}%)")
    return lines, {"title": f"Per marketplace {y}-{m:02d} (own currency)", "rows": trows,
                   "columns": ["Marketplace", "Sales", "Ad spend", "ACOS", "TACOS", "TACOS month before", "TACOS last year", "Target"]}


def segment_block(pb, y, m, rows, camp_port):
    groups = {g["asin"]: g["group"] for g in pb_list_all(pb, "asin_group_mapping", 'asin != ""', fields="asin,group")}
    ads = defaultdict(lambda: [0.0, 0.0])
    for r in rows:
        text = f"{camp_port.get(r['campaign_id'], '')} {r['campaign_name']}".upper()
        seg = "pareo" if any(w in text for w in PAREO_WORDS) else "home"
        a = ads[(r["country_code"].upper(), seg)]
        a[0] += r.get("spend") or 0
        a[1] += r.get("sales") or 0
    sales = defaultdict(float)
    for r in pb_list_all(pb, "asin_traffic_monthly", f"year = {y} && month = {m}", fields="asin,marketplace,sales"):
        g = (groups.get(r["asin"]) or "").upper()
        if g and not g.startswith("IGNORE") and r["marketplace"] in TRAFFIC_MP:
            sales[(TRAFFIC_MP[r["marketplace"]], "pareo" if "PAREO" in g else "home")] += r.get("sales") or 0
    lines = ["2) Home decor vs pareo (ACOS / TACOS)"]
    trows = []
    for _mp, cc in MARKETS:
        parts = []
        for seg, name in (("home", "home decor"), ("pareo", "pareo")):
            spend, adsales = ads.get((cc, seg), (0, 0))
            if spend < 1:
                continue
            tac = _pct(spend, sales.get((cc, seg), 0))
            mark = "🔴" if tac and tac > 35 else ""
            parts.append(f"{name} {spend:,.0f}: {_f(_pct(spend, adsales))} / {_f(tac)}{mark}")
            trows.append([cc, name, round(spend), _f(_pct(spend, adsales)), (_f(tac) + (" 🔴" if mark else ""))])
        if parts:
            lines.append(f"• {cc}: " + " | ".join(parts))
    return lines, {"title": "Home decor vs pareo", "columns": ["Marketplace", "Segment", "Ad spend", "ACOS", "TACOS"], "rows": trows}


def adtype_block(rows):
    t = defaultdict(lambda: [0.0, 0.0])
    for r in rows:
        prod = r.get("ad_product") or ""
        kind = "SD" if "DISPLAY" in prod else ("SB video" if "VIDEO" in (r["campaign_name"] or "").upper() else "SB") \
            if prod != "SPONSORED_PRODUCTS" else "SP"
        a = t[(r["country_code"].upper(), kind)]
        a[0] += r.get("spend") or 0
        a[1] += r.get("sales") or 0
    lines = ["3) Ad types (spend, ACOS)"]
    for _mp, cc in MARKETS:
        parts = [f"{k} {t[(cc, k)][0]:,.0f} {_f(_pct(*t[(cc, k)]))}{' 🔴' if (_pct(*t[(cc, k)]) or 999) > 60 else ''}"
                 for k in ("SP", "SB", "SB video", "SD") if t[(cc, k)][0] >= 1]
        if parts:
            lines.append(f"• {cc}: " + " · ".join(parts))
    trows = []
    for _mp, cc in MARKETS:
        row = [cc]
        for k in ("SP", "SB", "SB video", "SD"):
            sp, sa = t[(cc, k)]
            row += [round(sp) if sp >= 1 else "", (_f(_pct(sp, sa)) + (" 🔴" if (_pct(sp, sa) or 999) > 60 else "")) if sp >= 1 else ""]
        if any(row[1:]):
            trows.append(row)
    return lines, {"title": "Ad types (spend / ACOS)", "rows": trows,
                   "columns": ["Marketplace", "SP spend", "SP ACOS", "SB spend", "SB ACOS", "SB video spend", "SB video ACOS", "SD spend", "SD ACOS"]}


SEG_TARGET = {"home": 35, "pareo": 30}  # target ACOS % (USA home decor plan; pareo 30)


def campaigns_block(pb, y, m, rows, camps, camp_port):
    """All campaigns with spend last month, grouped per marketplace + department
    (per the user 2026-11-02: "show all campaigns, sort them per mp/department").
    Verdict: 🟢 grow = ACOS <= target and >= 70% of the budget used; 🔴 ACOS >= 2x
    target, or spend >= 15 (x SCALE) with no sales; 🟠 above target; else ok."""
    import calendar
    days = calendar.monthrange(y, m)[1]
    py, pm = _prev(y, m)
    prev_rows = _camp_stats(pb, f"{py}-{pm:02d}-01", f"{py}-{pm:02d}-{calendar.monthrange(py, pm)[1]:02d}")

    def agg(rs):
        t = defaultdict(lambda: {"spend": 0.0, "sales": 0.0, "orders": 0.0, "name": "", "cc": "", "prod": ""})
        for r in rs:
            a = t[r["campaign_id"]]
            a["spend"] += r.get("spend") or 0
            a["sales"] += r.get("sales") or 0
            a["orders"] += r.get("orders") or 0
            a["name"], a["cc"], a["prod"] = r["campaign_name"], r["country_code"].upper(), r.get("ad_product") or ""
        return t

    cur, prev = agg(rows), agg(prev_rows)
    groups = defaultdict(list)
    for cid, a in cur.items():
        if a["spend"] < 0.5:
            continue
        text = f"{camp_port.get(cid, '')} {a['name']}".upper()
        seg = "pareo" if any(w in text for w in PAREO_WORDS) else "home"
        c = camps.get(cid, {})
        kind = "SP" if a["prod"] == "SPONSORED_PRODUCTS" else "SD" if "DISPLAY" in a["prod"] else \
            ("SB video" if "VIDEO" in a["name"].upper() else "SB")
        acos = _pct(a["spend"], a["sales"])
        target = SEG_TARGET[seg]
        sc = SCALE.get(a["cc"], 1)
        budget = c.get("budget") or 0
        used = a["spend"] / days / budget * 100 if budget else None
        if not a["sales"] and a["spend"] >= 15 * sc or (acos or 0) >= 2 * target:
            verdict = "🔴 cut"
        elif acos is not None and acos <= target and (used or 0) >= 70:
            verdict = "🟢 grow"
        elif acos is not None and acos <= target:
            verdict = "🟢 ok"
        else:
            verdict = "🟠 above target"
        status = (c.get("campaign_status") or "").title()
        if status and status != "Enabled":
            verdict = f"⏸ {status.lower()} now ({verdict.split(' ', 1)[1]})"
        p = prev.get(cid)
        groups[(a["cc"], seg)].append([
            a["name"], kind, (c.get("campaign_status") or "").title(), round(budget, 2) if budget else "",
            round(a["spend"] / days, 2), f"{used:.0f}%" if used is not None else "", round(a["spend"]), round(a["sales"]),
            round(a["orders"]), _f(acos), _f(_pct(p["spend"], p["sales"])) if p and p["spend"] >= 0.5 else "", f"{target}%", verdict])
    order = {cc: i for i, (_mp, cc) in enumerate(MARKETS)}
    trows, lines = [], ["7) All campaigns per marketplace / department (own currency) - see the table"]
    for (cc, seg) in sorted(groups, key=lambda k: (order.get(k[0], 99), k[1])):
        kids = sorted(groups[(cc, seg)], key=lambda r: -r[6])
        sp, sa = sum(r[6] for r in kids), sum(r[7] for r in kids)
        grow = [r[0] for r in kids if r[12] == "🟢 grow"]
        cut = [r[0] for r in kids if r[12].startswith("🔴")]  # paused ones are "⏸ ..." and left out
        trows.append({"cells": [f"{cc} · {'Home decor' if seg == 'home' else 'Pareo'} ({len(kids)})", "", "", "", "", "",
                                sp, sa, sum(r[8] for r in kids), _f(_pct(sp, sa)), "", f"{SEG_TARGET[seg]}%",
                                f"🟢 {len(grow)} · 🔴 {len(cut)}"], "children": kids})
        lines.append(f"• {cc} {seg}: spend {sp:,.0f}, ACOS {_f(_pct(sp, sa))}"
                     + (f" | grow: {', '.join(grow[:4])}" if grow else "") + (f" | cut: {', '.join(cut[:4])}" if cut else ""))
    return lines, {"title": f"All campaigns {y}-{m:02d} per marketplace / department (own currency) - click a row",
                   "columns": ["Campaign", "Type", "Status now", "Budget/day now", "Spend/day", "Budget used", "Spend", "Sales",
                               "Orders", "ACOS", "ACOS month before", "Target", "Verdict"], "rows": trows}


def keyword_cleanup(pb, end, camps):
    """-> proposals (one per campaign) to pause targets with 60+ days, spend >= 10, no order ever."""
    e = date.fromisoformat(end)
    w0 = (e - timedelta(days=59)).isoformat()
    rows = pb_list_all(pb, "ads_keyword_stats", f'date >= "{(e - timedelta(days=365)).isoformat()}"',
                       fields="profile_id,target_id,target_text,match_type,target_type,campaign_id,campaign_name,ad_group_id,"
                              "ad_product,country_code,date,spend,orders,bid")
    best = {}
    for r in rows:
        k = (r["target_id"], r["date"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    agg = {}
    for r in best.values():
        if r["date"] > end:
            continue
        a = agg.setdefault(r["target_id"], {"first": r["date"], "orders": 0, "spend60": 0.0, "last": r})
        a["first"] = min(a["first"], r["date"])
        a["orders"] += r.get("orders") or 0
        if r["date"] >= w0:
            a["spend60"] += r.get("spend") or 0
        if r["date"] >= a["last"]["date"]:
            a["last"] = r
    paused = {r["target_id"] for r in pb_list_all(pb, "ads_bid_change_log", 'action = "disable" || action = "pause"',
                                                   fields="target_id")}
    by_camp = defaultdict(list)
    for tid, a in agg.items():
        r = a["last"]
        cc = r["country_code"].upper()
        if a["orders"] or a["first"] > (e - timedelta(days=60)).isoformat() or a["spend60"] < 10 * SCALE.get(cc, 1):
            continue
        if r["match_type"] in ("TARGETING_EXPRESSION_PREDEFINED", "THEME") or r["target_text"] in ("*", "") or tid in paused:
            continue
        if (camps.get(r["campaign_id"]) or {}).get("campaign_status") != "ENABLED" or not r.get("bid"):
            continue
        by_camp[(cc, r["campaign_id"], r["campaign_name"])].append({
            "target_id": tid, "text": r["target_text"], "match_type": r["match_type"], "campaign_id": r["campaign_id"],
            "campaign": r["campaign_name"], "ad_group_id": r.get("ad_group_id") or "", "profile_id": r["profile_id"],
            "ad_product": r["ad_product"], "country": cc, "bid": r["bid"], "spend60": round(a["spend60"], 2),
            "since": a["first"]})
    props = []
    for (cc, _cid, cname), items in sorted(by_camp.items(), key=lambda kv: -sum(i["spend60"] for i in kv[1])):
        items.sort(key=lambda i: -i["spend60"])
        cur = CUR.get(cc, "€")
        props.append({"section": "Keyword clean-up", "title": f"{cc} {cname}: pause {len(items)} keyword(s)/target(s) with no sale ever",
                      "detail": "; ".join(f"{i['text']} ({i['match_type'].lower()}, {cur}{i['spend60']:.0f} in 60d, since {i['since']})"
                                          for i in items[:12]) + (" …" if len(items) > 12 else ""),
                      "action": {"type": "pause_targets", "items": items}})
    return props


NO_ADS_JOB = "products-no-ads"
NO_ADS_LABEL = "📭 Products without ads"


def no_ads_block(pb, end):
    """Products with FBA stock >= 5 and no ad impressions in the last 14 days, per
    marketplace. Its own /checks section with a Reply per product (per the user
    2026-11-02 - not proposals). -> (text, table)."""
    e = date.fromisoformat(end)
    s = (e - timedelta(days=13)).isoformat()
    seen = defaultdict(set)
    for r in pb_list_all(pb, "ads_advertised_product_stats", f'date >= "{s}" && date <= "{end}" && impressions > 0',
                         fields="asin,country_code"):
        seen[r["country_code"].upper()].add(r["asin"])
    sku_asin = {g["sku"]: (g["asin"], g["group"]) for g in pb_list_all(pb, "asin_group_mapping", 'asin != ""',
                                                                         fields="sku,asin,group")}
    stock = pb_list_all(pb, "sku_statistics", "", fields="sku,usa_balance_fba,de_balance_fba,uk_balance_fba")
    winter = end < "2027-02-20"  # pareo winter mode (no pareo ads on purpose) - same date as ProductAdPauses
    lines = [f"Products with FBA stock (≥ 5) but no ad impressions {s} – {end}"
             + (" (pareos left out until the season restart 2027-02-20)" if winter else "")]
    rows = []
    for field, countries in (("usa_balance_fba", ["US"]), ("de_balance_fba", ["DE", "FR", "IT", "ES"]),
                             ("uk_balance_fba", ["UK"])):
        for cc in countries:
            miss = []
            for r in stock:
                asin, grp = sku_asin.get(r["sku"], (None, ""))
                if not asin or (grp or "").upper().startswith("IGNORE") or (r.get(field) or 0) < 5:
                    continue
                if winter and "PAREO" in (grp or "").upper():
                    continue
                if asin not in seen[cc]:
                    miss.append([r["sku"], asin, grp or "", round(r.get(field) or 0)])
            if not miss:
                continue
            miss.sort(key=lambda x: (x[2], -x[3]))
            pool = "Pan-EU stock" if cc != "US" and cc != "UK" else "FBA stock"
            rows.append({"cells": [f"{cc} ({len(miss)})", "", "", f"{pool}: {sum(x[3] for x in miss)}"],
                         "children": [[f"{cc} {x[0]}", x[1], x[2], x[3]] for x in miss]})
            lines.append(f"\n{cc}: {len(miss)} product(s)")
            lines += [f"  {cc} {x[0]} ({x[2]}): FBA {x[3]}" for x in miss]
    if not rows:
        lines.append("✅ every product with stock had ads")
    return "\n".join(lines), {"title": f"Products with FBA stock but no ads, {s} – {end} - click a marketplace, 💬 per product",
                              "columns": ["Product", "ASIN", "Group", "FBA stock"], "rows": rows}


def build(today=None):
    today = today or date.today()
    first = today.replace(day=1)
    last_day = first - timedelta(days=1)
    y, m = _ym(last_day)
    start, end = f"{y}-{m:02d}-01", last_day.isoformat()
    period = f"{y}-{m:02d}"
    pb = pb_authenticate()
    camps = {c["campaign_id"]: c for c in pb_list_all(pb, "ads_campaigns", 'campaign_id != ""',
                                                       fields="campaign_id,campaign_name,campaign_status,portfolio_id,budget")}
    ports = {p["portfolio_id"]: p["name"] for p in pb_list_all(pb, "ads_portfolios", 'portfolio_id != ""', fields="portfolio_id,name")}
    camp_port = {cid: ports.get(c.get("portfolio_id"), "") for cid, c in camps.items()}
    rows = _camp_stats(pb, start, end)

    lines = [f"📊 MONTHLY ADS REVIEW - {period}", "(last ~7 days can still gain ad sales - 7-day attribution)", ""]
    tables = []
    for blk in (tacos_block(pb, y, m), segment_block(pb, y, m, rows, camp_port), adtype_block(rows),
                campaigns_block(pb, y, m, rows, camps, camp_port)):
        lines += blk[0] + [""]
        tables.append(blk[1])

    proposals = []
    from ProductAdPauses import profit_section
    _p_lines, p_data = profit_section(pb, today, save=False, start=start, end=end, fresh=True)
    lines.append(f"4) Products losing money (USD; profit incl. fees, product cost, SP+SD ads, SB share, storage, AWD - "
                 f"returns not counted): 🔴 {len(p_data['red'])} because of ads, 🟠 {len(p_data['orange'])} even before ads")
    for r in p_data["red"][:25]:
        why = "; ".join(f"{n} ad{'s' if n > 1 else ''} {w}" for w, n in (r.get("notes") or {}).items())
        lines.append(f"🔴 {r['mp'].upper()} {r['sku']}: {r['units']:.0f} units at ${r['price']:.2f}, ads ${r['ads']:.0f} "
                     f"(TACOS {r['tacos']}%) → ${r['profit']:.0f}" + (f" - {why}" if why else ""))
    for r in p_data["orange"][:15]:
        lines.append(f"🟠 {r['mp'].upper()} {r['sku']}: {r['units']:.0f} units at ${r['price']:.2f} → ${r['before']:.0f} before ads")
    tables.append({"title": "Products losing money (USD)", "columns": ["", "MP", "Product", "Units", "Price", "Ads", "TACOS", "Profit", "Before ads", "Note"],
                   "rows": [["🔴", r["mp"].upper(), r["sku"], round(r["units"]), round(r["price"], 2), round(r["ads"]), r["tacos"],
                             round(r["profit"]), round(r["before"]), "; ".join(f"{n} {w}" for w, n in (r.get("notes") or {}).items())]
                            for r in p_data["red"]] +
                           [["🟠", r["mp"].upper(), r["sku"], round(r["units"]), round(r["price"], 2), round(r["ads"]), r["tacos"],
                             round(r["profit"]), round(r["before"]), "loses money before ads"] for r in p_data["orange"]]})
    lines.append("")
    for r in p_data["red"]:
        if r.get("pauseItems"):
            proposals.append({"section": "Products losing money",
                              "title": f"{r['mp'].upper()} {r['sku']}: pause {len(r['pauseItems'])} product ad(s)",
                              "detail": f"{r['units']:.0f} units at ${r['price']:.2f}, ads ${r['ads']:.0f} (TACOS {r['tacos']}%) → "
                                        f"profit ${r['profit']:.0f}; " + "; ".join(
                                            f"{it['campaign']} ({it['history']} history, {it['pause_days']} days)" for it in r["pauseItems"]),
                              "action": {"type": "pause_product_ads", "items": r["pauseItems"]}})
    for r in p_data["orange"]:
        proposals.append({"section": "Products losing money",
                          "title": f"{r['mp'].upper()} {r['sku']}: loses money before ads - price / coupon",
                          "detail": f"{r['units']:.0f} units at ${r['price']:.2f} → ${r['before']:.0f} before ads. Decide price or coupon (Claude prepares it)."})
    kc = keyword_cleanup(pb, end, camps)
    lines += [f"5) Keyword clean-up (60+ days, spend ≥ 10, no order ever): {sum(len(p['action']['items']) for p in kc)} in {len(kc)} campaign(s)"]
    lines += [f"• {p['title']}" for p in kc[:15]] + [""]
    proposals += kc
    from AdsGrowth import Api, harvest_block, raise_block
    api = Api(pb)
    for blk in (raise_block(pb, today, api), harvest_block(pb, today, api)):
        lines += blk[0] + [""]
        tables.append(blk[1])
        proposals += blk[2]
    no_ads = no_ads_block(pb, end)
    lines += [f"6) Products with FBA stock but no ads: see the \"{NO_ADS_LABEL}\" section", ""]
    for i, p in enumerate(proposals, 1):
        p.update(id=i, status="proposed")
    lines.append(f"{len(proposals)} proposals - approve them on the page: {PAGE.format(period)}")
    return period, "\n".join(lines), proposals, tables, no_ads


def save_all(period, text, proposals, tables, no_ads):
    from CheckReports import save_report
    save_report("monthly", period, JOB, "🤖 Ads monthly review (automatic)", text, proposals=proposals, tables=tables)
    save_report("monthly", period, NO_ADS_JOB, NO_ADS_LABEL, no_ads[0], tables=[no_ads[1]])


def SendMonthlyAdsReview(request):
    from CheckReports import save_report
    from NotificationRouting import notify
    args = getattr(request, "args", {}) or {}
    try:
        period, text, proposals, tables, no_ads = build(date.fromisoformat(args["today"]) if args.get("today") else None)
        if str(args.get("dry")) == "1":
            return json.dumps({"text": text, "proposals": proposals}), 200, {"Content-Type": "application/json"}
        if str(args.get("quiet")) == "1":  # rebuild the page entry without a Telegram message
            save_all(period, text, proposals, tables, no_ads)
            return json.dumps({"period": period, "proposals": len(proposals)}), 200, {"Content-Type": "application/json"}
        save_all(period, text, proposals, tables, no_ads)
        head = "\n".join(text.splitlines()[:40])
        notify("monthly-ads-review", "amzbot", head + f"\n\n✅ {len(proposals)} proposals to approve: {PAGE.format(period)}")
        return json.dumps({"period": period, "proposals": len(proposals)}), 200, {"Content-Type": "application/json"}
    except Exception as exc:
        notify("monthly-ads-review", "amzbot", f"🚨 Monthly ads review failed: {exc}", is_error=True)
        return json.dumps({"error": str(exc)}), 500, {"Content-Type": "application/json"}
