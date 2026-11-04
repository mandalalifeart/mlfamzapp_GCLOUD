"""TACOS targets per marketplace x department + the monthly TACOS bid rule
(per the user 2026-11-03: "lets work on tacos, not acos ... define tacos per
mp/department based on 10 percent profit, and then all our ad recommendations
should look at tacos"; formulas confirmed in the chat the same day).

Target TACOS (last 12 months, ProductCost.product_ranking per store, products
with a landed cost):
  target = (profit before ads - 10% x sales without VAT - 4.5% x sales) / sales
  profit before ads = sales w/o VAT - Amazon fees - product cost (purchase +
  duty + freight by weight) - returns - storage/Lagerpark/AWD; 4.5% = overheads +
  what the real books lose beyond the model (ModelVsReal, Jan-Oct 2026).
  A marketplace-department with < $10k sales in 12 months uses its region's
  value (EU countries -> all EU stores, CA/MX -> US).
  adShare (last 90 days) = SP+SD attributed sales / all sales -> the keyword
  ACOS limit the ACOS-based tools use = target / adShare.
Stored in job_state "tacos_targets"; recalculated on the 2nd of every month.

Monthly TACOS bid rule (last full month, per item and store):
  item TACOS = (SP+SD spend + its SB share) / all its sales
  cap = 20% at 3 units sold, rising evenly to 50% at 10+ units
  TACOS < target and >= 3 units -> +min(target/TACOS - 1, cap)
  TACOS > target and >= 3 units -> -min(1 - target/TACOS, cap)
  0-2 units but ad spend >= 15 (own currency) -> -30%
  new bid = old bid x (1 + change). The % goes to every keyword/target of the
  SP ad groups that advertise the item (several items -> spend-weighted
  average); SB campaigns get their marketplace-department's change.
  No bid limits except Amazon's minimum: a bid that would go below it is
  paused instead - "temporary" (re-proposed next month) if the target had >= 3
  orders in the 60 days before, else "for good".
  Skipped: targets changed in the last 14 days, Amazon theme targets, paused
  (manually or not) targets, pareo winter (Oct-Apr): targets x80%, raises max +15% with >= 5 orders."""
import json
import math
from collections import defaultdict
from datetime import date, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state, set_job_state
from ProductCost import ATOMIC, _section_of_group, fx_to, product_ranking

STATE_KEY = "tacos_targets"
CONFIG_KEY = "ads_rules_config"
# Every number of the TACOS framework, editable on /ads-reviews (per the user
# 2026-11-03 "lets make all the numbers configurable").
DEFAULTS = {
    "profitPct": 10.0,         # profit wanted after everything, % of sales without VAT
    "gapPct": 4.5,             # overheads + what the real books lose beyond the model, % of sales
    "smallSales": 10000,       # USD / 12 months: below -> the region's target
    "minUnits": 3,             # units sold in the month for a TACOS raise/cut
    "capAtMinUnits": 20.0,     # max change % at minUnits
    "fullUnits": 10,           # units for the full cap
    "capAtFullUnits": 50.0,    # max change % at fullUnits+
    "noSaleSpend": 15.0,       # ad spend (own currency) with < minUnits sold -> cut
    "noSaleCut": 30.0,         # that cut %
    "skipChangedDays": 14,     # don't touch bids changed in the last N days
    "sbProfitPct": 15.0,       # SB = independent sales: profit on them (shown as "SB ACOS at profit%")
    "sbAcos": 20.0,            # SB ACOS limit used by the rules (per the user 2026-11-03, option C)
    "sbAcosOverrides": "DE home 15, FR home 15, ES home 15",  # "<market> <home|pareo> <acos>, ..."
    "pauseSpend": 45.0,        # < minUnits orders even over 3 months and spent >= this (own currency) -> pause
    "lowSpend": 15.0,          # < minUnits orders over 3 months and spent < this -> raise to give it a chance
    "lowSpendRaise": 20.0,     # that raise %
    "kwMinClicks": 5,          # keyword clicks in the month: own ACOS > limit -> no raise, lower instead
    "historyDays": 60,         # history window before the month (pause temporary vs for good)
    "historyOrders": 3,        # orders in that window -> temporary pause
    "orangeBand": 3.0,         # TACOS up to target + this = orange, above = red
    "growBand": 5.0,           # TACOS this far under target -> raises/budget increases proposed
    # Pareo winter (per the user 2026-11-04): from pareoWinterStart until pareoSeasonStart
    # (MM-DD, every year) pareo targets / ACOS limits are pareoWinterAcosPct % of the
    # summer ones, and raises are allowed only up to pareoWinterMaxRaise % per step with
    # >= pareoWinterMinOrders orders in the window.
    "pareoSeasonStart": "05-01",
    "pareoWinterStart": "10-01",
    "pareoWinterAcosPct": 80.0,
    "pareoWinterMaxRaise": 15.0,
    "pareoWinterMinOrders": 5.0,
}


def pareo_winter(today=None, cfg=None):
    cfg = cfg or DEFAULTS
    md = (today or date.today()).strftime("%m-%d")
    a, b = cfg["pareoWinterStart"], cfg["pareoSeasonStart"]
    return (md >= a or md < b) if a > b else (a <= md < b)


def winter_raise(pct, orders, cfg):
    """A pareo raise in winter: only with enough orders, capped."""
    if pct <= 0:
        return pct
    if orders < cfg["pareoWinterMinOrders"]:
        return 0.0
    return min(pct, cfg["pareoWinterMaxRaise"] / 100)


def config(token=None):
    cfg = dict(DEFAULTS)
    try:
        raw = get_job_state(token or pb_authenticate(), CONFIG_KEY)
        if raw:
            cfg.update({k: v for k, v in json.loads(raw).items() if k in DEFAULTS})
    except Exception:
        pass
    return cfg
EU = {"DE", "FR", "IT", "ES", "NL", "BE", "IE", "SE", "PL"}
CUR = {"US": "USD", "CA": "CAD", "MX": "MXN", "UK": "GBP", "SE": "SEK", "PL": "PLN"}  # others EUR
MIN_BID = {"SPONSORED_PRODUCTS": {"MX": 0.10, "SE": 0.20, "PL": 0.04},  # others 0.02
           "SPONSORED_BRANDS": {"US": 0.25, "SE": 1.0, "PL": 0.4, "MX": 1.0}}  # others 0.10
PAUSES_KEY = "tacos_rule_pauses"


def _region(country):
    return "EU" if country in EU else ("US" if country in ("US", "CA", "MX") else country)


def _agg(token, start, end):
    """{(country, section): totals} from product_ranking per store."""
    sec = {r["asin"]: _section_of_group(r.get("group")) for r in
           pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,group")}
    out = defaultdict(lambda: defaultdict(float))
    for mp in sorted(ATOMIC):
        for asin, v in product_ranking(start, end, atomic=[mp], _token=token)["byAsin"].items():
            s = sec.get(asin)
            if not s or v.get("profit") is None:
                continue
            country = ATOMIC[mp][1]
            for key in ((country, s), (_region(country) + "*", s)):
                d = out[key]
                for k in ("units", "sales", "netSales", "fees", "productCost", "ads", "sbAds", "adSales", "returns",
                          "storage", "awd"):
                    d[k] += v.get(k) or 0
    return out


def compute_targets(token=None, end=None):
    token = token or pb_authenticate()
    cfg = config(token)
    profit, gap = cfg["profitPct"] / 100, cfg["gapPct"] / 100
    end = end or (date.today().replace(day=1) - timedelta(days=1))
    start = (end.replace(day=1) - timedelta(days=330)).replace(day=1)
    year = _agg(token, start.isoformat(), end.isoformat())

    def calc(d):
        s = d["sales"]
        before = d["netSales"] - d["fees"] - d["productCost"] - d["returns"] - d["storage"] - d["awd"]
        return {"sales": round(s), "units": round(d["units"]), "beforeAdsPct": round(100 * before / s, 1),
                "targetTacos": round(100 * (before - profit * d["netSales"] - gap * s) / s, 1),
                # Per the user 2026-11-03: SB sales are independent, ACOS = TACOS,
                # and SB should make sbProfitPct (15%) on them.
                "sbAcos": round(100 * (before - cfg["sbProfitPct"] / 100 * d["netSales"] - gap * s) / s, 1),
                # SP keyword ACOS limit = break-even on the ad sale itself (user's option B
                # 2026-11-04): profit before ads - gap, the organic lift is the profit.
                "breakEvenAcos": round(100 * (before - gap * s) / s, 1),
                "tacos": round(100 * (d["ads"] + d["sbAds"]) / s, 1)}

    overrides = {}
    for part in str(cfg.get("sbAcosOverrides") or "").split(","):
        bits = part.split()
        if len(bits) == 3:
            try:
                overrides[f"{bits[0].upper()}|{bits[1].lower()}"] = float(bits[2])
            except ValueError:
                pass
    targets = {}
    for (country, seg), d in year.items():
        if country.endswith("*") or not d["sales"]:
            continue
        row = calc(d)
        if d["sales"] < cfg["smallSales"] and _region(country) != country:
            reg = year.get((_region(country) + "*", seg))
            if reg and reg["sales"]:
                row["targetTacos"] = calc(reg)["targetTacos"]
                row["sbAcos"] = calc(reg)["sbAcos"]  # replaced below by the configured limit
                row["breakEvenAcos"] = calc(reg)["breakEvenAcos"]
                row["source"] = _region(country)
        row.setdefault("source", "own")
        # SB limit used by the rules: flat sbAcos with per-market overrides
        # (user's option C 2026-11-03); the computed 15%-profit value stays as sbAcosCalc.
        row["sbAcosCalc"] = row.pop("sbAcos", None)
        row["sbAcos"] = overrides.get(f"{country}|{seg}", float(cfg["sbAcos"]))
        # 12 months, not the last 90 days: pareo's off-season ad share is tiny
        row["adShare"] = round(100 * d.get("adSales", 0) / d["sales"], 1) if d.get("sales") else None
        # old limit (target TACOS / ad share) kept for reference only
        row["acosLimitShare"] = (round(min(100.0, row["targetTacos"] / row["adShare"] * 100), 1)
                                 if row["adShare"] and row["targetTacos"] > 0 else None)
        row["acosLimit"] = row["breakEvenAcos"] if row["breakEvenAcos"] > 0 else None
        targets[f"{country}|{seg}"] = row
    state = {"asOf": date.today().isoformat(), "from": start.isoformat(), "to": end.isoformat(),
             "profitPct": cfg["profitPct"], "gapPct": cfg["gapPct"], "targets": targets}
    set_job_state(token, STATE_KEY, json.dumps(state))
    return state


def load_targets(token, today=None, summer=False):
    """Stored targets; in the pareo winter the pareo targets / ACOS limits are
    scaled to pareoWinterAcosPct % (summer=True gives the stored values)."""
    raw = get_job_state(token, STATE_KEY)
    state = json.loads(raw) if raw else compute_targets(token)
    cfg = config(token)
    if summer or not pareo_winter(today, cfg):
        return state
    f = cfg["pareoWinterAcosPct"] / 100
    state = json.loads(json.dumps(state))
    for k, t in (state.get("targets") or {}).items():
        if k.endswith("|pareo"):
            for fld in ("targetTacos", "acosLimit", "acosLimitShare", "sbAcos"):
                if isinstance(t.get(fld), (int, float)) and t[fld] > 0:
                    t["summer" + fld[0].upper() + fld[1:]] = t[fld]
                    t[fld] = round(t[fld] * f, 1)
            t["winter"] = True
    state["pareoWinter"] = {"active": True, "pct": cfg["pareoWinterAcosPct"], "until": cfg["pareoSeasonStart"]}
    return state


def target_for(state, country, seg):
    t = (state.get("targets") or {}).get(f"{(country or '').upper()}|{seg}")
    return t["targetTacos"] if t else None


def sb_acos(state, country, seg):
    """SB ACOS limit = the TACOS that leaves sbProfitPct profit (SB sales count as independent)."""
    t = (state.get("targets") or {}).get(f"{(country or '').upper()}|{seg}")
    return (t or {}).get("sbAcos")


def acos_limit(state, country, seg, default=None):
    """Keyword-level ACOS limit derived from the TACOS target (for the ACOS-based tools)."""
    t = (state.get("targets") or {}).get(f"{(country or '').upper()}|{seg}")
    return (t or {}).get("acosLimit") or default


# --------------------------------------------------------------- bid rule
def _cap(units, cfg):
    lo, hi = cfg["capAtMinUnits"], cfg["capAtFullUnits"]
    u0, u1 = cfg["minUnits"], cfg["fullUnits"]
    if u1 <= u0:
        return hi / 100
    return min(hi, lo + (min(units, u1) - u0) * (hi - lo) / (u1 - u0)) / 100


def change_pct(tacos, target, units, spend_local, cfg=None):
    """Bid change as a fraction (+0.25 = +25%), or 0."""
    cfg = cfg or DEFAULTS
    if target is None:
        return 0.0
    if units >= cfg["minUnits"]:
        cap = _cap(units, cfg)
        if tacos < target:
            return cap if tacos <= 0 else min(target / tacos - 1, cap)
        if tacos > target:
            return -min(1 - target / tacos, cap)
        return 0.0
    if spend_local >= cfg["noSaleSpend"]:
        return -cfg["noSaleCut"] / 100
    return 0.0


def window_decision(wins, limit, cfg, fx=1.0):
    """Per the user 2026-11-03: < minUnits orders in the month -> look at 2
    months, then 3 months; still < minUnits -> pause if it spent >= pauseSpend
    with ACOS/TACOS above the limit (or no sales), raise lowSpendRaise% if it
    spent < lowSpend, else leave. wins = [1-month, 2-month, 3-month] dicts
    {units, sales, spend} (spend x fx = own currency).
    -> (kind "change"|"pause"|"raise"|"none", pct, months used, ratio %)."""
    if limit is None:
        return "none", 0.0, 0, None
    for i, w in enumerate(wins, 1):
        units, sales, spend = w.get("units") or 0, w.get("sales") or 0, w.get("spend") or 0
        ratio = 100 * spend / sales if sales else (math.inf if spend else 0.0)
        if units >= cfg["minUnits"]:
            cap = _cap(units, cfg)
            if ratio < limit:
                return "change", (cap if ratio <= 0 else min(limit / ratio - 1, cap)), i, ratio
            if ratio > limit:
                return "change", -min(1 - limit / ratio, cap), i, ratio
            return "none", 0.0, i, ratio
    w = wins[-1]
    spend_local = (w.get("spend") or 0) * fx
    ratio = 100 * w["spend"] / w["sales"] if w.get("sales") else (math.inf if w.get("spend") else 0.0)
    if spend_local >= cfg["pauseSpend"] and ratio > limit:
        return "pause", -1.0, len(wins), ratio
    if spend_local < cfg["lowSpend"]:
        return "raise", cfg["lowSpendRaise"] / 100, len(wins), ratio
    return "none", 0.0, len(wins), ratio


def bid_rule(token, y, m, today=None, api=None):
    """-> (lines, table, proposals) for the month y-m."""
    from AdsGrowth import Api, _campaigns, _dedupe, _recently_changed
    today = today or date.today()
    api = api or Api(token)
    state = load_targets(token)
    cfg = config(token)
    start = date(y, m, 1)
    end = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    sec = {r["asin"]: _section_of_group(r.get("group")) for r in
           pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,group,sku")}
    sku_of = {r["asin"]: r["sku"] for r in pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,sku")}

    # window starts: the month, 2 months, 3 months (2026-11-03 rule)
    starts = [start, (start - timedelta(days=1)).replace(day=1)]
    starts.append((starts[1] - timedelta(days=1)).replace(day=1))

    # items per store and their change; wins[i] = cumulative over i+1 months
    items, dept = {}, defaultdict(lambda: defaultdict(float))
    for wi, ws in enumerate(starts):
        for mp in sorted(ATOMIC):
            country = ATOMIC[mp][1]
            for asin, v in product_ranking(ws.isoformat(), end.isoformat(), atomic=[mp], _token=token)["byAsin"].items():
                seg = sec.get(asin)
                if not seg:
                    continue
                k = (country, asin)
                it = items.setdefault(k, {"seg": seg, "wins": [defaultdict(float) for _ in starts]})
                w = it["wins"][wi]
                w["units"] += v.get("units") or 0
                w["sales"] += v.get("sales") or 0
                w["spend"] += v.get("ads") or 0       # SP + SD only - SB is judged on its own (2026-11-03)
                if wi == 0:
                    d = dept[(country, seg)]
                    d["units"] += v.get("units") or 0
                    d["sales"] += v.get("sales") or 0
                    d["spend"] += (v.get("ads") or 0) + (v.get("sbAds") or 0)
    for it in items.values():
        w = it["wins"][0]
        it.update(units=w["units"], sales=w["sales"], spend=w["spend"])
    winter = pareo_winter(today, cfg)

    def decide(country, seg, units, sales, spend):
        tacos = 100 * spend / sales if sales else (math.inf if spend else 0.0)
        target = target_for(state, country, seg)
        pct = change_pct(tacos, target, units, spend * fx_to(CUR.get(country, "EUR")), cfg)
        if seg == "pareo" and winter:
            pct = winter_raise(pct, units, cfg)
        return tacos, target, pct

    for (country, asin), it in items.items():
        it["target"] = target_for(state, country, it["seg"])
        kind, pct, months, ratio = window_decision(it["wins"], it["target"], cfg, fx_to(CUR.get(country, "EUR")))
        wunits = it["wins"][max(months, 1) - 1]["units"]
        if pct > 0 and it["seg"] == "pareo" and winter:
            pct = winter_raise(pct, wunits, cfg)
            kind = kind if pct > 0 else "none"
        it.update(kind=kind, pct=pct, months=months, tacos=ratio, wunits=wunits)
    dept_pct = {k: decide(k[0], k[1], d["units"], d["sales"], d["spend"]) for k, d in dept.items()}

    # SP ad groups -> spend-weighted change of the items they advertise
    rows = pb_list_all(token, "ads_advertised_product_stats", f'date >= "{starts[-1]}" && date <= "{end}"',
                       fields="profile_id,campaign_id,ad_group_id,asin,country_code,date,spend")
    ag = defaultdict(lambda: [0.0, 0.0, []])
    for r in _dedupe(rows, lambda r: (r["campaign_id"], r["ad_group_id"], r["asin"], r["date"])):
        it = items.get((r["country_code"].upper(), r["asin"]))
        if not it or not r.get("spend"):
            continue
        a = ag[(r["campaign_id"], r["ad_group_id"])]
        a[0] += r["spend"] * it["pct"]
        a[1] += r["spend"]
        a[2].append(r["asin"])
    ag_pct = {k: v[0] / v[1] for k, v in ag.items() if v[1]}
    ag_items = {k: sorted(set(v[2])) for k, v in ag.items()}

    camps = _campaigns(token)
    changed = _recently_changed(token, today, days=int(cfg["skipChangedDays"]))
    hist_end = starts[-1] - timedelta(days=1)
    hist = defaultdict(float)
    for r in _dedupe(pb_list_all(token, "ads_keyword_stats",
                                 f'date >= "{hist_end - timedelta(days=int(cfg["historyDays"]) - 1)}" && date <= "{hist_end}"',
                                 fields="profile_id,campaign_id,target_id,date,orders"),
                     lambda r: (r["campaign_id"], r["target_id"], r["date"])):
        hist[r["target_id"]] += r.get("orders") or 0
    by_camp = defaultdict(dict)
    for (cid, agid), pct in ag_pct.items():
        if abs(pct) >= 0.01:
            by_camp[cid][agid] = pct
    # SB: every keyword on its own ACOS vs the SB ACOS limit (per the user
    # 2026-11-03: "SB performs on its own, based on ACOS ... make 15% on them").
    for cid, c in camps.items():
        if c.get("ad_product") == "SPONSORED_BRANDS" and c.get("campaign_status") == "ENABLED":
            by_camp[cid]["*"] = "sb"

    # keyword texts + the month's numbers per keyword for the page (the live
    # list carries only ids; per the user 2026-11-03 "more info on bid raises")
    names, kw = {}, defaultdict(lambda: defaultdict(float))
    kwin = defaultdict(lambda: [defaultdict(float) for _ in starts])
    for r in _dedupe(pb_list_all(token, "ads_keyword_stats", f'date >= "{starts[-1]}" && date <= "{end}"',
                                 fields="profile_id,campaign_id,target_id,target_text,date,clicks,orders,spend,sales"),
                     lambda r: (r["campaign_id"], r["target_id"], r["date"])):
        names[r["target_id"]] = r.get("target_text") or r["target_id"]
        for wi, ws in enumerate(starts):
            if r["date"] >= ws.isoformat():
                w = kwin[r["target_id"]][wi]
                for k in ("clicks", "spend", "sales"):
                    w[k] += r.get(k) or 0
                w["units"] += r.get("orders") or 0
        if r["date"] >= start.isoformat():
            for k in ("clicks", "orders", "spend", "sales"):
                kw[r["target_id"]][k] += r.get(k) or 0
    for r in pb_list_all(token, "ads_keyword_stats", f'date > "{end}"', fields="target_id,target_text"):
        names.setdefault(r["target_id"], r.get("target_text") or r["target_id"])
    groups = defaultdict(list)
    trows = []
    for cid, agp in by_camp.items():
        c = camps.get(cid)
        if not c or c.get("campaign_status") != "ENABLED":
            continue
        try:
            live = api.live_targets(c["profile_id"], cid, c["ad_product"])
        except Exception as exc:
            print("tacos rule live bids failed", c["campaign_name"], exc)
            continue
        cc = c.get("country_code", "").upper()
        min_bid = MIN_BID[c["ad_product"]].get(cc, 0.10 if c["ad_product"] == "SPONSORED_BRANDS" else 0.02)
        for tid, lv in live.items():
            pct = agp.get(lv["ad_group_id"], agp.get("*"))
            if pct is None or tid in changed or not lv.get("bid"):
                continue
            old = float(lv["bid"])
            k = kw.get(tid) or {}
            kws = kwin.get(tid) or [defaultdict(float) for _ in starts]
            is_sb = pct == "sb"
            force_pause, kbasis = False, ""
            if is_sb:
                # SB keyword on its own ACOS, 1 -> 2 -> 3 months (2026-11-03)
                sb_limit = sb_acos(state, cc, c["seg"])
                if not sb_limit or sb_limit <= 0:
                    continue
                kind, pct, months, kacos = window_decision(kws, sb_limit, cfg)
                if pct > 0 and c["seg"] == "pareo" and winter:
                    pct = winter_raise(pct, kws[months - 1]["units"] if months else 0, cfg)
                    if pct <= 0:
                        continue
                if kind == "pause":
                    force_pause = True
                elif abs(pct) < 0.01:
                    continue
                kbasis = f"{months} mo: {kws[months - 1]['units']:.0f} orders, ACOS " + \
                         ("∞" if kacos == math.inf else f"{kacos:.0f}%") + f" vs {sb_limit:.0f}%" + \
                         {"pause": " → pause", "raise": " → few clicks, raise"}.get(kind, "")
            else:
                if pct <= -0.95:   # every product in the ad group: no sales over 3 months, spent a lot
                    force_pause = True
                # Keyword guard: a raise from the products is checked against the
                # keyword's OWN ACOS over 1 -> 2 -> 3 months (>= kwMinClicks clicks):
                # above the ACOS limit -> lowered; no sales and spent >= pauseSpend -> pause.
                limit = acos_limit(state, cc, c["seg"])
                if pct > 0 and limit and (kws[-1].get("clicks") or 0) >= cfg["kwMinClicks"]:
                    kind, kpct, months, kacos = window_decision(kws, limit, cfg)
                    if kind == "pause":
                        force_pause = True
                    elif kind == "change" and kpct < 0:
                        pct = kpct
                    elif kind == "none" and kacos > limit:
                        pct = 0.0
                    if kind in ("change", "pause"):
                        kbasis = f"keyword {months} mo: {kws[months - 1]['units']:.0f} orders, ACOS " + \
                                 ("∞" if kacos == math.inf else f"{kacos:.0f}%") + f" vs {limit:.0f}%"
                if not force_pause and abs(pct) < 0.01:
                    continue
                pct = max(pct, -0.5) if not force_pause else pct
            if force_pause:
                pct = -1.0
            new = round(old * (1 + pct), 2)
            if new == old:
                continue
            why = (", ".join(sku_of.get(a, a) for a in ag_items.get((cid, lv["ad_group_id"]), []))[:80]
                   if not is_sb else f"SB own ACOS vs limit {sb_limit:.0f}%")
            if not is_sb:
                drivers = "; ".join(
                    f"{sku_of.get(a, a)} {items[(cc, a)]['wunits']:.0f}u/{max(items[(cc, a)]['months'], 1)}mo TACOS "
                    + ("∞" if items[(cc, a)]["tacos"] in (None, math.inf) else f"{items[(cc, a)]['tacos']:.0f}%")
                    for a in ag_items.get((cid, lv["ad_group_id"]), []) if (cc, a) in items)[:160]
                if kbasis:
                    drivers = f"{kbasis} | {drivers}"
            else:
                drivers = f"SB {kbasis}"
            x = {"profile_id": c["profile_id"], "ad_product": c["ad_product"], "is_keyword": lv["is_keyword"],
                 "target_id": tid, "campaign_id": cid, "ad_group_id": lv["ad_group_id"], "campaign": c["campaign_name"],
                 "text": tid, "country": cc, "old_bid": old, "new_bid": new,
                 "reason": f"TACOS rule {y}-{m:02d}: {pct * 100:+.0f}% ({why})", "pct": pct, "drivers": drivers,
                 "ag_name": lv.get("ad_group_name") or ""}
            if force_pause:
                x["pause"] = "temporary" if hist[tid] >= cfg["historyOrders"] else "for good"
                x["new_bid"] = new = old
            elif new < min_bid and pct > 0:
                x["new_bid"] = new = min_bid
                if new <= old:
                    continue
            elif new < min_bid:
                x["pause"] = "temporary" if hist[tid] >= cfg["historyOrders"] else "for good"
                x["new_bid"] = old
            groups[(cc, c["seg"] + (" SB" if is_sb else ""), "up" if pct > 0 else "down")].append(x)
            trows.append([cc, c["seg"], c["campaign_name"], tid, why, f"{pct * 100:+.0f}%", old,
                          "pause " + x["pause"] if x.get("pause") else new])
    for xs in groups.values():
        for x in xs:
            x["text"] = names.get(x["target_id"], x["target_id"])
    for r in trows:
        r[3] = names.get(r[3], r[3])

    proposals = []
    for (cc, seg, way), xs in sorted(groups.items()):
        is_sb_group = seg.endswith(" SB")
        base_seg = seg[:-3] if is_sb_group else seg
        d = dept.get((cc, base_seg)) or {}
        tacos = 100 * d.get("spend", 0) / d["sales"] if d.get("sales") else None
        pauses = sum(1 for x in xs if x.get("pause"))
        proposals.append({
            "section": "TACOS bid rule",
            "title": f"{cc} {seg}: {'raise' if way == 'up' else 'lower'} {len(xs) - pauses} bid(s)"
                     + (f", pause {pauses}" if pauses else "")
                     + (f" – SB ACOS limit {sb_acos(state, cc, base_seg):.0f}%" if is_sb_group
                        else f" – TACOS {tacos:.0f}% vs target {target_for(state, cc, seg)}%" if tacos is not None else ""),
            "detail": "; ".join(f"{x['text'][:35]} {x['old_bid']}→{'pause' if x.get('pause') else x['new_bid']}"
                                for x in xs[:12]) + (" …" if len(xs) > 12 else ""),
            "why": (f"{y}-{m:02d}: each SB keyword on its own ACOS vs the SB limit; < {cfg['minUnits']:.0f} orders → 2, then 3 months; "
                    f"still < {cfg['minUnits']:.0f}: spent ≥ {cfg['pauseSpend']:.0f} → pause, < {cfg['lowSpend']:.0f} → +{cfg['lowSpendRaise']:.0f}%") if is_sb_group else
                   (f"{y}-{m:02d}: bid × target ÷ TACOS (SP+SD spend) of the products the ad group sells, capped by units; "
                    f"< {cfg['minUnits']:.0f} units → 2, then 3 months; still < {cfg['minUnits']:.0f}: spent ≥ {cfg['pauseSpend']:.0f} → pause, "
                    f"< {cfg['lowSpend']:.0f} → +{cfg['lowSpendRaise']:.0f}%; keywords with own ACOS above the limit are lowered"),
            "columns": ["Keyword / target", "Campaign", "Products (units, TACOS)", "Clicks", "Orders", "Spend", "Sales",
                        "ACOS", "Bid now", "New bid", "Change"],
            "rows": [[x["text"][:50], x["campaign"], x.get("drivers", ""),
                      round(kw[x["target_id"]]["clicks"]), round(kw[x["target_id"]]["orders"]),
                      round(kw[x["target_id"]]["spend"], 2), round(kw[x["target_id"]]["sales"], 2),
                      (f"{100 * kw[x['target_id']]['spend'] / kw[x['target_id']]['sales']:.0f}%"
                       if kw[x["target_id"]]["sales"] else ("no sales" if kw[x["target_id"]]["spend"] else "-")),
                      x["old_bid"], "pause " + x["pause"] if x.get("pause") else x["new_bid"],
                      "pause" if x.get("pause") else f"{x['pct'] * 100:+.0f}%"]
                     for x in sorted(xs, key=lambda x: -kw[x["target_id"]]["spend"])],
            "rowIds": [x["target_id"] for x in sorted(xs, key=lambda x: -kw[x["target_id"]]["spend"])],
            "action": {"type": "set_bids", "items": [{k: v for k, v in x.items() if k not in ("drivers", "pct", "ag_name")}
                                                     for x in xs]}})
    # temporary pauses of earlier months come back for a decision
    back = [p for p in json.loads(get_job_state(token, PAUSES_KEY) or "[]")
            if p.get("status") == "active" and p.get("paused_on", "") < date(today.year, today.month, 1).isoformat()]
    if back:
        items_back = [{**{k: p[k] for k in ("profile_id", "ad_product", "is_keyword", "target_id", "campaign_id",
                                             "ad_group_id", "campaign", "text", "country", "old_bid")},
                       "new_bid": p["old_bid"], "unpause": True,
                       "reason": f"TACOS rule: back after the temporary pause of {p['paused_on']}"} for p in back]
        proposals.append({"section": "TACOS bid rule", "title": f"Unpause {len(items_back)} temporarily paused keyword(s)",
                          "detail": "; ".join(f"{x['country']} {x['text'][:35]} @ {x['old_bid']}" for x in items_back[:12]),
                          "action": {"type": "set_bids", "items": items_back}})
    lines = [f"🎯 TACOS bid rule {y}-{m:02d} (target per marketplace × department = {cfg['profitPct']:.0f}% profit; "
             f"change = target/TACOS, capped {cfg['capAtMinUnits']:.0f}% at {cfg['minUnits']:.0f} units → "
             f"{cfg['capAtFullUnits']:.0f}% at {cfg['fullUnits']:.0f}+): "
             f"{sum(len(p['action']['items']) for p in proposals)} bids in {len(proposals)} group(s)"]
    for (cc, seg), (tacos, target, pct) in sorted(dept_pct.items()):
        if target is None or not dept[(cc, seg)]["sales"]:
            continue
        mark = "🟢" if tacos <= target else ("🟠" if tacos <= target + cfg["orangeBand"] else "🔴")
        lines.append(f"{mark} {cc} {seg}: TACOS {tacos:.0f}% vs target {target:.0f}% "
                     f"({dept[(cc, seg)]['units']:.0f} units)")
    lines += [f"• {p['title']}" for p in proposals]
    table = {"title": f"TACOS bid rule {y}-{m:02d}", "rows": trows,
             "columns": ["Country", "Dept", "Campaign", "Keyword / target", "Items", "Change", "Bid now", "New bid"]}
    return lines, table, proposals


def SendTacosBidRule(request):
    """Monthly (2nd): the TACOS bid rule for last month -> /ads-reviews proposals."""
    from CheckReports import save_report
    from MonthlyChecks import last_month
    args = getattr(request, "args", {}) or {}
    token = pb_authenticate()
    y, m = (int(args["year"]), int(args["month"])) if args.get("year") else last_month()
    lines, table, props = bid_rule(token, y, m)
    for i, p in enumerate(props, 1):
        p.update(id=i, status="proposed")
    text = "\n".join(lines)
    if str(args.get("dry")) != "1":
        save_report("monthly", f"{y}-{m:02d}", "tacos-bids", "🎯 TACOS bid rule", text, token=token,
                    proposals=props, tables=[table])
    return json.dumps({"text": text, "proposals": len(props)}), 200, {"Content-Type": "application/json"}


def UpdateTacosTargets(request):
    state = compute_targets()
    return json.dumps(state), 200, {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}


def GetTacosTargets(request):
    if getattr(request, "method", "GET") == "OPTIONS":
        return "", 204, {"Access-Control-Allow-Origin": "*"}
    try:
        return json.dumps(load_targets(pb_authenticate())), 200, {"Content-Type": "application/json",
                                                                  "Access-Control-Allow-Origin": "*"}
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}


def GetAdsRulesConfig(request):
    if getattr(request, "method", "GET") == "OPTIONS":
        return "", 204, {"Access-Control-Allow-Origin": "*"}
    return json.dumps({"config": config(), "defaults": DEFAULTS}), 200, {"Content-Type": "application/json",
                                                                          "Access-Control-Allow-Origin": "*"}


def SetAdsRulesConfig(request):
    """POST ?key=PROPOSAL_KEY {name: value, ...} - unknown names are ignored."""
    import os
    h = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*",
         "Access-Control-Allow-Methods": "POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type"}
    if getattr(request, "method", "POST") == "OPTIONS":
        return "", 204, h
    if (getattr(request, "args", {}) or {}).get("key") != os.environ.get("PROPOSAL_KEY", "~"):
        return json.dumps({"error": "Unauthorized"}), 401, h
    body = request.get_json(silent=True) or {}
    token = pb_authenticate()
    cfg = config(token)
    for k, v in body.items():
        if k in DEFAULTS:
            cfg[k] = v if isinstance(DEFAULTS[k], str) else float(v)
    set_job_state(token, CONFIG_KEY, json.dumps(cfg))
    return json.dumps({"config": cfg}), 200, h
