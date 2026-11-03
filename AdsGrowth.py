"""Growth side of the ads checks (per the user 2026-11-02: "where do we raise
bids for good keywords?", keyword harvest with checkboxes, and pauses coming
back). Every block returns (lines, table, proposals); proposals carry an
`action` that CheckReports.ApplyApprovedProposals runs after the user ticks it
on /checks (cron every 10 min).

1. Bid raises (rule OK'd by the user 2026-11-02): last 30 days ending 7 days
   ago, SP + SB keywords/targets in ENABLED campaigns: >= 3 orders and ACOS <=
   70% of target (home decor 35%, pareo 30%) -> +15%; >= 5 orders and ACOS <=
   50% of target -> +25%. Not changed in the last 14 days, bid max 2.00, pareo
   only from the season restart (2027-02-20). One proposal per campaign.
   Monthly review + every 2nd Monday.
2. Keyword harvest (USA home decor plan rule, all countries): search terms of
   the last 30 days (ending 2 days ago) with >= 2 orders, >= 3 clicks, ACOS <=
   target and no exact keyword yet -> exact keyword in the country's EXACT SP
   campaign of that department (bid = CPC + 10%) + negative exact in the SP
   auto/broad/phrase campaign it came from; >= 15 clicks, >= 10 spend, 0 orders,
   still spending in the last 14 days -> negative exact in that SP campaign.
   Weekly + monthly.
3. Coming back: temporary product-ad pauses (job_state product_ad_pauses) and
   temporary bid cuts (temp_cuts) that are due -> proposal to unpause/restore;
   the next 30 days are listed. Weekly."""
import json
from collections import defaultdict
from datetime import date, timedelta

import requests

from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state, set_job_state

PAREO_WORDS = ("PAREO", "SARONG", "BEACH")
TARGET = {"home": 35, "pareo": 30}  # fallback only - the ACOS limit now comes from the TACOS target (TargetTacos)


def acos_target(pb, cc, seg, product=None, _cache={}):
    """Keyword ACOS limit derived from the marketplace-department TACOS target
    (per the user 2026-11-03: every ad recommendation looks at TACOS)."""
    if "state" not in _cache:
        from TargetTacos import load_targets
        try:
            _cache["state"] = load_targets(pb)
        except Exception:
            _cache["state"] = {}
    from TargetTacos import acos_limit, sb_acos
    if product == "SPONSORED_BRANDS":  # SB = independent sales, own 15%-profit ACOS limit (2026-11-03)
        sb = sb_acos(_cache["state"], cc, seg)
        if sb and sb > 0:
            return sb
    return acos_limit(_cache["state"], cc, seg, TARGET[seg])
SEASON_RESTART = "2027-02-20"
MAX_BID = 2.0
SCALE = {"SE": 11, "PL": 4.3, "MX": 20, "CA": 1.4}
MIN_BID = {"US": 0.20, "CA": 0.20, "MX": 2.0, "SE": 1.5, "PL": 0.6}  # others 0.15


def _media(name):
    return f"application/vnd.{name}.v3+json"


def seg_of(text):
    return "pareo" if any(w in (text or "").upper() for w in PAREO_WORDS) else "home"


def winter(today, seg):
    return seg == "pareo" and today.isoformat() < SEASON_RESTART


class Api:
    def __init__(self, pb):
        self.pb, self.conns = pb, {}

    def ctx(self, profile):
        from AdsBidWriter import find_profile_connection, refresh_access_token
        if profile not in self.conns:
            c = find_profile_connection(self.pb, profile)
            self.conns[profile] = (c, refresh_access_token(c["profile_key"], c["refresh_token"]))
        return self.conns[profile]

    def call(self, profile, method, path, body=None, media=None, params=None):
        c, tok = self.ctx(profile)
        h = {"Authorization": f"Bearer {tok}", "Amazon-Advertising-API-ClientId": c["client_id"],
             "Amazon-Advertising-API-Scope": str(profile)}
        if media:
            h.update({"Content-Type": media, "Accept": media})
        r = requests.request(method, c["base_url"] + path, json=body, params=params, timeout=90, headers=h)
        if r.status_code >= 400:
            raise RuntimeError(f"{path}: HTTP {r.status_code} {r.text[:300]}")
        return r.json() if r.content else {}

    def live_targets(self, profile, campaign_id, ad_product):
        """{target_id: {bid, is_keyword, ad_group_id}} for ENABLED keywords/targets of a campaign."""
        out = {}
        if ad_product == "SPONSORED_PRODUCTS":
            flt = {"campaignIdFilter": {"include": [campaign_id]}, "stateFilter": {"include": ["ENABLED"]}, "maxResults": 1000}
            ags = {a["adGroupId"]: a.get("defaultBid") for a in self.call(
                profile, "POST", "/sp/adGroups/list", {"campaignIdFilter": {"include": [campaign_id]}},
                _media("spAdGroup")).get("adGroups", [])}
            for k in self.call(profile, "POST", "/sp/keywords/list", flt, _media("spKeyword")).get("keywords", []):
                out[str(k["keywordId"])] = {"bid": k.get("bid") or ags.get(k["adGroupId"]), "is_keyword": True,
                                            "ad_group_id": str(k["adGroupId"])}
            for t in self.call(profile, "POST", "/sp/targets/list", flt, _media("spTargetingClause")).get("targetingClauses", []):
                out[str(t["targetId"])] = {"bid": t.get("bid") or ags.get(t["adGroupId"]), "is_keyword": False,
                                           "ad_group_id": str(t["adGroupId"])}
        else:
            for k in self.call(profile, "GET", "/sb/keywords", params={"campaignIdFilter": campaign_id, "count": 1000}) or []:
                if k.get("state") == "enabled":
                    out[str(k["keywordId"])] = {"bid": k.get("bid"), "is_keyword": True, "ad_group_id": str(k["adGroupId"])}
            r = self.call(profile, "POST", "/sb/targets/list", {"filters": [{"filterType": "CAMPAIGN_ID", "values": [campaign_id]}]})
            for t in r.get("targets", []) if isinstance(r, dict) else []:
                if (t.get("state") or "").lower() == "enabled":
                    out[str(t["targetId"])] = {"bid": t.get("bid"), "is_keyword": False, "ad_group_id": str(t["adGroupId"])}
        return out


def _campaigns(pb):
    camps = {c["campaign_id"]: c for c in pb_list_all(pb, "ads_campaigns", 'campaign_id != ""', fields=(
        "campaign_id,campaign_name,campaign_status,portfolio_id,profile_id,country_code,ad_product,targeting_type"))}
    ports = {p["portfolio_id"]: p["name"] for p in pb_list_all(pb, "ads_portfolios", 'portfolio_id != ""', fields="portfolio_id,name")}
    for c in camps.values():
        c["seg"] = seg_of(f"{ports.get(c.get('portfolio_id'), '')} {c['campaign_name']}")
    return camps


def _dedupe(rows, key):
    best = {}
    for r in rows:
        k = key(r)
        if k not in best or str(r["profile_id"]) < str(best[k]["profile_id"]):
            best[k] = r
    return best.values()


def _recently_changed(pb, today, days=14):
    since = (today - timedelta(days=days)).isoformat()
    return {r["target_id"] for r in pb_list_all(pb, "ads_bid_change_log", f'changed_at >= "{since}"', fields="target_id")}


# ------------------------------------------------------------------ bid raises
def raise_block(pb, today, api=None):
    api = api or Api(pb)
    end = today - timedelta(days=7)
    start = end - timedelta(days=29)
    camps = _campaigns(pb)
    changed = _recently_changed(pb, today)
    rows = pb_list_all(pb, "ads_keyword_stats", f'date >= "{start}" && date <= "{end}"',
                       fields="profile_id,campaign_id,campaign_name,ad_group_id,ad_product,target_id,target_text,match_type,"
                              "country_code,date,clicks,orders,spend,sales")
    agg = defaultdict(lambda: {"clicks": 0, "orders": 0, "spend": 0.0, "sales": 0.0})
    for r in _dedupe(rows, lambda r: (r["campaign_id"], r["target_id"], r["date"])):
        if r.get("ad_product") not in ("SPONSORED_PRODUCTS", "SPONSORED_BRANDS"):
            continue
        a = agg[(r["campaign_id"], r["target_id"])]
        for k in ("clicks", "orders", "spend", "sales"):
            a[k] += r.get(k) or 0
        a.update(text=r.get("target_text") or "", match=r.get("match_type") or "", cc=r["country_code"].upper(),
                 product=r["ad_product"], ag=r["ad_group_id"])
    cands = defaultdict(list)
    for (cid, tid), a in agg.items():
        c = camps.get(cid)
        if not c or c.get("campaign_status") != "ENABLED" or tid in changed or not a["sales"]:
            continue
        if a["match"] == "THEME" or "KEYWORDS_RELATED" in a["text"].upper() or "keywords-related" in a["text"].lower() \
                or a["text"].strip() == "*":
            continue
        if winter(today, c["seg"]):
            continue
        acos = 100 * a["spend"] / a["sales"]
        t = acos_target(pb, a["cc"], c["seg"], a["product"])
        a["target"] = t
        pct = 25 if a["orders"] >= 5 and acos <= 0.5 * t else 15 if a["orders"] >= 3 and acos <= 0.7 * t else 0
        if pct:
            cands[cid].append((tid, a, acos, pct))
    proposals, trows = [], []
    for cid, items in cands.items():
        c = camps[cid]
        try:
            live = api.live_targets(c["profile_id"], cid, c["ad_product"])
        except Exception as exc:
            print("raise_block live bids failed", c["campaign_name"], exc)
            continue
        acts = []
        for tid, a, acos, pct in sorted(items, key=lambda x: -x[1]["orders"]):
            lv = live.get(tid)
            if not lv or not lv.get("bid"):
                continue
            old = lv["bid"]
            new = min(round(old * (1 + pct / 100), 2), MAX_BID)
            if new <= old:
                continue
            acts.append({"profile_id": c["profile_id"], "ad_product": c["ad_product"], "is_keyword": lv["is_keyword"],
                         "target_id": tid, "campaign_id": cid, "ad_group_id": lv["ad_group_id"], "campaign": c["campaign_name"],
                         "text": a["text"], "country": a["cc"], "old_bid": old, "new_bid": new})
            trows.append([f"{a['cc']} {c['campaign_name']}", a["text"][:60], {"TARGETING_EXPRESSION_PREDEFINED": "auto", "TARGETING_EXPRESSION": "product"}.get(a["match"], (a["match"] or "").lower()),
                          round(a["orders"]), round(a["clicks"]), round(a["spend"], 2), round(a["sales"], 2),
                          f"{acos:.0f}%", f"{a['target']:.0f}%",
                          round(a["spend"] / a["clicks"], 2) if a["clicks"] else None, old, new, f"+{pct}%"])
        if acts:
            # Per the user 2026-11-03 ("give more info on bid raises"): the card
            # on /ads-reviews shows one row per keyword with its numbers.
            prows = [r[1:] for r in trows[-len(acts):]]
            proposals.append({"section": "Bid raises", "title": f"{acts[0]['country']} {c['campaign_name']}: raise {len(acts)} bid(s)",
                              "why": f"{start:%d %b} – {end:%d %b}: ≥3 orders & ACOS ≤ 70% of target → +15%; "
                                     f"≥5 orders & ACOS ≤ 50% of target → +25%",
                              "columns": ["Keyword / target", "Match", "Orders", "Clicks", "Spend", "Sales", "ACOS", "Target",
                                          "CPC", "Bid now", "New bid", "Raise"],
                              "rows": prows,
                              "detail": "; ".join(f"{x['text'][:40]} {x['old_bid']}→{x['new_bid']}" for x in acts[:10])
                                        + (" …" if len(acts) > 10 else ""),
                              "action": {"type": "set_bids", "items": acts}})
    trows.sort(key=lambda r: (r[0], -r[3]))
    lines = [f"📈 Bid raises – good keywords {start} – {end} (≥3 orders & ACOS ≤ 70% of target → +15%; ≥5 orders & ≤ 50% → +25%;"
             f" not changed in 14 days; max {MAX_BID:.2f}; pareo from {SEASON_RESTART}): "
             f"{sum(len(p['action']['items']) for p in proposals)} in {len(proposals)} campaign(s)"]
    lines += [f"• {p['title']}: {p['detail']}" for p in proposals]
    table = {"title": f"Bid raises {start} – {end}", "rows": trows,
             "columns": ["Campaign", "Keyword / target", "Match", "Orders", "Clicks", "Spend", "Sales", "ACOS", "Target", "CPC",
                        "Bid now", "New bid", "Raise"]}
    return lines, table, proposals


# ------------------------------------------------------------- keyword harvest
def _exact_destination(api, camps, cc, seg, cache={}):
    """(campaign, ad_group_id) of the country's manual SP EXACT campaign for the department."""
    key = (cc, seg)
    if key in cache:
        return cache[key]
    opts = [c for c in camps.values() if c.get("country_code", "").upper() == cc and c["seg"] == seg
            and c.get("campaign_status") == "ENABLED" and c.get("ad_product") == "SPONSORED_PRODUCTS"
            and "EXACT" in c["campaign_name"].upper() and (c.get("targeting_type") or "").upper() != "AUTO"]
    opts.sort(key=lambda c: ("WINNER" not in c["campaign_name"].upper(), c["campaign_name"]))
    dest = None
    for c in opts:
        try:
            ags = [a for a in api.call(c["profile_id"], "POST", "/sp/adGroups/list", {
                "campaignIdFilter": {"include": [c["campaign_id"]]}, "stateFilter": {"include": ["ENABLED"]}},
                _media("spAdGroup")).get("adGroups", [])]
        except Exception:
            continue
        if ags:
            ags.sort(key=lambda a: ("LONG" in (a.get("name") or "").upper(), a.get("name") or ""))
            dest = (c, str(ags[0]["adGroupId"]))
            break
    cache[key] = dest
    return dest


def harvest_block(pb, today, api=None):
    api = api or Api(pb)
    end = today - timedelta(days=2)
    start = end - timedelta(days=29)
    recent = (end - timedelta(days=13)).isoformat()
    camps = _campaigns(pb)
    rows = pb_list_all(pb, "ads_search_term_stats", f'date >= "{start}" && date <= "{end}"')
    exact = defaultdict(set)
    for r in pb_list_all(pb, "ads_keyword_stats", f'date >= "{(today - timedelta(days=60)).isoformat()}" && match_type = "EXACT"',
                         fields="country_code,target_text"):
        exact[r["country_code"].upper()].add((r.get("target_text") or "").lower().strip())
    agg = defaultdict(lambda: {"clicks": 0, "orders": 0, "spend": 0.0, "sales": 0.0, "recent_spend": 0.0})
    for r in _dedupe(rows, lambda r: (r["campaign_id"], r["ad_group_id"], r["target_id"], r["search_term"], r["date"])):
        term = (r.get("search_term") or "").lower().strip()
        if not term or (term.startswith("b0") and len(term) == 10):
            continue
        a = agg[(r["country_code"].upper(), r["campaign_id"], term)]
        for k in ("clicks", "orders", "spend", "sales"):
            a[k] += r.get(k) or 0
        if r["date"] >= recent:
            a["recent_spend"] += r.get("spend") or 0
        a.update(camp=r["campaign_name"], product=r.get("ad_product"), match=r.get("match_type") or "")
    adds, negs = {}, defaultdict(list)
    for (cc, cid, term), a in agg.items():
        c = camps.get(cid) or {"seg": seg_of(a["camp"]), "campaign_name": a["camp"], "profile_id": None}
        acos = 100 * a["spend"] / a["sales"] if a["sales"] else None
        if (a["orders"] >= 2 and a["clicks"] >= 3 and acos is not None and acos <= acos_target(pb, cc, c["seg"])
                and term not in exact[cc] and not winter(today, c["seg"])):
            prev = adds.get((cc, term))
            if not prev or a["orders"] > prev["orders"]:
                adds[(cc, term)] = {**a, "cid": cid, "acos": acos, "seg": c["seg"]}
        if a["clicks"] >= 15 and a["spend"] >= 10 * SCALE.get(cc, 1) and a["orders"] == 0 and a["recent_spend"] > 0:
            negs[(cc, cid)].append((term, dict(a)))
    proposals, trows = [], []
    by_dest = defaultdict(list)
    for (cc, term), a in sorted(adds.items(), key=lambda kv: -kv[1]["orders"]):
        dest = _exact_destination(api, camps, cc, a["seg"])
        cpc = a["spend"] / a["clicks"] if a["clicks"] else 0.3
        bid = round(min(max(cpc * 1.1, MIN_BID.get(cc, 0.15)), 1.5 * SCALE.get(cc, 1)), 2)
        src = camps.get(a["cid"], {})
        neg_src = a["cid"] if (src.get("ad_product") == "SPONSORED_PRODUCTS" and a["match"] != "EXACT"
                               and (not dest or a["cid"] != dest[0]["campaign_id"])) else None
        item = {"keyword": term, "bid": bid, "country": cc, "orders": a["orders"], "acos": round(a["acos"]),
                "source": a["camp"], "neg_campaign_id": neg_src, "neg_profile_id": src.get("profile_id") if neg_src else None}
        trows.append([cc, "➕ exact", term, round(a["orders"]), round(a["clicks"]), round(a["spend"], 2), f"{a['acos']:.0f}%",
                      a["camp"], f"{dest[0]['campaign_name']} @ {bid}" if dest else "no EXACT campaign - Claude creates it"])
        by_dest[(cc, a["seg"], dest[0]["campaign_id"] if dest else None)].append((item, dest))
    for (cc, seg, dcid), items in by_dest.items():
        dest = items[0][1]
        its = [x for x, _d in items]
        if dest:
            c, ag = dest
            for x in its:
                x.update(profile_id=c["profile_id"], campaign_id=c["campaign_id"], ad_group_id=ag, campaign=c["campaign_name"])
            proposals.append({"section": "Keyword harvest", "title": f"{cc} {c['campaign_name']}: add {len(its)} exact keyword(s)",
                              "detail": "; ".join(f"{x['keyword']} ({x['orders']} orders, ACOS {x['acos']}%) @ {x['bid']}" for x in its[:12])
                                        + " - plus negative exact where it came from (auto/broad/phrase)",
                              "action": {"type": "add_exact_keywords", "items": its}})
        else:
            proposals.append({"section": "Keyword harvest", "title": f"{cc} {'pareo' if seg == 'pareo' else 'home decor'}: "
                                                                     f"{len(its)} exact keyword(s) - no EXACT campaign yet",
                              "detail": "; ".join(f"{x['keyword']} ({x['orders']} orders)" for x in its[:12]) +
                                        " - Claude adds them to a new/other campaign after approval"})
    for (cc, cid), items in negs.items():
        c = camps.get(cid, {})
        items = [(t, a) for t, a in items if (cc, t) not in adds]
        if not items:
            continue
        sp = c.get("ad_product") == "SPONSORED_PRODUCTS"
        acts, manual = [], []
        for t, a in items:
            if a["match"] == "EXACT":  # our own exact keyword - a negative would switch it off
                how = "our exact keyword with 0 orders → lower / pause it (Claude)"
                manual.append((t, a, "lower/pause the exact keyword"))
            elif not sp:
                how = "SB campaign → Claude adds the negative"
                manual.append((t, a, "SB negative"))
            else:
                how = "negative exact in this campaign" + (" (we also bid on it as exact elsewhere)" if t in exact[cc] else "")
                acts.append({"profile_id": c.get("profile_id"), "campaign_id": cid, "keyword": t, "country": cc,
                             "campaign": a["camp"]})
            trows.append([cc, "➖ negative", t, 0, round(a["clicks"]), round(a["spend"], 2), "-", a["camp"], how])
        if acts:
            proposals.append({"section": "Keyword harvest", "title": f"{cc} {items[0][1]['camp']}: {len(acts)} negative exact",
                              "detail": "; ".join(f"{x['keyword']} ({items_d[x['keyword']]['clicks']:.0f} clicks, "
                                                  f"{items_d[x['keyword']]['spend']:.2f}, 0 orders)"
                                                  for x in acts[:12] for items_d in [dict(items)]),
                              "action": {"type": "add_negative_exact", "items": acts}})
        if manual:
            proposals.append({"section": "Keyword harvest", "title": f"{cc} {items[0][1]['camp']}: {len(manual)} search term(s) "
                                                                     f"with clicks and no orders (done by Claude)",
                              "detail": "; ".join(f"{t} ({a['clicks']:.0f} clicks, {a['spend']:.2f}) - {w}" for t, a, w in manual[:12])})
    lines = [f"🌾 Keyword harvest {start} – {end}: ➕ {len(adds)} new exact keyword(s), "
             f"➖ {sum(len(v) for v in negs.values())} negative(s)"
             + (f" (pareo additions wait for the season restart {SEASON_RESTART})" if today.isoformat() < SEASON_RESTART else ""),
             "Rules: ➕ ≥2 orders, ≥3 clicks, ACOS ≤ 35% home decor / 30% pareo, not an exact keyword yet → exact in the "
             "EXACT campaign (bid = CPC +10%) + negative exact where it came from; ➖ ≥15 clicks, ≥10 spent, 0 orders, "
             "still spending in the last 14 days → negative exact in that campaign."]
    lines += [f"• {p['title']}: {p['detail']}" for p in proposals]
    table = {"title": f"Keyword harvest {start} – {end}", "rows": trows,
             "columns": ["MP", "Kind", "Search term", "Orders", "Clicks", "Spend", "ACOS", "From campaign", "Goes to"]}
    return lines, table, proposals


# ----------------------------------------------------------------- coming back
def comeback_block(pb, today):
    t = today.isoformat()
    soon = (today + timedelta(days=30)).isoformat()
    week_ago = (today - timedelta(days=7)).isoformat()
    pauses = json.loads(get_job_state(pb, "product_ad_pauses") or "[]")
    cuts = json.loads(get_job_state(pb, "temp_cuts") or "[]")
    due_p = [p for p in pauses if p["status"] == "active" and p["unpause_on"] <= t]
    due_c = [c for c in cuts if c.get("status") == "active" and c["restore_on"] <= t]
    trows, proposals = [], []
    for p in sorted(due_p, key=lambda p: (p["country"], p["campaign"])):
        trows.append(["▶️ due now", p["country"], "product ad", p["sku"] or p["asin"], p["campaign"], p["paused_on"], p["unpause_on"]])
    for c in due_c:
        trows.append(["▶️ due now", c["country"], "bid cut", c["text"], c["campaign"], c["cut_date"],
                      f"{c['restore_on']} ({c['new_bid']}→{c['old_bid']})"])
    for p in sorted([p for p in pauses if p["status"] == "active" and t < p["unpause_on"] <= soon], key=lambda p: p["unpause_on"]):
        trows.append(["⏳ next 30 days", p["country"], "product ad", p["sku"] or p["asin"], p["campaign"], p["paused_on"], p["unpause_on"]])
    for c in sorted([c for c in cuts if c.get("status") == "active" and t < c["restore_on"] <= soon], key=lambda c: c["restore_on"]):
        trows.append(["⏳ next 30 days", c["country"], "bid cut", c["text"], c["campaign"], c["cut_date"], c["restore_on"]])
    for p in pauses:
        if p["status"] == "unpaused" and p.get("unpaused_on", "") >= week_ago:
            trows.append(["✅ came back", p["country"], "product ad", p["sku"] or p["asin"], p["campaign"], p["paused_on"],
                          f"{p['unpaused_on']} (re-check {p.get('recheck_on', '')})"])
    for c in cuts:
        if c.get("status") == "restored" and c.get("restored_on", "") >= week_ago:
            trows.append(["✅ came back", c["country"], "bid cut", c["text"], c["campaign"], c["cut_date"], c["restored_on"]])
    later = defaultdict(int)
    for p in pauses:
        if p["status"] == "active" and p["unpause_on"] > soon:
            later[p["unpause_on"]] += 1
    by_country = defaultdict(list)
    for p in due_p:
        by_country[p["country"]].append(p)
    for cc, ps in sorted(by_country.items()):
        proposals.append({"section": "Coming back", "title": f"{cc}: unpause {len(ps)} product ad(s)",
                          "detail": "; ".join(f"{p['sku'] or p['asin']} in {p['campaign']} (paused {p['paused_on']})" for p in ps[:12]),
                          "action": {"type": "unpause_product_ads", "keys": [p["key"] for p in ps]}})
    if due_c:
        proposals.append({"section": "Coming back", "title": f"Restore {len(due_c)} temporary bid cut(s)",
                          "detail": "; ".join(f"{c['country']} {c['text']} {c['new_bid']}→{c['old_bid']}" for c in due_c[:12]),
                          "action": {"type": "restore_temp_cuts", "ids": [c["target_id"] for c in due_c]}})
    lines = [f"▶️ Coming back – paused product ads and temporary bid cuts ({t})",
             f"Due now: {len(due_p)} product ad(s), {len(due_c)} bid cut(s) – tick to unpause / restore (applied within 10 min)."]
    lines += [f"  {r[0]} {r[1]} {r[2]} {r[3]} – {r[4]} (paused {r[5]}, back {r[6]})" for r in trows]
    if later:
        lines.append("Later: " + ", ".join(f"{n} on {d}" for d, n in sorted(later.items())))
    if not trows:
        lines.append("Nothing due in the next 30 days.")
    table = {"title": "Paused product ads and bid cuts coming back", "rows": trows,
             "columns": ["When", "MP", "Kind", "Product / keyword", "Campaign", "Paused", "Back on"]}
    if later:
        table["title"] += " (later: " + ", ".join(f"{n} on {d}" for d, n in sorted(later.items())) + ")"
    return lines, table, proposals


# --------------------------------------------------------------------- actions
ACTIONS = ("set_bids", "add_exact_keywords", "add_negative_exact", "unpause_product_ads", "restore_temp_cuts")


def apply(pb, action, label):
    from AdsBidWriter import record_bid_change, write_bid_or_state
    api = Api(pb)
    done, failed = [], []
    today = date.today().isoformat()
    typ = action["type"]
    if typ == "set_bids":
        changed = _recently_changed(pb, date.today())  # the same raise can sit in the weekly + monthly page
        for x in action["items"]:
            if x["target_id"] in changed:
                failed.append(f"{x['text']}: skipped - bid already changed in the last 14 days")
                continue
            try:
                c, tok = api.ctx(x["profile_id"])
                state = None
                if x.get("pause"):
                    state = "PAUSED"
                elif x.get("unpause"):
                    state = "ENABLED"
                resp = write_bid_or_state(c, tok, x["profile_id"], x["ad_product"], x["is_keyword"], x["target_id"],
                                          x["campaign_id"], x["ad_group_id"], x["new_bid"], state=state)
                why = x.get("reason") or "bid raise (good keyword)"
                if x.get("pause"):
                    why += f" - paused {x['pause']} (bid would go below Amazon's minimum)"
                record_bid_change(pb, {"action": "disable" if x.get("pause") else "bid_change", "ad_product": x["ad_product"],
                                       "campaign_id": x["campaign_id"],
                                       "campaign_name": x["campaign"], "changed_at": today, "country_code": x["country"],
                                       "profile_id": x["profile_id"], "target_id": x["target_id"], "target_text": x["text"],
                                       "old_bid": x["old_bid"], "new_bid": x["new_bid"], "status": "applied",
                                       "reason": f"{label}: {why}, approved on the page",
                                       "amazon_response": json.dumps(resp)[:2000]})
                if x.get("pause") == "temporary":
                    from TargetTacos import PAUSES_KEY
                    ps = json.loads(get_job_state(pb, PAUSES_KEY) or "[]")
                    ps = [p for p in ps if p["target_id"] != x["target_id"]]
                    ps.append({**x, "paused_on": today, "status": "active"})
                    set_job_state(pb, PAUSES_KEY, json.dumps(ps))
                elif x.get("unpause"):
                    from TargetTacos import PAUSES_KEY
                    ps = json.loads(get_job_state(pb, PAUSES_KEY) or "[]")
                    for p in ps:
                        if p["target_id"] == x["target_id"]:
                            p.update(status="unpaused", unpaused_on=today)
                    set_job_state(pb, PAUSES_KEY, json.dumps(ps))
                done.append(f"{x['text']} " + (f"paused ({x['pause']})" if x.get("pause") else
                                               "unpaused" if x.get("unpause") else f"{x['old_bid']}→{x['new_bid']}"))
            except Exception as exc:
                failed.append(f"{x['text']}: {str(exc)[:120]}")
    elif typ == "add_exact_keywords":
        for x in action["items"]:
            try:
                r = api.call(x["profile_id"], "POST", "/sp/keywords", {"keywords": [{
                    "campaignId": x["campaign_id"], "adGroupId": x["ad_group_id"], "keywordText": x["keyword"],
                    "matchType": "EXACT", "state": "ENABLED", "bid": x["bid"]}]}, _media("spKeyword"))
                res = r.get("keywords", {})
                if res.get("error"):
                    raise RuntimeError(json.dumps(res["error"])[:200])
                kid = (res.get("success") or [{}])[0].get("keywordId", "")
                record_bid_change(pb, {"action": "add_keyword", "ad_product": "SPONSORED_PRODUCTS", "campaign_id": x["campaign_id"],
                                       "campaign_name": x["campaign"], "changed_at": today, "country_code": x["country"],
                                       "profile_id": x["profile_id"], "target_id": str(kid), "target_text": x["keyword"],
                                       "old_bid": 0, "new_bid": x["bid"], "status": "applied",
                                       "reason": f"{label}: harvest - search term {x['orders']} orders, ACOS {x['acos']}% in {x['source']}",
                                       "amazon_response": json.dumps(r)[:2000]})
                msg = f"+{x['keyword']} @ {x['bid']}"
                if x.get("neg_campaign_id"):
                    _neg(api, pb, x["neg_profile_id"], x["neg_campaign_id"], x["keyword"], x["country"], x["source"], label)
                    msg += f" (negative in {x['source']})"
                done.append(msg)
            except Exception as exc:
                failed.append(f"{x['keyword']}: {str(exc)[:120]}")
    elif typ == "add_negative_exact":
        for x in action["items"]:
            try:
                _neg(api, pb, x["profile_id"], x["campaign_id"], x["keyword"], x["country"], x["campaign"], label)
                done.append(f"-{x['keyword']}")
            except Exception as exc:
                failed.append(f"{x['keyword']}: {str(exc)[:120]}")
    elif typ == "unpause_product_ads":
        from ProductAdPauses import RECHECK_DAYS, _set_state
        pauses = json.loads(get_job_state(pb, "product_ad_pauses") or "[]")
        for p in pauses:
            if p["key"] in action["keys"] and p["status"] == "active":
                try:
                    _set_state(pb, p, "ENABLED", f"{label}: unpause after product pause of {p['paused_on']}, approved on /checks")
                    p.update(status="unpaused", unpaused_on=today,
                             recheck_on=(date.today() + timedelta(days=RECHECK_DAYS)).isoformat())
                    done.append(f"{p['sku'] or p['asin']} in {p['campaign']}")
                except Exception as exc:
                    failed.append(f"{p['sku']}: {str(exc)[:120]}")
        set_job_state(pb, "product_ad_pauses", json.dumps(pauses))
    elif typ == "restore_temp_cuts":
        from TempBidCuts import RECHECK_DAYS as RC, _write
        cuts = json.loads(get_job_state(pb, "temp_cuts") or "[]")
        for c in cuts:
            if c["target_id"] in action["ids"] and c.get("status") == "active":
                try:
                    _write(pb, c, c["old_bid"], f"{label}: restore after temporary cut of {c['cut_date']}, approved on /checks")
                    c.update(status="restored", restored_on=today, recheck_on=(date.today() + timedelta(days=RC)).isoformat())
                    done.append(f"{c['text']} {c['new_bid']}→{c['old_bid']}")
                except Exception as exc:
                    failed.append(f"{c['text']}: {str(exc)[:120]}")
        set_job_state(pb, "temp_cuts", json.dumps(cuts))
    return done, failed


def _neg(api, pb, profile, campaign_id, keyword, cc, campaign, label):
    from AdsBidWriter import record_bid_change
    r = api.call(profile, "POST", "/sp/campaignNegativeKeywords", {"campaignNegativeKeywords": [{
        "campaignId": campaign_id, "keywordText": keyword, "matchType": "NEGATIVE_EXACT", "state": "ENABLED"}]},
        _media("spCampaignNegativeKeyword"))
    res = r.get("campaignNegativeKeywords", {})
    if res.get("error"):
        err = json.dumps(res["error"])
        if "DUPLICATE" not in err.upper():
            raise RuntimeError(err[:200])
    record_bid_change(pb, {"action": "add_negative", "ad_product": "SPONSORED_PRODUCTS", "campaign_id": campaign_id,
                           "campaign_name": campaign, "changed_at": date.today().isoformat(), "country_code": cc,
                           "profile_id": profile, "target_id": f"neg:{campaign_id}:{keyword}"[:200], "target_text": keyword,
                           "old_bid": 0, "new_bid": 0, "status": "applied", "reason": f"{label}: negative exact (harvest)",
                           "amazon_response": json.dumps(r)[:2000]})


# ------------------------------------------------------------- weekly entries
def _weekly(job, label, text, tables, proposals, today):
    from CheckReports import save_report
    for i, p in enumerate(proposals, 1):
        p.update(id=i, status="proposed")
    save_report("weekly", today.isoformat(), job, label, text, proposals=proposals, tables=tables)


def comebacks_weekly(today=None, store=True):
    today = today or date.today()
    pb = pb_authenticate()
    lines, table, props = comeback_block(pb, today)
    text = "\n".join(lines)
    if store:
        _weekly("SendComebacksWeekly", "▶️ Coming back (pauses / bid cuts)", text, [table], props, today)
    return text, props
