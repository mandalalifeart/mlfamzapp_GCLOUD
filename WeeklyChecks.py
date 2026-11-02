"""Weekly Monday checks added 2026-10-25 (per the user: "build it all" from
plans/recurring-checks.md). All report-only; they run under the combined
Monday digest (TELEGRAM_CAPTURE=monday, see MondayDigest.py).

  SendAccountHealthWeekly  - seller performance report per marketplace
  SendListingHealthWeekly  - stock but not buyable, Buy Box lost, other sellers on our ASINs
  SendCouponHealthWeekly   - coupons/promotions: days left, budget used, 30-day price rule risk, unnamed ids
  SendOrganicRankWeekly    - Brand Analytics search share on our top queries, week vs week
  SendSearchTermHarvest    - every Monday: new exact keywords + negatives (+ bid raises in even weeks), checkboxes on /checks
  SendComebacksWeekly      - every Monday: paused product ads / bid cuts coming back (checkboxes)
Each takes dry=1 (return text, no Telegram).
"""
import json
import os
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

import requests
from sp_api.api import ListingsItems, Products, Reports
from sp_api.base import Marketplaces

from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state, set_job_state
from NotificationRouting import notify

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
NA = ("US", "CA", "MX")
CUR_SYMBOL = {"US": "$", "CA": "C$", "MX": "MX$", "UK": "£", "PL": "zł", "SE": "kr", "AE": "AED ", "SA": "SAR "}


def _json(body, status=200):
    return json.dumps(body, default=str), status, {"Access-Control-Allow-Origin": ALLOWED_ORIGIN, "Content-Type": "application/json"}


def creds(mp):
    if mp in NA:
        return (dict(refresh_token=os.environ["REFRESH_TOKEN_USA"], lwa_app_id=os.environ["CLIENT_ID_USA"],
                     lwa_client_secret=os.environ["CLIENT_SECRET_USA"]), os.environ["AMAZON_SELLER_ID"])
    return (dict(refresh_token=os.environ["REFRESH_TOKEN_EU"], lwa_app_id=os.environ["CLIENT_ID_EU"],
                 lwa_client_secret=os.environ["CLIENT_SECRET_EU"]), os.environ["AMAZON_SELLER_ID_EU"])


def sym(mp):
    return CUR_SYMBOL.get(mp, "€")


def run_report(mp, report_type, options=None, start=None, end=None, wait_minutes=40):
    """Request one SP-API report for marketplace `mp` and return its text
    (None if Amazon returns FATAL/CANCELLED)."""
    cred, _ = creds(mp)
    m = getattr(Marketplaces, mp)
    api = Reports(credentials=cred, marketplace=m)
    kw = {"reportType": report_type, "marketplaceIds": [m.marketplace_id]}
    if options:
        kw["reportOptions"] = options
    if start:
        kw["dataStartTime"] = start
    if end:
        kw["dataEndTime"] = end
    for attempt in range(6):
        try:
            rid = api.create_report(**kw).payload["reportId"]
            break
        except Exception as exc:
            if "QuotaExceeded" not in str(exc) or attempt == 5:
                raise
            time.sleep(60)
    deadline = time.time() + wait_minutes * 60
    while time.time() < deadline:
        st = api.get_report(rid).payload
        if st["processingStatus"] in ("DONE", "FATAL", "CANCELLED"):
            break
        time.sleep(15)
    else:
        raise TimeoutError(f"{report_type} {mp} not ready after {wait_minutes} min")
    if st["processingStatus"] != "DONE" or not st.get("reportDocumentId"):
        return None
    doc = api.get_report_document(st["reportDocumentId"], download=True).payload.get("document")
    return doc.decode("utf-8", "replace") if isinstance(doc, bytes) else doc


def listings(mp, included="summaries,offers"):
    """{sku: {"asin", "status": [...], "price"}} for all our listings in mp."""
    cred, seller = creds(mp)
    m = getattr(Marketplaces, mp)
    li = ListingsItems(credentials=cred, marketplace=m, version="2021-08-01", timeout=120)
    out, token = {}, None
    while True:
        kw = dict(marketplaceIds=m.marketplace_id, includedData=included, pageSize=20)
        if token:
            kw["pageToken"] = token
        for attempt in range(4):
            try:
                resp = li.search_listings_items(seller, **kw)
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(3 * (attempt + 1))
        for it in resp.payload.get("items", []):
            if it["sku"].startswith("amzn.gr."):
                continue
            s = (it.get("summaries") or [{}])[0]
            price = next((float(o["price"]["amount"]) for o in it.get("offers") or [] if o.get("offerType") == "B2C"), None)
            out[it["sku"]] = {"asin": s.get("asin"), "status": s.get("status") or [], "price": price,
                              "attributes": it.get("attributes"), "issues": it.get("issues")}
        token = resp.next_token
        if not token:
            return out
        time.sleep(0.25)


def _send(job, text, args):
    if args.get("dry") != "1":
        for i in range(0, len(text), 3900):
            notify(job, "amzbot", text[i:i + 3900])
    return _json({"text": text})


# --------------------------------------------------------------- account health
HEALTH_MARKETPLACES = ["US", "CA", "MX", "DE", "UK", "FR", "IT", "ES", "NL", "PL", "SE", "AE", "SA"]
POLICY_KEYS = ["listingPolicyViolations", "productAuthenticityCustomerComplaints", "productConditionCustomerComplaints",
               "productSafetyCustomerComplaints", "receivedIntellectualPropertyComplaints",
               "restrictedProductPolicyViolations", "suspectedIntellectualPropertyViolations",
               "foodAndProductSafetyIssues", "customerProductReviewsPolicyViolations", "otherPolicyViolations",
               "documentRequests"]
RATE_KEYS = ["lateShipmentRate", "invoiceDefectRate", "onTimeDeliveryRate", "validTrackingRate",
             "preFulfillmentCancellationRate"]


def account_health_text(pb, save=True):
    last = json.loads(get_job_state(pb, "account_health_last") or "{}")
    now, lines, alerts = {}, [], []
    for mp in HEALTH_MARKETPLACES:
        try:
            doc = run_report(mp, "GET_V2_SELLER_PERFORMANCE_REPORT", wait_minutes=15)
        except Exception as exc:
            lines.append(f"⚪ {mp}: report failed ({str(exc)[:80]})")
            continue
        if not doc:
            lines.append(f"⚪ {mp}: no report (not selling there?)")
            continue
        j = json.loads(doc)
        status = next((s["status"] for s in j.get("accountStatuses", [])), "?")
        pm = (j.get("performanceMetrics") or [{}])[0]
        ahr = pm.get("accountHealthRating") or {}
        odr = ((pm.get("orderDefectRate") or {}).get("afn") or {})
        bad = []
        for k in POLICY_KEYS:
            v = pm.get(k) or {}
            if v.get("defectsCount"):
                bad.append(f"{k.replace('PolicyViolations', ' violations').replace('CustomerComplaints', ' complaints')}: {v['defectsCount']}")
        for k in RATE_KEYS:
            v = pm.get(k) or {}
            if v.get("status") not in (None, "GOOD", "NONE"):
                bad.append(f"{k}: {v.get('status')} {v.get('rate')}")
        warn = (pm.get("policyViolationWarnings") or {}).get("warningsCount") or 0
        if warn:
            bad.append(f"policy warnings: {warn}")
        ws = pm.get("warningStates") or []
        if ws:
            bad.append(f"warning states: {ws}")
        now[mp] = {"status": status, "ahr": ahr.get("ahrScore"), "bad": bad}
        prev = last.get(mp) or {}
        changed = prev and (prev.get("status") != status or prev.get("bad") != bad)
        icon = "✅" if status == "NORMAL" and not bad else ("🔴" if status not in ("NORMAL",) else "🟠")
        line = (f"{icon} {mp}: {status} · rating {ahr.get('ahrStatus', '?')} {ahr.get('ahrScore', '')}"
                f" · ODR {100 * (odr.get('rate') or 0):.2f}% of {odr.get('orderCount', 0)} orders")
        if bad:
            line += "\n     " + "; ".join(bad)
        if changed:
            line += f"\n     ↪ changed since last week (was {prev.get('status')}, {len(prev.get('bad') or [])} issues)"
        lines.append(line)
        if icon != "✅":
            alerts.append(mp)
    if save and now:
        set_job_state(pb, "account_health_last", json.dumps(now))
    head = "🩺 Account health – " + (f"check {', '.join(alerts)}" if alerts else "all marketplaces OK")
    return head + "\n" + "\n".join(lines) + "\n(Details: Seller Central → Performance → Account Health)"


def SendAccountHealthWeekly(request):
    args = getattr(request, "args", {}) or {}
    try:
        pb = pb_authenticate()
        return _send("account-health-weekly", account_health_text(pb, save=args.get("dry") != "1"), args)
    except Exception as exc:
        notify("account-health-weekly", "amzbot", f"🚨 Account health check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


# --------------------------------------------------------------- listing health
STOCK_FIELD = {"US": "usa_balance_fba", "UK": "uk_balance_fba", "DE": "de_balance_fba"}


def offers_batch(mp, asins):
    """{asin: payload} via getItemOffersBatch (20 per call, 0.1 rps)."""
    cred, _ = creds(mp)
    m = getattr(Marketplaces, mp)
    p = Products(credentials=cred, marketplace=m)
    out = {}
    for i in range(0, len(asins), 20):
        reqs = [{"uri": f"/products/pricing/v0/items/{a}/offers", "method": "GET", "MarketplaceId": m.marketplace_id,
                 "ItemCondition": "New", "CustomerType": "Consumer"} for a in asins[i:i + 20]]
        for attempt in range(5):
            try:
                resp = p.get_item_offers_batch(reqs).payload
                break
            except Exception as exc:
                if attempt == 4:
                    raise
                time.sleep(12 if "QuotaExceeded" in str(exc) else 5)
        for r in resp.get("responses", []):
            body = (r.get("body") or {}).get("payload") or {}
            if body.get("ASIN"):
                out[body["ASIN"]] = body
        time.sleep(10.5)
    return out


def listing_health_text(pb):
    stats = {r["sku"]: r for r in pb_list_all(pb, "sku_statistics", 'sku != ""')}
    out = ["🛒 Listing health – stock vs buyable, Buy Box, other sellers"]
    problems = 0
    for mp, field in STOCK_FIELD.items():
        lst = listings(mp, "summaries,offers")
        _, my_id = creds(mp)
        not_buyable, with_stock = [], {}
        for sku, l in lst.items():
            stock = (stats.get(sku) or {}).get(field) or 0
            if stock <= 0 or not l["asin"]:
                continue
            with_stock[l["asin"]] = (sku, stock)
            if "BUYABLE" not in l["status"]:
                not_buyable.append(f"{sku} ({stock} in FBA, status {'/'.join(l['status']) or 'none'})")
        offers = offers_batch(mp, sorted(with_stock))
        lost, others = [], []
        for asin, (sku, stock) in sorted(with_stock.items(), key=lambda kv: -kv[1][1]):
            body = offers.get(asin)
            if not body:
                continue
            offs = body.get("Offers") or []
            mine = [o for o in offs if o.get("SellerId") == my_id]
            winner = next((o for o in offs if o.get("IsBuyBoxWinner")), None)
            if mine and not (winner and winner.get("SellerId") == my_id):
                lost.append(f"{sku}: Buy Box {'with another seller' if winner else 'not shown (suppressed?)'}"
                            f" – our price {sym(mp)}{(mine[0].get('ListingPrice') or {}).get('Amount')}")
            foreign = [o for o in offs if o.get("SellerId") != my_id]
            if foreign:
                p = min((o.get("ListingPrice") or {}).get("Amount") or 0 for o in foreign)
                others.append(f"{sku}: {len(foreign)} other new offer(s), lowest {sym(mp)}{p}")
        out.append(f"\n{mp}: {len(with_stock)} products with FBA stock")
        for title, rows, icon in (("in stock but NOT buyable", not_buyable, "🔴"), ("Buy Box not ours", lost, "🟠"),
                                  ("other sellers on our ASIN", others, "⚠️")):
            if rows:
                problems += len(rows)
                out.append(f"{icon} {len(rows)} {title}:")
                out += [f"   {r}" for r in rows[:12]]
                if len(rows) > 12:
                    out.append(f"   … +{len(rows) - 12} more")
        if not (not_buyable or lost or others):
            out.append("✅ all buyable, Buy Box ours, no other sellers")
    out[0] += f" ({problems} issues)" if problems else " – all OK"
    return "\n".join(out)


def SendListingHealthWeekly(request):
    args = getattr(request, "args", {}) or {}
    try:
        return _send("listing-health-weekly", listing_health_text(pb_authenticate()), args)
    except Exception as exc:
        notify("listing-health-weekly", "amzbot", f"🚨 Listing health check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


# --------------------------------------------------------------- coupons & promotions
# Registry of what we created (job_state "coupon_registry"); seeded from the
# 2026-10-21/25 specs in ~/videos/amazon/COUPONS_*.md. Update it whenever a
# coupon/promotion is created or ended.
_P15_US = "B0CR8J92QV B0DQGKH3X4 B0GKM8FML6 B0CR8L4K5Z B07QMQKS5J B0GLJN5785 B0B59W8L1P B0GBLHPF4G B0GBM2GLH8 B06Y22LRTP B07Q4FKR8C B08GC3B7C1 B07Q5JHJRJ B07B939QZK B07Q5JDW16 B0CR8KGMR5 B0BRXQGTKJ"
_P15_EU = "B0DQGKH3X4 B0GBLHPF4G B0GBM2GLH8 B0GBNFGSMF B0GBM1SWZK B07Q4FM2CL B0CR8KGMR5 B0BRXQGTKJ B0CR8JXTMK B07B939QZK B0GKM8FML6 B0GKN41W6G B0GLJN5785"
_HD15_EU = "B07N2ZTSXZ B0GLMG27TP B0GLN69R5M B08FMTFT1Y B08FH9ZH5Y B07SNR99TW B08FH6ZWV2 B08FH8C4LP B083NR1Y46 B083NPJJ1B B083NR5LPC B0GLMSXHCD B0GLNDMWNX B083NHS71H B083NMKC1L B083NRT1T6"
_HD15_US = "B08FMTFT1Y B07N2YYGXL B083NMKC1L B07SLQY5F8 B07N315FXJ B083NR8TPM B08FH8C4LP B083NHS71H B0DCPZ1QJ6 B08FH89S8G B0GLNP5HL5 B0GLNS1F87 B0DCQ74DWY B0GLMFWBCC"
_P15_UK = "B07Q4FKR8C B07QMQKS5J B0BRXQGTKJ B07Q5JHJRJ B08GC3B7C1 B07B939QZK B07Q5JDW16 B07CBMX9BS B0GKM8FML6 B0CR8KGMR5 B0GLJN5785 B0CR8JXTMK"
DEFAULT_REGISTRY = [
    {"label": "US coupon 15% Pareos & Sarongs", "mp": "US", "kind": "coupon", "pct": 15, "budget": 2500,
     "start": "2026-10-21", "end": "2026-12-31", "id": "PLM-a5d0c355-c7fd-4cc6-b9eb-af26dbe29520", "asins": _P15_US.split()},
    {"label": "US coupon 20% PAREO_ACA_6D", "mp": "US", "kind": "coupon", "pct": 20, "budget": 500,
     "start": "2026-10-21", "end": "2026-12-31", "id": "PLM-a1861969-a326-4408-9210-8a4daae47f13", "asins": ["B0CR8K86NM"],
     "note": "to be replaced by the 35% coupon (6D/6C/6B_NEW), 2026-10-25"},
    {"label": "US coupon 15% Boho Pouf Covers", "mp": "US", "kind": "coupon", "pct": 15, "budget": 1000,
     "start": "2026-11-20", "end": "2026-12-01", "asins": _HD15_US.split()},
    {"label": "US promo pareo B2-5 B3-10", "mp": "US", "kind": "promotion", "pct": 10, "budget": 10000,
     "start": "2026-10-22", "end": "2026-12-31", "id": "USA All Pareos 2026-10 B2-5 B3-10"},
    {"label": "US promo home decor B2-10", "mp": "US", "kind": "promotion", "pct": 10, "budget": 10000,
     "start": "2026-11-20", "end": "2026-12-01", "id": "USA All Home Decor 2026-11 B2-10"},
    {"label": "UK coupon 15% Pareos & Sarongs", "mp": "UK", "kind": "coupon", "pct": 15, "budget": 300,
     "start": "2026-10-22", "end": "2026-12-31", "id": "PLM-558c1ad8-6bed-424f-b539-e3834a00af2a", "asins": _P15_UK.split()},
    {"label": "UK promo pareo B2-10 B3-15", "mp": "UK", "kind": "promotion", "pct": 15, "budget": 3000,
     "start": "2026-10-22", "end": "2026-12-31", "id": "UK All Pareos 2026-10 B2-10 B3-15"},
] + [
    {"label": f"{cc} coupon 15% pareo", "mp": cc, "kind": "coupon", "pct": 15, "budget": b,
     "start": "2026-10-22", "end": "2026-12-31", "asins": _P15_EU.split(),
     **({"id": "PLM-4b71350b-99bf-404d-931b-f90271a1860b"} if cc == "IT" else {})}
    for cc, b in (("DE", 600), ("FR", 250), ("IT", 250), ("ES", 200))
] + [
    {"label": f"{cc} coupon 15% home decor", "mp": cc, "kind": "coupon", "pct": 15, "budget": b,
     "start": "2026-11-20", "end": "2026-12-01", "asins": [a for a in _HD15_EU.split() if not (a == "B07SNR99TW" and cc in ("DE", "FR"))]}
    for cc, b in (("DE", 400), ("FR", 150), ("IT", 150), ("ES", 150))
] + [
    {"label": f"{cc} promo pareo B3-10 B5-15", "mp": cc, "kind": "promotion", "pct": 15, "budget": 5000,
     "start": "2026-10-22", "end": "2026-12-31", "id": f"{cc} All Pareos 2026-10 B3-10 B5-15"}
    for cc in ("DE", "FR", "IT", "ES")
] + [
    {"label": f"{cc} promo home decor B2-10", "mp": cc, "kind": "promotion", "pct": 10, "budget": 5000,
     "start": "2026-11-20", "end": "2026-12-01", "id": f"{cc} All Home Decor 2026-11 B2-10"}
    for cc in ("DE", "FR", "IT", "ES")
]
PROMO_MP = {"US": "usa", "UK": "uk", "DE": "de", "FR": "fr", "IT": "it", "ES": "es"}


def coupon_health_text(pb, today=None):
    today = today or date.today()
    registry = json.loads(get_job_state(pb, "coupon_registry") or "null")
    if registry is None:
        registry = DEFAULT_REGISTRY
        set_job_state(pb, "coupon_registry", json.dumps(registry))
    names = {r["promotion_id"] for r in pb_list_all(pb, "promotion_names", 'promotion_id != ""', fields="promotion_id")}
    since = today - timedelta(days=70)
    promo_rows = pb_list_all(pb, "promo_sales_monthly", f'year >= {since.year} && ((year = {since.year} && month >= {since.month}) || year > {since.year})')
    use = defaultdict(lambda: {"orders": 0, "units": 0, "discount": 0.0})
    seen = defaultdict(set)
    for r in promo_rows:
        k = (r["marketplace"], r["promotion_id"])
        for f in ("orders", "units", "discount"):
            use[k][f] += r.get(f) or 0
        seen[r["promotion_id"]].add(r["marketplace"])
    # 30-day lowest price per (marketplace, asin) from price_history (daily since 2026-10-25).
    hist = defaultdict(list)
    for r in pb_list_all(pb, "price_history", 'since != ""', fields="marketplace,asin,sku,price,since"):
        hist[(r["marketplace"], r["asin"])].append(r)
    cut = (today - timedelta(days=30)).isoformat()

    def low30(mp, asin):
        rows = sorted(hist.get((mp, asin), []), key=lambda r: r["since"])
        if not rows:
            return None, None
        current = rows[-1]["price"]
        relevant = [r["price"] for r in rows if r["since"] >= cut]
        before = [r for r in rows if r["since"] < cut]
        if before:
            relevant.append(before[-1]["price"])
        return min(relevant), current

    out = ["🏷 Coupons & promotions"]
    alerts = 0
    for c in registry:
        start, end = date.fromisoformat(c["start"]), date.fromisoformat(c["end"])
        s = sym(c["mp"])
        if end < today:
            continue
        state = "upcoming" if start > today else "live"
        line = f"\n{'🟢' if state == 'live' else '🕒'} {c['label']} ({c['start'][5:]} → {c['end'][5:]}, budget {s}{c['budget']:,})"
        if c.get("note"):
            line += f"\n   note: {c['note']}"
        u = use.get((PROMO_MP.get(c["mp"], c["mp"].lower()), c.get("id")))
        if state == "live":
            if c.get("id") and u:
                pct = 100 * u["discount"] / c["budget"] if c["budget"] else 0
                line += f"\n   used: {u['orders']} orders, {u['units']} units, {s}{u['discount']:.2f} off ({pct:.0f}% of budget)"
                if pct >= 80:
                    line = line.replace("\n🟢", "\n🔴", 1)
                    alerts += 1
            elif c.get("id"):
                line += "\n   used: no orders yet"
            else:
                line += "\n   used: coupon id not seen in orders yet"
            run_end = min(end, start + timedelta(days=29))
            while run_end < today:
                run_end = min(end, run_end + timedelta(days=30))
            left = (run_end - today).days
            if c["kind"] == "coupon" and left <= 7 and run_end < end:
                line += f"\n🟠 current run ends in {left} days ({run_end}) – check the next run is scheduled"
                alerts += 1
            elif (end - today).days <= 7:
                line += f"\n🟠 ends in {(end - today).days} days"
                alerts += 1
        risky = []
        for asin in c.get("asins") or []:
            lo, cur = low30(c["mp"], asin)
            if lo is None:
                continue
            net = cur * (1 - c["pct"] / 100)
            if net >= lo - 0.01:
                need = 100 * (1 - (lo - 0.01) / cur) if cur else 0
                risky.append(f"{asin}: {s}{cur:.2f} −{c['pct']}% = {s}{net:.2f} ≥ 30-day low {s}{lo:.2f} (needs ~{need:.0f}%)")
        if risky:
            alerts += len(risky)
            line += f"\n⚠️ {len(risky)} ASIN(s) may be dropped by Amazon (30-day lowest price rule):\n   " + "\n   ".join(risky[:8])
        out.append(line)
    unnamed = [f"{pid} ({', '.join(sorted(mps))})" for pid, mps in seen.items() if pid not in names]
    if unnamed:
        alerts += len(unnamed)
        out.append("\n⚠️ promotion ids in orders without a name (add to promotion_names):\n   " + "\n   ".join(unnamed))
    out.append(f"\n(Prices tracked daily since 2026-10-25; the 30-day rule check gets complete from 2026-11-24.)")
    out[0] += f" – {alerts} to check" if alerts else " – all OK"
    return "\n".join(out)


def SendCouponHealthWeekly(request):
    args = getattr(request, "args", {}) or {}
    try:
        return _send("coupon-health-weekly", coupon_health_text(pb_authenticate()), args)
    except Exception as exc:
        notify("coupon-health-weekly", "amzbot", f"🚨 Coupon health check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


# --------------------------------------------------------------- organic search share
def organic_rank_text(pb, top_n=15):
    rows = pb_list_all(pb, "asin_search_queries", 'period = "WEEK"')
    by_mp = defaultdict(list)
    for r in rows:
        by_mp[r["marketplace"]].append(r)
    out = ["🔍 Organic search share (Brand Analytics, our ASINs combined) – last week vs week before"]
    for mp, rs in sorted(by_mp.items()):
        weeks = sorted({r["start_date"] for r in rs})
        if len(weeks) < 2:
            continue
        w1, w0 = weeks[-1], weeks[-2]
        agg = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        for r in rs:
            if r["start_date"] not in (w0, w1):
                continue
            a = agg[r["search_query"]][r["start_date"]]
            for f in ("asin_impressions", "asin_clicks", "asin_purchases"):
                a[f] += r.get(f) or 0
            for f in ("total_impressions", "total_clicks", "total_purchases"):
                a[f] = max(a[f], r.get(f) or 0)
        # Rank queries by what matters to us over the last 4 weeks: our
        # purchases, our clicks, then market size (total purchases).
        four = defaultdict(float)
        for r in rs:
            if r["start_date"] in weeks[-4:]:
                four[r["search_query"]] += (r.get("asin_purchases") or 0) * 50 + (r.get("asin_clicks") or 0) * 3
        for q, v in agg.items():
            four[q] += max(v[w1]["total_purchases"], v[w0]["total_purchases"])
        top = [q for q in sorted(agg, key=lambda q: -four[q])
               if agg[q][w1]["total_impressions"] >= 200
               and (agg[q][w1]["asin_impressions"] or agg[q][w0]["asin_impressions"])][:top_n]
        out.append(f"\n{mp.upper()} (week {w1} vs {w0}) – impr share / click share / purchases")

        def share(a, k):
            return 100 * a[f"asin_{k}"] / a[f"total_{k}"] if a[f"total_{k}"] else 0

        for q in top:
            a1, a0 = agg[q][w1], agg[q][w0]
            d = share(a1, "impressions") - share(a0, "impressions")
            icon = "🔺" if d >= 2 else ("🔻" if d <= -2 else "  ")
            out.append(f"{icon} {q[:34]}: {share(a1, 'impressions'):.1f}% ({d:+.1f}) · {share(a1, 'clicks'):.1f}%"
                       f" · {a1['asin_purchases']:.0f}/{a1['total_purchases']:.0f} buys")
    out.append("\n🔺/🔻 = impression share up/down by 2+ points")
    return "\n".join(out)


def SendOrganicRankWeekly(request):
    args = getattr(request, "args", {}) or {}
    try:
        return _send("organic-rank-weekly", organic_rank_text(pb_authenticate()), args)
    except Exception as exc:
        notify("organic-rank-weekly", "amzbot", f"🚨 Organic search share check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


def SendComebacksWeekly(request):
    from AdsGrowth import comebacks_weekly
    args = getattr(request, "args", {}) or {}
    try:
        text, _props = comebacks_weekly(store=args.get("dry") != "1")
        return _send("comebacks-weekly", text, args)
    except Exception as exc:
        notify("comebacks-weekly", "amzbot", f"🚨 Coming-back check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


# --------------------------------------------------------------- search-term harvest
PAREO_RE = ("PAREO", "SARONG", "BEACH")
MONEY_SCALE = {"SE": 11, "PL": 4.3}


def harvest_text(pb, today=None):
    """USA home decor plan rule (plans/usa-home-decor-ads-plan.md), all
    countries: search terms over the last 30 days (ending 2 days ago) with
    >=2 orders and ACOS <= target (home decor 35%, pareo 30%) that aren't an
    exact keyword yet -> add as exact; >=15 clicks and 0 orders with spend in
    the last 14 days -> negative exact in that campaign. Proposals only."""
    today = today or date.today()
    end = today - timedelta(days=2)
    start = end - timedelta(days=29)
    recent = (end - timedelta(days=13)).isoformat()
    rows = pb_list_all(pb, "ads_search_term_stats", f'date >= "{start}" && date <= "{end}"')
    best = {}
    for r in rows:
        k = (r["campaign_id"], r["ad_group_id"], r["target_id"], r["search_term"], r["date"])
        if k not in best or str(r["profile_id"]) < str(best[k]["profile_id"]):
            best[k] = r
    exact = defaultdict(set)
    kw_since = (today - timedelta(days=60)).isoformat()
    for r in pb_list_all(pb, "ads_keyword_stats", f'date >= "{kw_since}" && match_type = "EXACT"', fields="country_code,target_text"):
        exact[r["country_code"]].add((r.get("target_text") or "").lower().strip())
    agg = defaultdict(lambda: {"clicks": 0, "orders": 0, "spend": 0.0, "sales": 0.0, "recent_spend": 0.0})
    for r in best.values():
        term = (r.get("search_term") or "").lower().strip()
        if not term or term.startswith("b0") and len(term) == 10:
            continue
        a = agg[(r["country_code"], r["campaign_name"], term)]
        a["clicks"] += r.get("clicks") or 0
        a["orders"] += r.get("orders") or 0
        a["spend"] += r.get("spend") or 0
        a["sales"] += r.get("sales") or 0
        if r["date"] >= recent:
            a["recent_spend"] += r.get("spend") or 0
    add, neg = defaultdict(dict), defaultdict(list)
    for (cc, camp, term), a in agg.items():
        pareo = any(p in camp.upper() for p in PAREO_RE)
        target = 30 if pareo else 35
        acos = 100 * a["spend"] / a["sales"] if a["sales"] else None
        scale = MONEY_SCALE.get(cc, 1)
        if a["orders"] >= 2 and a["clicks"] >= 3 and acos is not None and acos <= target and term not in exact[cc]:
            prev = add[cc].get(term)
            if not prev or a["orders"] > prev["orders"]:
                add[cc][term] = {**a, "camp": camp, "acos": acos}
        if a["clicks"] >= 15 and a["spend"] >= 10 * scale and a["orders"] == 0 and a["recent_spend"] > 0:
            neg[cc].append((camp, term, a))
    out = [f"🌾 Search-term harvest {start} – {end} (proposals only – reply which to apply)",
           "Rules: ➕ ≥2 orders, ≥3 clicks, ACOS ≤ 35% home decor / 30% pareo, not an exact keyword yet;"
           " ➖ ≥15 clicks, ≥10 spent, 0 orders, still spending in the last 14 days."]
    for cc in sorted(set(add) | set(neg)):
        s = sym(cc)
        out.append(f"\n{cc}")
        if add[cc]:
            out.append(f"➕ new exact keywords ({len(add[cc])}):")
            for term, a in sorted(add[cc].items(), key=lambda kv: -kv[1]["orders"])[:15]:
                out.append(f"   {term} – {a['orders']} orders, ACOS {a['acos']:.0f}%, {s}{a['spend']:.2f} ({a['camp']})")
        neg[cc] = [x for x in neg[cc] if x[1] not in add[cc]]
        if neg[cc]:
            out.append(f"➖ negative exact ({len(neg[cc])}):")
            for camp, term, a in sorted(neg[cc], key=lambda x: -x[2]["spend"])[:15]:
                mark = " ⚠️ we bid on it as exact – negate only in auto/broad/phrase" if term in exact[cc] else ""
                out.append(f"   {term} – {a['clicks']} clicks, 0 orders, {s}{a['spend']:.2f} ({camp}){mark}")
    if len(out) == 1:
        out.append("Nothing to harvest.")
    return "\n".join(out)


def SendSearchTermHarvest(request):
    """Every Monday since 2026-11-02 (per the user): keyword harvest, plus bid
    raises for good keywords in even ISO weeks (or raises=1). Proposals with
    checkboxes on /checks (AdsGrowth), applied after the user ticks them."""
    from AdsGrowth import Api, _weekly, harvest_block, raise_block
    args = getattr(request, "args", {}) or {}
    try:
        today = date.fromisoformat(args["today"]) if args.get("today") else date.today()
        pb = pb_authenticate()
        api = Api(pb)
        lines, table, props = harvest_block(pb, today, api)
        tables = [table]
        if args.get("raises") == "1" or not today.isocalendar()[1] % 2:
            r_lines, r_table, r_props = raise_block(pb, today, api)
            lines += [""] + r_lines
            tables.append(r_table)
            props += r_props
        else:
            lines += ["", "📈 Bid raises: every 2nd Monday (even weeks) and in the monthly review."]
        text = "\n".join(lines) + "\n\nTick the proposals on the /checks page - applied within 10 minutes."
        if args.get("dry") != "1":
            _weekly("SendSearchTermHarvest", "🌾 Keyword harvest + bid raises", text, tables, props, today)
        return _send("search-term-harvest", text, args)
    except Exception as exc:
        notify("search-term-harvest", "amzbot", f"🚨 Search-term harvest failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)
