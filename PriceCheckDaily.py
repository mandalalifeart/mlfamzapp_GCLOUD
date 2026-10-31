"""Daily cross-marketplace price check (2026-10-25, per the user: "a daily
check in all non eur usd marketplaces that prices are correct, same prices
across all eur marketplaces, pln, sek, aed, sar, mex, ca prices are not too
small").

Prices in the target marketplaces are set by Amazon's Build International
Listings (BIL) from the source marketplace (DE for Europe/Middle East, US for
Canada/Mexico). This job only REPORTS - it never changes a price, since a
direct price edit in a target marketplace breaks the BIL sync.

Rules (per the user 2026-10-25 "always check the price makes sense vs de
price, in all mp, except uk and au"):
  * FR/IT/ES/NL/BE: price must equal the DE price.
  * IE/PL/SE/AE/SA (FORMULA_TARGETS): BIL converts every DE price with one
    formula per marketplace (FX + its fee adjustment), e.g. AE ~ DE x 3.0 +
    27.5 AED. The check fits price = a + b x DE (Theil-Sen, per marketplace and
    pareo/home) on all checked listings and flags a SKU more than
    FORMULA_TOLERANCE away from it; the formula itself is flagged when its
    price at a reference DE price moved more than FORMULA_JUMP since the last
    run (job_state "price_check_formula") - BIL broken for a whole marketplace.
  * CA/MX (from US stock, prices don't follow a formula): price >= US price x
    FX + the per-unit remote fulfilment fee (real orders: MX 173 / 264 MXN,
    CA 10.62 / 16.38 CAD); flagged when more than TOLERANCE below.
Only active listings with stock are checked (per the user 2026-10-25 "check
only active listings with positive inventory in eu"): BUYABLE in the target
marketplace, and FBA fulfillable + FC transfer > 0 for its ASIN in the source
region's pool (Pan-EU via DE for Europe/Middle East, US for CA/MX).
Business price (2026-10-31, per the user): in US/CA/MX/UK/DE/FR/IT/ES every
buyable pouf/pareo listing must have a business (B2B) price equal to its
normal price - quantity discounts for business buyers sit on top of it
(set by business_prices_all_mp_2026_10_31.py). Missing/different is flagged.
Fees can be overridden via job_state key "price_check_fees"
({"AE": {"pareo": 31, "home": 30}, ...}).
"""
import json
import os
import time
from datetime import date

import requests
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state, set_job_state
from NotificationRouting import notify

JOB_NAME = "price-check-daily"
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
TOLERANCE = 0.03

EUR_TARGETS = ["FR", "IT", "ES", "NL", "BE"]
FORMULA_TARGETS = {"IE": "EUR", "PL": "PLN", "SE": "SEK", "AE": "AED", "SA": "SAR"}
FORMULA_TOLERANCE = 0.10
FORMULA_JUMP = 0.05
FORMULA_REF = {"pareo": 25.0, "home": 60.0}  # DE price used to compare formulas day to day
# target -> (source, currency, remote fulfilment fee per category in target currency)
MIN_TARGETS = {
    "CA": ("US", "CAD", {"pareo": 10.62, "home": 16.38}),
    "MX": ("US", "MXN", {"pareo": 173.34, "home": 264.19}),
}
USD_PEGS = {"AED": 3.6725, "SAR": 3.75}
BUSINESS_GROUPS = {"VELVET", "COVER_BOHO", "COVER_VINTAGE", "STUFFED"}  # + every PAREO_* group
BUSINESS_MPS = ["US", "CA", "MX", "UK", "DE", "FR", "IT", "ES"]  # NL/BE/PL/SE/IE/JP reject B2B offers
FALLBACK_RATES = {("EUR", "PLN"): 4.25, ("EUR", "SEK"): 11.0, ("EUR", "USD"): 1.12,
                  ("USD", "CAD"): 1.38, ("USD", "MXN"): 18.5}


def _json(body, status=200):
    return json.dumps(body), status, {"Access-Control-Allow-Origin": ALLOWED_ORIGIN, "Content-Type": "application/json"}


def _creds(region):
    if region == "EU":
        return dict(refresh_token=os.environ["REFRESH_TOKEN_EU"], lwa_app_id=os.environ["CLIENT_ID_EU"],
                    lwa_client_secret=os.environ["CLIENT_SECRET_EU"]), os.environ["AMAZON_SELLER_ID_EU"]
    return dict(refresh_token=os.environ["REFRESH_TOKEN_USA"], lwa_app_id=os.environ["CLIENT_ID_USA"],
                lwa_client_secret=os.environ["CLIENT_SECRET_USA"]), os.environ["AMAZON_SELLER_ID"]


def category(sku):
    s = sku.upper()
    return "pareo" if ("PAREO" in s or "SARONG" in s) else "home"


def fetch_prices(mp):
    """{sku: price} for every B2C offer we have in marketplace `mp`."""
    m = getattr(Marketplaces, mp)
    cred, seller = _creds("US" if mp in ("US", "CA", "MX") else "EU")
    li = ListingsItems(credentials=cred, marketplace=m, version="2021-08-01")
    prices, token = {}, None
    while True:
        kw = dict(marketplaceIds=m.marketplace_id, includedData="summaries,offers", pageSize=20)
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
            sku = it["sku"]
            if sku.startswith("amzn.gr."):
                continue
            status = ((it.get("summaries") or [{}])[0].get("status")) or []
            price = next((o["price"]["amount"] for o in it.get("offers") or [] if o.get("offerType") == "B2C"), None)
            b2b = next((o["price"]["amount"] for o in it.get("offers") or [] if o.get("offerType") == "B2B"), None)
            if price is not None:
                prices[sku] = {"price": float(price), "buyable": "BUYABLE" in status,
                               "b2b": float(b2b) if b2b is not None else None,
                               "asin": (it.get("summaries") or [{}])[0].get("asin")}
        token = resp.next_token
        if not token:
            return prices
        time.sleep(0.25)


def fx_rate(base, cur):
    """Units of `cur` per 1 `base`; AED/SAR via their USD peg."""
    if cur in USD_PEGS:
        return (1.0 if base == "USD" else fx_rate(base, "USD")) * USD_PEGS[cur]
    try:
        r = requests.get(f"https://api.frankfurter.dev/v1/latest?from={base}&to={cur}", timeout=15)
        r.raise_for_status()
        return float(r.json()["rates"][cur])
    except Exception:
        return FALLBACK_RATES[(base, cur)]


MP_CURRENCY = {"DE": "EUR", "FR": "EUR", "IT": "EUR", "ES": "EUR", "NL": "EUR", "BE": "EUR", "IE": "EUR",
               "PL": "PLN", "SE": "SEK", "AE": "AED", "SA": "SAR", "UK": "GBP", "US": "USD", "CA": "CAD", "MX": "MXN"}


def record_price_history(pb_token, all_prices, day):
    """price_history keeps one row per price change (marketplace, sku, price,
    since) - used by the weekly coupon check for Amazon's 30-day lowest-price
    rule. Only writes when a price differs from the latest stored one."""
    from AsinAnalytics import pb_list_all
    latest = {}
    for r in pb_list_all(pb_token, "price_history", 'since != ""', fields="marketplace,sku,price,since"):
        k = (r["marketplace"], r["sku"])
        if k not in latest or r["since"] > latest[k]["since"]:
            latest[k] = r
    base = os.environ.get("POCKETBASE_URL", "http://127.0.0.1:8090")
    new = 0
    for mp, prices in all_prices.items():
        for sku, p in prices.items():
            old = latest.get((mp, sku))
            if old and abs((old.get("price") or 0) - p["price"]) < 0.005:
                continue
            requests.post(f"{base}/api/collections/price_history/records", headers={"Authorization": pb_token},
                          json={"marketplace": mp, "sku": sku, "asin": p.get("asin") or "", "price": p["price"],
                                "currency": MP_CURRENCY.get(mp, ""), "since": day}, timeout=15).raise_for_status()
            new += 1
    return new


def fit_line(points):
    """Theil-Sen fit y = a + b*x over [(x, y)]: robust to a few wrong prices."""
    import itertools
    import statistics
    slopes = [(y2 - y1) / (x2 - x1) for (x1, y1), (x2, y2) in itertools.combinations(points, 2) if abs(x2 - x1) > 0.5]
    b = statistics.median(slopes) if slopes else 1.0
    a = statistics.median(y - b * x for x, y in points)
    return a, b


def run_check(fees_override=None, pb_token=None, prev_formula=None):
    fees_override = fees_override or {}
    cache = {}

    def prices(mp):
        if mp not in cache:
            cache[mp] = fetch_prices(mp)
        return cache[mp]

    from InventoryWarning import fetch_fba_by_asin
    stock = {"DE": fetch_fba_by_asin("de"), "US": fetch_fba_by_asin("usa")}

    def active(t, src_mp):
        s = stock[src_mp].get(t.get("asin")) or {}
        return t.get("buyable") and (s.get("fulfillable", 0) + s.get("transfer", 0)) > 0

    prev_formula = prev_formula or {}
    result = {"eur": {}, "formula": {}, "min": {}, "rates": {}}
    de = prices("DE")
    for mp in EUR_TARGETS:
        tgt = {k: v for k, v in prices(mp).items() if active(v, "DE")}
        bad = []
        for sku, t in tgt.items():
            s = de.get(sku)
            if s and abs(t["price"] - s["price"]) > 0.01:
                bad.append({"sku": sku, "price": t["price"], "source": s["price"]})
        result["eur"][mp] = {"checked": sum(1 for k in tgt if k in de), "bad": sorted(bad, key=lambda b: b["price"] - b["source"])}
    for mp, cur in FORMULA_TARGETS.items():
        tgt = {k: v for k, v in prices(mp).items() if active(v, "DE")}
        r = {"currency": cur, "checked": 0, "cats": {}, "bad": []}
        for cat in ("pareo", "home"):
            rows = [(sku, de[sku]["price"], t["price"]) for sku, t in tgt.items() if sku in de and category(sku) == cat]
            r["checked"] += len(rows)
            if len(rows) < 5:
                continue
            a, b = fit_line([(x, y) for _, x, y in rows])
            ref = a + b * FORMULA_REF[cat]
            old = (prev_formula.get(mp) or {}).get(cat)
            jump = None
            if old:
                old_ref = old["a"] + old["b"] * FORMULA_REF[cat]
                jump = round(100 * (ref / old_ref - 1), 1) if old_ref else None
            r["cats"][cat] = {"a": round(a, 3), "b": round(b, 4), "n": len(rows), "jump_pct": jump}
            for sku, x, y in rows:
                exp = a + b * x
                if exp > 0 and abs(y / exp - 1) > FORMULA_TOLERANCE:
                    r["bad"].append({"sku": sku, "price": y, "source": x, "expected": round(exp, 2),
                                     "diff_pct": round(100 * (y / exp - 1), 1)})
        r["bad"].sort(key=lambda x: -abs(x["diff_pct"]))
        result["formula"][mp] = r
    for mp, (src_mp, cur, fees) in MIN_TARGETS.items():
        fees = {**fees, **(fees_override.get(mp) or {})}
        src = prices(src_mp)
        tgt = {k: v for k, v in prices(mp).items() if active(v, src_mp)}
        base = "EUR" if src_mp == "DE" else "USD"
        rate = fx_rate(base, cur)
        result["rates"][mp] = rate
        low = []
        checked = 0
        for sku, t in tgt.items():
            s = src.get(sku)
            if not s:
                continue
            checked += 1
            fee = fees[category(sku)]
            minimum = s["price"] * rate + fee
            if t["price"] < minimum * (1 - TOLERANCE):
                low.append({"sku": sku, "price": t["price"], "min": round(minimum, 2), "source": s["price"],
                            "fee": fee, "short_pct": round(100 * (1 - t["price"] / minimum), 1)})
        result["min"][mp] = {"checked": checked, "currency": cur, "source": src_mp, "low": sorted(low, key=lambda x: -x["short_pct"])}
    # Business price = normal price (per the user 2026-10-31; quantity discounts
    # sit on top of it). Only marketplaces where Amazon Business runs for us.
    result["business"] = {}
    groups = {}
    if pb_token:
        for r in pb_list_all(pb_token, "asin_group_mapping", ""):
            groups.setdefault(r.get("asin"), r.get("group") or "")
    ours = {a for a, g in groups.items() if g in BUSINESS_GROUPS or g.startswith("PAREO")}
    for mp in BUSINESS_MPS if ours else []:
        rows = {k: v for k, v in prices(mp).items() if v.get("buyable") and v.get("asin") in ours}
        missing = sorted(k for k, v in rows.items() if v.get("b2b") is None)
        differ = sorted(({"sku": k, "price": v["price"], "b2b": v["b2b"]} for k, v in rows.items()
                         if v.get("b2b") is not None and abs(v["b2b"] - v["price"]) > 0.005), key=lambda x: x["sku"])
        result["business"][mp] = {"checked": len(rows), "missing": missing, "differ": differ}
    if pb_token:
        prices("UK")
        result["historyRows"] = record_price_history(pb_token, cache, date.today().isoformat())
    return result


def build_text(result, day, max_lines=8):
    lines = [f"💲 Daily price check {day}", "(report only - prices come from BIL; fix them in BIL, not by hand)",
             "(active listings with FBA stock only)"]
    lines.append("\nEUR marketplaces = DE price:")
    for mp, r in result["eur"].items():
        if not r["bad"]:
            lines.append(f"✅ {mp}: {r['checked']} SKUs, all equal to DE")
            continue
        lines.append(f"⚠️ {mp}: {len(r['bad'])} of {r['checked']} differ from DE")
        for b in r["bad"][:max_lines]:
            lines.append(f"   {b['sku']}: €{b['price']:.2f} (DE €{b['source']:.2f})")
        if len(r["bad"]) > max_lines:
            lines.append(f"   … +{len(r['bad']) - max_lines} more")
    lines.append(f"\nIE/PL/SE/AE/SA = BIL formula from DE (flag > {FORMULA_TOLERANCE:.0%} off):")
    for mp, r in result.get("formula", {}).items():
        cur = r["currency"]
        forms = []
        for cat, f in r["cats"].items():
            sign = "+" if f["a"] >= 0 else "−"
            forms.append(f"{cat} DE×{f['b']:.3g}" + (f" {sign} {abs(f['a']):.1f}" if abs(f["a"]) >= 0.5 else ""))
            if f["jump_pct"] is not None and abs(f["jump_pct"]) > FORMULA_JUMP * 100:
                lines.append(f"🔴 {mp} {cat}: formula moved {f['jump_pct']:+.0f}% since last check - BIL change for the whole marketplace?")
        head = f"{mp}: {r['checked']} SKUs ({'; '.join(forms)} {cur})"
        if not r["bad"]:
            lines.append(f"✅ {head}")
            continue
        lines.append(f"⚠️ {head}, {len(r['bad'])} off")
        for x in r["bad"][:max_lines]:
            lines.append(f"   {x['sku']}: {x['price']:.2f} vs ~{x['expected']:.2f} {cur} (DE €{x['source']:.2f}, {x['diff_pct']:+.0f}%)")
        if len(r["bad"]) > max_lines:
            lines.append(f"   … +{len(r['bad']) - max_lines} more")
    lines.append("\nCA/MX, minimum = US price × FX + remote fulfilment fee:")
    for mp, r in result["min"].items():
        cur = r["currency"]
        src_cur = "€" if r["source"] == "DE" else "$"
        rate = result["rates"][mp]
        if not r["low"]:
            lines.append(f"✅ {mp}: {r['checked']} SKUs OK (1{src_cur} = {rate:.2f} {cur})")
            continue
        lines.append(f"🔴 {mp}: {len(r['low'])} of {r['checked']} too low (1{src_cur} = {rate:.2f} {cur})")
        for x in r["low"][:max_lines]:
            fee = f" + fee {x['fee']:.0f}" if x["fee"] else ""
            lines.append(f"   {x['sku']}: {x['price']:.2f} < min {x['min']:.2f} {cur} "
                         f"({r['source']} {src_cur}{x['source']:.2f}{fee}, −{x['short_pct']:.0f}%)")
        if len(r["low"]) > max_lines:
            lines.append(f"   … +{len(r['low']) - max_lines} more")
    lines.append("\nBusiness price = normal price (buyable listings):")
    for mp, r in (result.get("business") or {}).items():
        if not r["missing"] and not r["differ"]:
            lines.append(f"✅ {mp}: {r['checked']} SKUs OK")
            continue
        lines.append(f"⚠️ {mp}: {len(r['differ'])} differ, {len(r['missing'])} without business price (of {r['checked']})")
        for x in r["differ"][:max_lines]:
            lines.append(f"   {x['sku']}: business {x['b2b']:.2f} vs normal {x['price']:.2f}")
        if r["missing"]:
            lines.append("   no business price: " + ", ".join(r["missing"][:max_lines]) + (" …" if len(r["missing"]) > max_lines else ""))
    return "\n".join(lines)


def SendPriceCheckDaily(request):
    try:
        args = getattr(request, "args", {}) or {}
        pb = pb_authenticate()
        override = json.loads(get_job_state(pb, "price_check_fees") or "{}")
        prev = json.loads(get_job_state(pb, "price_check_formula") or "{}")
        result = run_check(override, pb_token=pb, prev_formula=prev)
        if args.get("dry") != "1":
            set_job_state(pb, "price_check_formula", json.dumps(
                {mp: r["cats"] for mp, r in result["formula"].items()}))
        text = build_text(result, date.today().isoformat())
        os.makedirs("logs", exist_ok=True)
        with open("logs/price_check_last.json", "w") as fh:
            json.dump(result, fh, indent=1)
        if args.get("dry") != "1":
            notify(JOB_NAME, "amzbot", text[:4000])
        return _json({"text": text, "result": result})
    except Exception as exc:
        notify(JOB_NAME, "amzbot", f"🚨 Daily price check failed: {exc}", is_error=True)
        return _json({"error": str(exc), "type": exc.__class__.__name__}, 500)
