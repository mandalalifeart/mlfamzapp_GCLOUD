"""Daily cross-marketplace price check (2026-10-25, per the user: "a daily
check in all non eur usd marketplaces that prices are correct, same prices
across all eur marketplaces, pln, sek, aed, sar, mex, ca prices are not too
small").

Prices in the target marketplaces are set by Amazon's Build International
Listings (BIL) from the source marketplace (DE for Europe/Middle East, US for
Canada/Mexico). This job only REPORTS - it never changes a price, since a
direct price edit in a target marketplace breaks the BIL sync.

Rules (per the user "price should be amazon price + fee adjustment" and "we
do not sell pareo there for less than 25 euro + the fba fee"):
  * EUR marketplaces (FR/IT/ES/NL/BE/IE): price must equal the DE price.
  * PL/SE (Pan-EU stock, local FBA fees like DE): price >= DE price x FX.
  * AE/SA: not checked - BIL's fee-adjusted price from DE is correct (per the
    user 2026-10-25).
  * CA/MX (remote fulfilment from US stock): price >= source price
    x FX + that marketplace's per-unit remote fulfilment fee (from real
    orders: AE 31 AED pareo / 30 cover, MX 173 / 260 MXN, CA 10.62 / 16.4
    CAD; SA has no orders yet, assumed like AE).
  A price more than TOLERANCE below its minimum is flagged.
Only active listings with stock are checked (per the user 2026-10-25 "check
only active listings with positive inventory in eu"): BUYABLE in the target
marketplace, and FBA fulfillable + FC transfer > 0 for its ASIN in the source
region's pool (Pan-EU via DE for Europe/Middle East, US for CA/MX).
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

from AsinAnalytics import pb_authenticate
from JobState import get_job_state
from NotificationRouting import notify

JOB_NAME = "price-check-daily"
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
TOLERANCE = 0.03

EUR_TARGETS = ["FR", "IT", "ES", "NL", "BE", "IE"]
# target -> (source, currency, remote fulfilment fee per category in target currency)
MIN_TARGETS = {
    "PL": ("DE", "PLN", {"pareo": 0, "home": 0}),
    "SE": ("DE", "SEK", {"pareo": 0, "home": 0}),
    "CA": ("US", "CAD", {"pareo": 10.62, "home": 16.38}),
    "MX": ("US", "MXN", {"pareo": 173.34, "home": 264.19}),
}
# AE/SA: BIL sets them from DE with its own fee adjustment (about DE x 2.9 +
# 31 AED) - per the user 2026-10-25 that is correct, so they are not checked.
BIL_FEE_ADJUSTED = ["AE", "SA"]
USD_PEGS = {"AED": 3.6725, "SAR": 3.75}
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
            if price is not None:
                prices[sku] = {"price": float(price), "buyable": "BUYABLE" in status,
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


def run_check(fees_override=None, pb_token=None):
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

    result = {"eur": {}, "min": {}, "rates": {}}
    de = prices("DE")
    for mp in EUR_TARGETS:
        tgt = {k: v for k, v in prices(mp).items() if active(v, "DE")}
        bad = []
        for sku, t in tgt.items():
            s = de.get(sku)
            if s and abs(t["price"] - s["price"]) > 0.01:
                bad.append({"sku": sku, "price": t["price"], "source": s["price"]})
        result["eur"][mp] = {"checked": sum(1 for k in tgt if k in de), "bad": sorted(bad, key=lambda b: b["price"] - b["source"])}
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
    lines.append("\nOther currencies, minimum = source price × FX + remote fulfilment fee:")
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
    lines.append(f"ℹ️ {'/'.join(BIL_FEE_ADJUSTED)}: not checked - BIL sets them from DE with its fee adjustment")
    return "\n".join(lines)


def SendPriceCheckDaily(request):
    try:
        args = getattr(request, "args", {}) or {}
        pb = pb_authenticate()
        override = json.loads(get_job_state(pb, "price_check_fees") or "{}")
        result = run_check(override, pb_token=pb)
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
