"""Profit simulation per landed cost (per the user 2026-11-03: "prepare profit
simulation page for each landing price"). Page /price-simulation.

GetPriceSimulation?mp=US|UK|DE|FR|IT|ES&segment=home|pareo&days=90 returns per
product, in the marketplace's currency:
  landed     = ProductCost landed cost for the marketplace's region
  price      = current price (price_history)
  vat        = VAT rate (price includes VAT in UK/EU)
  referralPct, fba = today's Amazon fee estimate (ProductCost.fee_table)
  units, adsPerUnit (SP+SD + SB share), otherPerUnit (returns + storage + AWD)
  over the last `days` days (ProductCost.product_ranking, order date)
The page computes, for any price P:
  profit/unit = P/(1+vat) - referralPct x P - fba - landed - ads/unit - other/unit
(ads and other cost per unit kept at today's level). Read only, local server."""
import json
import time
from datetime import date, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all
from ProductCost import (MARKETS, ATOMIC, fee_table, fx_to, landed_cost, landed_for, product_ranking,
                         _section_of_group)

MP_ATOMIC = {"US": "usa", "UK": "uk", "DE": "de", "FR": "fr", "IT": "it", "ES": "es"}
# Stock region per marketplace (sku_statistics.<region>_balance; FR/IT/ES are served from the Pan-EU pool)
MP_STOCK = {"US": "usa", "UK": "uk", "DE": "de", "FR": "de", "IT": "de", "ES": "de"}
_cache = {}


def _json(body, status=200):
    return (json.dumps(body), status, {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"})


def simulation(mp="US", segment="home", days=90):
    mp = mp.upper()
    if mp not in MP_ATOMIC:
        raise ValueError(f"mp must be one of {', '.join(MP_ATOMIC)}")
    key = (mp, segment, days)
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < 6 * 3600:
        return hit[1]
    token = pb_authenticate()
    _m, _t, _a, vat, cur, _c = next(x for x in MARKETS if x[0] == mp)
    rate = fx_to(cur)  # local currency per USD
    mapping = [r for r in pb_list_all(token, "asin_group_mapping", 'asin != ""',
                                      fields="asin,sku,ean,group,end_of_life")
               if (r.get("group") or "").upper() != "IGNORE"]
    want = "pareo" if segment == "pareo" else "home"
    mapping = [r for r in mapping if _section_of_group(r.get("group")) == want]
    prices, n_rows = {}, {}
    for r in pb_list_all(token, "price_history", f'marketplace = "{mp}"', fields="asin,price,since"):
        n_rows[r["asin"]] = n_rows.get(r["asin"], 0) + 1
        if r["asin"] not in prices or r["since"] >= prices[r["asin"]]["since"]:
            prices[r["asin"]] = r
    stock_field = f"{MP_STOCK[mp]}_balance"
    stock = {}
    for r in pb_list_all(token, "sku_statistics", "", fields=f"sku,{stock_field}"):
        stock[r["sku"]] = r.get(stock_field) or 0
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    ranking = product_ranking(start.isoformat(), end.isoformat(), atomic=[MP_ATOMIC[mp]], _token=token)["byAsin"]
    asins = sorted({r["asin"] for r in mapping if r["asin"] in prices})
    fees = fee_table(token, asins)
    rows, seen = [], set()
    for r in mapping:
        asin = r["asin"]
        if asin in seen or asin not in prices:
            continue
        seen.add(asin)
        lc = landed_cost(token, r.get("ean")) if r.get("ean") else {}
        landed_usd = landed_for(lc, MP_ATOMIC[mp]) if lc.get("purchase") else None
        f = fees.get((mp, asin)) or {}
        rk = ranking.get(asin) or {}
        units = rk.get("units") or 0
        per = (lambda v: round((v or 0) / units * rate, 2)) if units else (lambda v: 0.0)
        rows.append({
            "sku": r["sku"], "asin": asin, "group": r.get("group"), "endOfLife": bool(r.get("end_of_life")),
            "purchase": lc.get("purchase"), "invoice": lc.get("invoice"),
            "landed": round(landed_usd * rate, 2) if landed_usd else None,
            "price": prices[asin]["price"], # a single price_history row is the 2026-10-25 seed, not a change
            "priceSince": (prices[asin].get("since") or "")[:10] if n_rows.get(asin, 0) > 1 else None,
            "stock": stock.get(r["sku"]), "referralPct": round(f.get("referralPct", 0.15), 4),
            "fba": round(f.get("fbaUsd", 0) * rate, 2), "feesEstimated": bool(f),
            "units": units, "adsPerUnit": per((rk.get("ads") or 0) + (rk.get("sbAds") or 0)),
            "otherPerUnit": per((rk.get("returns") or 0) + (rk.get("storage") or 0) + (rk.get("awd") or 0)),
        })
    rows.sort(key=lambda x: (-(x["landed"] or 0), x["sku"]))
    out = {"mp": mp, "currency": cur, "vat": vat, "segment": want, "start": start.isoformat(),
           "end": end.isoformat(), "rows": rows}
    _cache[key] = (time.time(), out)
    return out


def GetPriceSimulation(request):
    if request.method == "OPTIONS":
        return ("", 204, {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET",
                          "Access-Control-Allow-Headers": "Content-Type"})
    try:
        a = request.args
        return _json(simulation(a.get("mp", "US"), a.get("segment", "home"), int(a.get("days", 90))))
    except Exception as exc:
        return _json({"error": str(exc)}, 500)
