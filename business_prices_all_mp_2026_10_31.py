"""Business price + quantity discounts in every marketplace (per the user
2026-10-31 "lets do it on all mp"): business price (B2B offer) = the normal
(B2C) price, quantity discounts for business buyers - home decor 3+ -10%,
5+ -12%, 10+ -15%; pareo 3+ -10%, 5+ -15%, 10+ -20%. Only the B2B entry of
purchasable_offer is sent (a replace only touches the entry matching its
marketplace_id/currency/audience selectors), so the normal price - set by BIL
outside the US - is never written. Products = ASINs of the pouf/home and
pareo groups in asin_group_mapping (any seller SKU on them).
Default VALIDATION_PREVIEW; --apply submits.
Usage: python business_prices_all_mp_2026_10_31.py MP[,MP] [--apply]"""
import json
import os
import sys
import time

import requests
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from us_business_prices_2026_10_31 import HOME_GROUPS, TIERS, mapping

JP_CRED = ("REFRESH_TOKEN_JP", "CLIENT_ID_USA", "CLIENT_SECRET_USA", "AMAZON_SELLER_ID_JP")


def creds(mp):
    names = (JP_CRED if mp == "JP" else
             ("REFRESH_TOKEN_USA", "CLIENT_ID_USA", "CLIENT_SECRET_USA", "AMAZON_SELLER_ID") if mp in ("US", "CA", "MX") else
             ("REFRESH_TOKEN_EU", "CLIENT_ID_EU", "CLIENT_SECRET_EU", "AMAZON_SELLER_ID_EU"))
    r, i, s, seller = (os.environ[n] for n in names)
    return dict(refresh_token=r, lwa_app_id=i, lwa_client_secret=s), seller


def kinds_by_asin():
    out = {}
    for row in mapping():
        g = row["group"]
        k = "home" if g in HOME_GROUPS else "pareo" if g.startswith("PAREO") else None
        if k and row["asin"]:
            out[row["asin"]] = k
    return out


def listings(li, seller, mid):
    token = None
    while True:
        kw = dict(marketplaceIds=mid, includedData="summaries,offers", pageSize=20)
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
        yield from resp.payload.get("items", [])
        token = resp.next_token
        if not token:
            return
        time.sleep(0.25)


def run(mp, apply, kinds):
    m = getattr(Marketplaces, mp)
    cred, seller = creds(mp)
    li = ListingsItems(credentials=cred, marketplace=m, version="2021-08-01", timeout=120)
    results = {}
    for it in listings(li, seller, m.marketplace_id):
        sku = it["sku"]
        summ = (it.get("summaries") or [{}])[0]
        kind = kinds.get(summ.get("asin"))
        if not kind or sku.startswith("amzn.gr."):
            continue
        offers = it.get("offers") or []
        b2c = next((o for o in offers if o.get("offerType") == "B2C"), None)
        if not b2c:
            continue
        price = float(b2c["price"]["amount"])
        cur = b2c["price"].get("currencyCode") or b2c["price"].get("currency")
        old = next((float(o["price"]["amount"]) for o in offers if o.get("offerType") == "B2B"), None)
        b2b = {"currency": cur, "audience": "B2B", "marketplace_id": m.marketplace_id,
               "our_price": [{"schedule": [{"value_with_tax": price}]}],
               "quantity_discount_plan": [{"schedule": [{"discount_type": "percent", "levels": [
                   {"lower_bound": q, "value": pct} for q, pct in TIERS[kind]]}]}]}
        body = {"productType": summ.get("productType"),
                "patches": [{"op": "replace", "path": "/attributes/purchasable_offer", "value": [b2b]}]}
        kw = {"marketplaceIds": [m.marketplace_id], "body": body}
        if not apply:
            kw["mode"] = "VALIDATION_PREVIEW"
        try:
            r = ListingsItems(credentials=cred, marketplace=m, timeout=120).patch_listings_item(seller, sku, **kw).payload
            status, issues = r.get("status"), [(i.get("code"), i.get("severity"), i.get("message", "")[:200]) for i in r.get("issues", [])]
        except Exception as exc:
            status, issues = "EXCEPTION", [str(exc)[:300]]
        results[sku] = {"kind": kind, "price": price, "currency": cur, "oldB2B": old, "status": status, "issues": issues}
        if status not in ("VALID", "ACCEPTED"):
            print(mp, sku, kind, price, cur, status, issues, flush=True)
        time.sleep(0.4)
    return results


def main():
    mps = sys.argv[1].split(",")
    apply = "--apply" in sys.argv
    kinds = kinds_by_asin()
    os.makedirs("logs", exist_ok=True)
    for mp in mps:
        res = run(mp, apply, kinds)
        with open(f"logs/business_prices_{mp}_{'apply' if apply else 'preview'}.json", "w") as fh:
            json.dump(res, fh, indent=1)
        from collections import Counter
        print(mp, len(res), dict(Counter(v["status"] for v in res.values())),
              "had B2B:", sum(1 for v in res.values() if v["oldB2B"] is not None),
              "B2B != price:", sum(1 for v in res.values() if v["oldB2B"] is not None and abs(v["oldB2B"] - v["price"]) > 0.005), flush=True)


if __name__ == "__main__":
    main()
