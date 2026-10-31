"""One-off check (2026-10-31 10:00, per the user): did Build International
Listings (US -> JP, run 2026-10-30) create the 15 JP listings, and are they
FBA? Read-only. Uses the JP FBA Inventory API (a SKU shows there once it is
an FBA listing in JP) and the JP catalog. Report to @baba_update_bot.
Usage: python jp_bil_check_2026_10_31.py [--dry]"""
import os
import sys
import time

from sp_api.api import CatalogItems, Inventories
from sp_api.base import Marketplaces

SKUS = ("PAREO_B_546,CoverPoufRoundYellow94,PAREO_B_545,PAREO_B_542,PAREO_B_549,CoverPoufPBlue60_99,"
        "PAREO_B_539,PAREO_B_550,PAREO_B_547,PAREO_B_544,CoverPoufRectBlue93,PAREO_B_548,"
        "CoverPoufRoundPurple10AA,PAREO_B_543,CoverPillowPBlue56A").split(",")
ASINS = {
    "PAREO_B_546": "B0GY1RJXHC", "CoverPoufRoundYellow94": "B07SLQY5F8", "PAREO_B_545": "B0GY1SVJL9",
    "PAREO_B_542": "B0GY1JKDCQ", "PAREO_B_549": "B0GY1X7ZTZ", "CoverPoufPBlue60_99": "B07SNR99TW",
    "PAREO_B_539": "B0GSH8F1RS", "PAREO_B_550": "B0GY1RHXMG", "PAREO_B_547": "B0GY1W99FR",
    "PAREO_B_544": "B0GY25F5M1", "CoverPoufRectBlue93": "B07SMQ1SYH", "PAREO_B_548": "B0GY1GFYWF",
    "CoverPoufRoundPurple10AA": "B083NMKC1L", "PAREO_B_543": "B0GY1JDWCJ", "CoverPillowPBlue56A": "B07N2ZTSXZ",
}
JP = Marketplaces.JP
CRED = dict(refresh_token=os.environ["REFRESH_TOKEN_JP"], lwa_app_id=os.environ["CLIENT_ID_USA"],
            lwa_client_secret=os.environ["CLIENT_SECRET_USA"])


def fba_skus():
    api = Inventories(credentials=CRED, marketplace=JP)
    out, token = {}, None
    while True:
        kw = {"details": True, "granularityType": "Marketplace", "granularityId": JP.marketplace_id,
              "marketplaceIds": [JP.marketplace_id]}
        if token:
            kw["nextToken"] = token
        resp = api.get_inventory_summary_marketplace(**kw)
        for x in (resp.payload or {}).get("inventorySummaries", []):
            out[x.get("sellerSku")] = x
        token = resp.next_token
        if not token:
            return out
        time.sleep(0.6)


def main():
    fba = fba_skus()
    cat = CatalogItems(credentials=CRED, marketplace=JP, version="2022-04-01")
    ok, lines = 0, []
    for sku in SKUS:
        asin = ASINS[sku]
        in_cat = False
        try:
            in_cat = bool(cat.get_catalog_item(asin, marketplaceIds=JP.marketplace_id, includedData="summaries").payload)
        except Exception:
            pass
        row = fba.get(sku) or next((v for v in fba.values() if v.get("asin") == asin), None)
        if row:
            ok += 1
            note = f"✅ FBA (as {row['sellerSku']})" if row["sellerSku"] != sku else "✅ FBA"
        elif in_cat:
            note = "⚠️ in JP catalog, but not an FBA listing of ours yet"
        else:
            note = "❌ not in JP yet"
        lines.append(f"{note} - {sku}")
        time.sleep(0.3)
    text = f"🇯🇵 Japan Build International Listings check: {ok}/{len(SKUS)} listed as FBA\n\n" + "\n".join(lines)
    print(text)
    if "--dry" not in sys.argv:
        from NotificationRouting import notify
        notify("jp-bil-check", "AmzBot", text)


if __name__ == "__main__":
    main()
