"""Weekly low-inventory warning (2026-10-04, per the user: "weekly inventory
check, warn me when I have less than 30 days of inventory, based on the last
3 months of sales - USA and EU, skip UK for now").

Per the user (2026-10-04): the check is only "is FBA stock enough or do we
need to replenish FBA" - so stock = FBA only, live from the SP-API FBA
Inventory API (getInventorySummaries, details=true): fulfillable + FC
transfer (reserved pendingTransshipment + fcProcessing) + inbound
(working + shipped + receiving). AWD, LG and OTW are NOT counted.
Only products with stock to replenish from are listed (per the user
2026-10-04): AWD on hand (USA) or AWD/LG warehouse (EU), from sku_statistics.
Days = that stock / sales per day over the last 3 months, counting only
days the product was in stock (same "A / recent" rate and in-stock-days
rule as the Next Order page). Regions: usa (US stock, US+CA+MX sales) and de (Pan-EU
stock, EU sales). Products in an IGNORE group or marked end-of-life are
skipped (load_mapping); products with no sales in 3 months never warn.
One line per product (ASIN), SKU spellings sharing an ASIN are merged.
Sent Mondays to @baba_update_bot via NotificationRouting.notify.
"""
import json
import os
import time
from types import SimpleNamespace

from sp_api.api import Inventories
from sp_api.base import Marketplaces

from GetDaysOfSupplyBySku import GetDaysOfSupplyBySku
from NotificationRouting import notify

JOB_NAME = "inventory-low-stock-weekly"
THRESHOLD_DAYS = 30
REGIONS = {"usa": "🇺🇸 USA", "de": "🇪🇺 EU"}
# region -> (credential suffix, marketplace); DE returns the Pan-EU pool.
FBA_SOURCES = {"usa": ("USA", Marketplaces.US), "de": ("EU", Marketplaces.DE)}


def fetch_fba_by_asin(region):
    """{asin: {"fulfillable", "transfer", "inbound"}} summed over SKUs."""
    suffix, mp = FBA_SOURCES[region]
    cred = {"refresh_token": os.environ[f"REFRESH_TOKEN_{suffix}"], "lwa_app_id": os.environ[f"CLIENT_ID_{suffix}"],
            "lwa_client_secret": os.environ[f"CLIENT_SECRET_{suffix}"]}
    api = Inventories(credentials=cred, marketplace=mp)
    out, token = {}, None
    while True:
        kwargs = {"details": True, "marketplaceIds": [mp.marketplace_id]}
        if token:
            kwargs["nextToken"] = token
        resp = api.get_inventory_summary_marketplace(**kwargs)
        for s in resp.payload.get("inventorySummaries", []):
            d = s.get("inventoryDetails") or {}
            res = d.get("reservedQuantity") or {}
            a = out.setdefault(s.get("asin"), {"fulfillable": 0, "transfer": 0, "inbound": 0})
            a["fulfillable"] += d.get("fulfillableQuantity") or 0
            a["transfer"] += (res.get("pendingTransshipmentQuantity") or 0) + (res.get("fcProcessingQuantity") or 0)
            a["inbound"] += sum(d.get(k) or 0 for k in ("inboundWorkingQuantity", "inboundShippedQuantity",
                                                         "inboundReceivingQuantity"))
        token = getattr(resp, "next_token", None) or (resp.pagination or {}).get("nextToken")
        if not token:
            return out
        time.sleep(0.6)


def find_low_stock(by_sku, fba, threshold=THRESHOLD_DAYS):
    out = {r: {} for r in REGIONS}
    for sku, data in by_sku.items():
        meta = data.get("_meta") or {}
        asin = meta.get("asin")
        # Per the user (2026-10-04): the stuffed poufs are not checked.
        if not asin or (meta.get("group") or "").upper().startswith("STUFFED"):
            continue
        for region in REGIONS:
            per_day = (data.get(region) or {}).get("rateRecent") or 0
            if per_day <= 0:
                continue
            parts = (data.get(region) or {}).get("parts") or {}
            reserve = {k: round(parts.get(k) or 0) for k in ("awd", "lg") if (parts.get(k) or 0) > 0}
            if not reserve:
                continue
            f = fba[region].get(asin) or {"fulfillable": 0, "transfer": 0, "inbound": 0}
            stock = f["fulfillable"] + f["transfer"] + f["inbound"]
            days = round(stock / per_day)
            if days >= threshold:
                continue
            prev = out[region].get(asin)
            # Prefer the plain SKU spelling over *_OLD / *_New variants.
            if prev and len(prev["sku"]) <= len(sku):
                continue
            out[region][asin] = {"sku": sku, "asin": asin, "group": meta.get("group") or "", "days": days,
                                 "stock": stock, **f, "perDay": per_day, "reserve": reserve,
                                 "need30": max(0, round(per_day * threshold - stock))}
    return {r: sorted(v.values(), key=lambda x: (x["days"], -x["perDay"])) for r, v in out.items()}


def build_text(low, threshold=THRESHOLD_DAYS):
    lines = [f"📦 Weekly FBA check - under {threshold} days of FBA stock "
             f"(fulfillable + FC transfer + inbound ÷ sales/day, last 3 months) - only items with AWD/LG stock to send"]
    for region, label in REGIONS.items():
        items = low[region]
        lines.append(f"\n{label}: {len(items)} product(s)" + ("" if items else " ✅"))
        for x in items:
            icon = "🔴" if x["days"] <= 7 else "🟠" if x["days"] <= 14 else "🟡"
            parts = [f"{x['fulfillable']} available"] + \
                ([f"{x['transfer']} FC transfer"] if x["transfer"] else []) + \
                ([f"{x['inbound']} inbound"] if x["inbound"] else [])
            reserve = " + ".join(f"{n} {k.upper()}" for k, n in x["reserve"].items())
            lines.append(f"{icon} {x['sku']} ({x['group']}): {x['days']} days · FBA {' + '.join(parts)} · "
                         f"{x['perDay']:.2f}/day · send ≥{x['need30']} (have {reserve})")
    lines.append("\nLegend: 🔴 ≤7 days · 🟠 ≤14 · 🟡 <30. UK not checked.")
    return "\n".join(lines)


def SendInventoryWarning(request):
    args = getattr(request, "args", {}) or {}
    try:
        threshold = int(args.get("threshold") or THRESHOLD_DAYS)
        body, status, *_ = GetDaysOfSupplyBySku(SimpleNamespace(method="POST", args={}))
        data = json.loads(body)
        if status != 200 or "bySku" not in data:
            raise RuntimeError(data.get("error") or f"HTTP {status}")
        fba = {region: fetch_fba_by_asin(region) for region in REGIONS}
        low = find_low_stock(data["bySku"], fba, threshold)
        text = build_text(low, threshold)
        if args.get("dry") != "1":
            notify(JOB_NAME, "amzbot", text)
        return json.dumps({"text": text, "low": low}), 200, {"Content-Type": "application/json"}
    except Exception as exc:
        notify(JOB_NAME, "amzbot", f"🚨 Weekly inventory check failed: {exc}", is_error=True)
        return json.dumps({"error": str(exc)}), 500, {"Content-Type": "application/json"}
