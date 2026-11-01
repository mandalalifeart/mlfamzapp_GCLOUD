"""Monthly FBA storage fee per ASIN (per the user 2026-11-01: returns + storage
costs per item). Amazon's GET_FBA_STORAGE_FEE_CHARGES_DATA, one row per ASIN x
fulfilment centre x month; US creds = US, EU creds (DE marketplace) = all EU
countries incl. GB. Stored in `fba_storage_monthly`, delete-then-insert per
(region, month). Endpoint UpdateFbaStorageFees (args month=YYYY-MM, default
the previous month; region=usa|eu, default both) - cron on the 10th."""
import csv
import io
import json
from datetime import date

from AsinAnalytics import pb_authenticate, pb_list_all
from UpdateSkuSalesMonth import pb_batch
from WeeklyChecks import run_report, _json

COLLECTION = "fba_storage_monthly"
REGIONS = {"usa": "US", "eu": "DE"}


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def pull_month(region, month):
    y, m = map(int, month.split("-"))
    nxt = f"{y + (m == 12):04d}-{m % 12 + 1:02d}-01"
    text = run_report(REGIONS[region], "GET_FBA_STORAGE_FEE_CHARGES_DATA",
                      start=f"{month}-01T00:00:00Z", end=f"{nxt}T00:00:00Z", wait_minutes=25)
    rows = []
    for r in csv.DictReader(io.StringIO(text or ""), delimiter="\t"):
        if (r.get("month_of_charge") or "")[:7] != month:
            continue
        rows.append({"month": month, "region": region, "country": r.get("country_code") or "",
                     "asin": r.get("asin") or "", "fnsku": r.get("fnsku") or "",
                     "fc": (r.get("fulfillment_center") or "").strip("'"),
                     "avg_qty": _f(r.get("average_quantity_on_hand")), "item_volume": _f(r.get("item_volume")),
                     "volume_units": r.get("volume_units") or "", "size_tier": r.get("product_size_tier") or "",
                     "fee": _f(r.get("estimated_monthly_storage_fee")), "currency": r.get("currency") or ""})
    token = pb_authenticate()
    old = pb_list_all(token, COLLECTION, f'region = "{region}" && month = "{month}"', fields="id")
    ops = [{"method": "DELETE", "url": f"/api/collections/{COLLECTION}/records/{r['id']}"} for r in old]
    ops += [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": b} for b in rows]
    for i in range(0, len(ops), 50):
        pb_batch(token, ops[i:i + 50])
    return {"rows": len(rows), "fee": round(sum(r["fee"] for r in rows), 2), "reportEmpty": text is None}


def UpdateFbaStorageFees(request):
    args = getattr(request, "args", {}) or {}
    month = args.get("month")
    if not month:
        t = date.today()
        month = f"{t.year - (t.month == 1):04d}-{(t.month - 2) % 12 + 1:02d}"
    out = {}
    for region in ([args["region"]] if args.get("region") else REGIONS):
        try:
            out[region] = pull_month(region, month)
        except Exception as exc:
            out[region] = {"error": str(exc)[:300]}
    return _json({"month": month, **out})
