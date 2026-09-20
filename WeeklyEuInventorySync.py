"""Weekly UK + DE FBA inventory sync into sku_statistics, added 2026-09-20
at the user's request ("yes for de/uk"), mirroring WeeklyUsaInventorySync.py's
shape (before/after diff, weekly inventory-history snapshot, CSV export) but
covering both EU regions in one run.
"""
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from AdsAuth import cors_headers, json_response
from AdsReporting import ADMIN_KEY
from InventoryHistory import write_inventory_history_snapshot
from NotificationRouting import notify, pb_authenticate
from UpdateEuInventory import sync_eu_inventory
from WeeklyUsaInventorySync import build_csv, fetch_stats, send_telegram_csv

SYSTEM_TZ = ZoneInfo("Asia/Jerusalem")
JOB_NAME = "amazon-weekly-eu-inventory-sync"
APP_NAME = "amzbot"

REGION_FIELDS = {
    "uk": {"fba": "uk_balance_fba", "balance": "uk_balance", "label": "UK"},
    "de": {"fba": "de_balance_fba", "balance": "de_balance", "label": "DE"},
}


def summarize_region(region, before_by_sku, after_by_sku, result):
    cfg = REGION_FIELDS[region]
    lines = [f"{cfg['label']}: {result['skusWritten']} SKUs written"]

    def total(rows_by_sku, field):
        return sum(int(r.get(field) or 0) for r in rows_by_sku.values())

    for field, label in ((cfg["fba"], "FBA available"), (cfg["balance"], "Total")):
        b, a = total(before_by_sku, field), total(after_by_sku, field)
        delta = a - b
        arrow = "→" if delta == 0 else ("↑" if delta > 0 else "↓")
        lines.append(f"  {label}: {b:,} {arrow} {a:,} ({delta:+,})")

    changed = []
    for sku, after_rec in after_by_sku.items():
        before_rec = before_by_sku.get(sku)
        if not before_rec:
            continue
        b, a = int(before_rec.get(cfg["balance"]) or 0), int(after_rec.get(cfg["balance"]) or 0)
        if b != a:
            changed.append((sku, b, a))
    changed.sort(key=lambda x: -abs(x[2] - x[1]))

    lines.append(f"  Changed: {len(changed)}")
    for sku, b, a in changed[:5]:
        lines.append(f"    {sku}: {b} → {a} ({a - b:+d})")
    if len(changed) > 5:
        lines.append(f"    ...and {len(changed) - 5} more")

    if result.get("unmappedSkus"):
        lines.append(f"  ⚠️ Unmapped SKUs (stock reported, no ASIN mapping): {len(result['unmappedSkus'])}")
        lines.append("    " + ", ".join(result["unmappedSkus"][:10]))

    return "\n".join(lines)


def RunWeeklyEuInventorySync(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    try:
        token = pb_authenticate()
        before = fetch_stats(token)
        before_by_sku = {r.get("sku"): r for r in before if r.get("sku")}

        week_date = datetime.now(SYSTEM_TZ).strftime("%Y-%m-%d")
        results = {}
        for region in ("uk", "de"):
            result = sync_eu_inventory(region)
            results[region] = result
            write_inventory_history_snapshot(token, result.get("written") or [], region, week_date, REGION_FIELDS[region]["balance"])

        after = fetch_stats(token)
        after_by_sku = {r.get("sku"): r for r in after if r.get("sku")}

        summary_lines = [f"\U0001f4e6 Weekly UK+DE inventory sync - {datetime.now(SYSTEM_TZ).strftime('%Y-%m-%d')}", ""]
        for region in ("uk", "de"):
            summary_lines.append(summarize_region(region, before_by_sku, after_by_sku, results[region]))
            summary_lines.append("")
        text = "\n".join(summary_lines).rstrip()

        csv_text = build_csv(after)
        filename = f"sku_statistics_{datetime.now(SYSTEM_TZ).strftime('%Y-%m-%d')}.csv"
        csv_sent = send_telegram_csv(csv_text, filename, text)

        if csv_sent:
            from NotificationRouting import _log_run
            _log_run(token, JOB_NAME, APP_NAME, "success", text, True, False)
        else:
            notify(JOB_NAME, APP_NAME, text, is_error=False, status="success")

        return json_response({
            "status": "success",
            "ukSkusWritten": results["uk"]["skusWritten"],
            "deSkusWritten": results["de"]["skusWritten"],
            "csvSent": csv_sent,
            "summary": text,
        })
    except Exception as exc:
        notify(JOB_NAME, APP_NAME, f"❌ Weekly UK+DE inventory sync FAILED\n\n{exc}", is_error=True,
               subject="AmzBot: weekly UK+DE inventory sync failed")
        return json_response({"status": "error", "error": str(exc), "type": exc.__class__.__name__}, 500)
