"""Weekly USA AWD + FBA inventory sync into sku_statistics, added 2026-09-18
at the user's request ("update usa awd and fba data to country statistic
table, lets do it every monday").

The actual pull/write is UpdateUsaInventory.sync_usa_inventory() - this
module only adds the scheduled-job wrapper around it: a before/after diff so
the notification says what actually moved (a bare "107 SKUs written" says
nothing about whether the numbers changed), plus the standing rule that any
change to sku_statistics is followed by a fresh CSV export sent to Telegram
without being asked for.
"""
import csv
import io
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from AdsAuth import cors_headers, json_response
from AdsReporting import ADMIN_KEY
from InventoryHistory import write_inventory_history_snapshot
from NotificationRouting import notify, pb_authenticate
from UpdateUsaInventory import POCKETBASE_STATS_COLLECTION, POCKETBASE_URL, sync_usa_inventory

TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")
SYSTEM_TZ = ZoneInfo("Asia/Jerusalem")
JOB_NAME = "amazon-weekly-usa-inventory-sync"
APP_NAME = "amzbot"

USA_FIELDS = ("usa_balance_fba", "usa_balance_awd", "usa_balance", "usa_on_the_way")
# Column order for the CSV export - the real product/stock fields first, in
# the same left-to-right order the Next Order page shows them, with the
# PocketBase bookkeeping columns dropped entirely (an export meant to be
# read in Excel shouldn't lead with collectionId/id).
EXPORT_FIELDS = (
    "sku", "malani_balance", "malani_order",
    "uk_balance", "uk_balance_fba", "uk_balance_awd", "uk_on_the_way", "uk_next_shipment",
    "de_balance", "de_balance_fba", "de_balance_awd", "de_balance_lg", "de_on_the_way", "de_next_shipment",
    "usa_balance", "usa_balance_fba", "usa_balance_awd",
    "usa_on_the_way", "usa_next_shipment", "next_order",
)


def fetch_stats(token):
    records, page = [], 1
    while True:
        resp = requests.get(
            f"{POCKETBASE_URL}/api/collections/{POCKETBASE_STATS_COLLECTION}/records",
            headers={"Authorization": token},
            params={"perPage": 500, "page": page},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return records




def build_csv(records):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(EXPORT_FIELDS), extrasaction="ignore")
    writer.writeheader()
    for rec in sorted(records, key=lambda r: (r.get("sku") or "").lower()):
        writer.writerow({f: rec.get(f, "") for f in EXPORT_FIELDS})
    return buf.getvalue()


def send_telegram_csv(csv_text, filename, caption):
    """Telegram's sendDocument, the one place in AmzBot that attaches a file
    rather than sending plain text - sendMessage can't carry the export, and
    the export is the point of this notification."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    resp = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
        data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1000]},
        files={"document": (filename, csv_text.encode("utf-8"), "text/csv")},
        timeout=60,
    )
    return resp.ok


def summarize(before, after, result):
    """Before/after totals plus the biggest per-SKU movements, so the weekly
    message answers "did anything actually change" rather than just "the job
    ran"."""
    before_by_sku = {r.get("sku"): r for r in before if r.get("sku")}
    after_by_sku = {r.get("sku"): r for r in after if r.get("sku")}

    def total(rows, field):
        return sum(int(r.get(field) or 0) for r in rows)

    lines = [
        f"\U0001f4e6 Weekly USA inventory sync - {datetime.now(SYSTEM_TZ).strftime('%Y-%m-%d')}",
        "",
        f"SKUs written: {result['skusWritten']}",
        "",
    ]
    for field, label in (
        ("usa_balance_fba", "FBA available"),
        ("usa_balance_awd", "AWD on hand"),
        ("usa_balance", "USA total"),
        ("usa_on_the_way", "AWD inbound"),
    ):
        b, a = total(before, field), total(after, field)
        delta = a - b
        arrow = "→" if delta == 0 else ("↑" if delta > 0 else "↓")
        lines.append(f"{label}: {b:,} {arrow} {a:,} ({delta:+,})")

    changed = []
    for sku, after_rec in after_by_sku.items():
        before_rec = before_by_sku.get(sku)
        if not before_rec:
            continue
        b, a = int(before_rec.get("usa_balance") or 0), int(after_rec.get("usa_balance") or 0)
        if b != a:
            changed.append((sku, b, a))
    changed.sort(key=lambda x: -abs(x[2] - x[1]))

    lines.append("")
    lines.append(f"SKUs with a changed USA balance: {len(changed)}")
    for sku, b, a in changed[:8]:
        lines.append(f"  {sku}: {b} → {a} ({a - b:+d})")
    if len(changed) > 8:
        lines.append(f"  ...and {len(changed) - 8} more")

    created = sorted(set(after_by_sku) - set(before_by_sku))
    if created:
        lines.append("")
        lines.append(f"⚠️ New sku_statistics rows created: {len(created)} - {', '.join(created[:10])}")

    for key, label in (("fbaUnmappedSkus", "FBA"), ("awdUnmappedSkus", "AWD")):
        unmapped = result.get(key) or []
        if unmapped:
            lines.append("")
            lines.append(f"⚠️ Unmapped {label} SKUs (stock Amazon reports but no ASIN mapping): {len(unmapped)}")
            lines.append("  " + ", ".join(unmapped[:10]))

    return "\n".join(lines)


def RunWeeklyUsaInventorySync(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    try:
        token = pb_authenticate()
        before = fetch_stats(token)
        result = sync_usa_inventory()
        after = fetch_stats(token)

        week_date = datetime.now(SYSTEM_TZ).strftime("%Y-%m-%d")
        write_inventory_history_snapshot(token, result.get("written") or [], "usa", week_date, "usa_balance")

        text = summarize(before, after, result)
        csv_text = build_csv(after)
        filename = f"sku_statistics_{datetime.now(SYSTEM_TZ).strftime('%Y-%m-%d')}.csv"
        csv_sent = send_telegram_csv(csv_text, filename, text)

        # The CSV's caption already carried the summary, so a successful
        # send only needs the job_runs entry - going through notify() too
        # would Telegram the same text a second time.
        if csv_sent:
            _log_only(token, text)
        else:
            notify(JOB_NAME, APP_NAME, text, is_error=False, status="success")

        return json_response({
            "status": "success",
            "skusWritten": result["skusWritten"],
            "csvSent": csv_sent,
            "summary": text,
        })
    except Exception as exc:
        notify(JOB_NAME, APP_NAME, f"❌ Weekly USA inventory sync FAILED\n\n{exc}", is_error=True,
               subject="AmzBot: weekly USA inventory sync failed")
        return json_response({"status": "error", "error": str(exc), "type": exc.__class__.__name__}, 500)


def _log_only(token, text):
    """Records the job_runs entry without re-sending the text to Telegram
    (the CSV's caption already carried it) - same shape NotificationRouting
    writes, kept here rather than widening notify()'s signature for this one
    caller."""
    from NotificationRouting import _log_run
    _log_run(token, JOB_NAME, APP_NAME, "success", text, True, False)
