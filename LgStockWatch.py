"""Email when an SKU's Lagerpark (LG) stock goes from non-zero to zero, per the
user 2026-11-18 ("send me mail each time item goes from non zero to zero stock
in lg, so I will change its awd replenishment setting to auto").
LG stock = sku_statistics.de_balance_lg (entered by the user). Each run compares
it with the last run's snapshot (job_state `lg_stock_last`); SKUs that dropped
from >0 to 0 are mailed (and sent on Telegram) via notify("lg-stock-zero"),
whose scheduled_jobs row has notify_email_always. The first run only stores the
snapshot. Args: dry=1 (no message, snapshot not saved)."""
import json

from AsinAnalytics import json_response, pb_authenticate, pb_list_all
from JobState import get_job_state, set_job_state

JOB = "lg-stock-zero"


def check(token, dry=False):
    rows = pb_list_all(token, "sku_statistics", 'sku != ""', fields="sku,de_balance_lg")
    now = {r["sku"]: int(r.get("de_balance_lg") or 0) for r in rows}
    raw = get_job_state(token, "lg_stock_last")
    last = json.loads(raw) if raw else None
    asin = {r["sku"]: r["asin"] for r in pb_list_all(token, "asin_group_mapping", 'sku != ""', fields="sku,asin")}
    zero = [] if last is None else sorted(s for s, q in last.items() if q > 0 and now.get(s, 0) == 0)
    if not dry:
        set_job_state(token, "lg_stock_last", json.dumps(now))
    return zero, [(s, asin.get(s, ""), last[s]) for s in zero], last is None


def SendLgZeroStockAlert(request):
    from NotificationRouting import notify
    args = getattr(request, "args", {}) or {}
    dry = bool(args.get("dry"))
    token = pb_authenticate()
    zero, rows, first = check(token, dry)
    if first:
        return json_response({"seeded": True})
    text = ""
    if rows:
        text = ("🏬 Lagerpark (LG) stock is now 0 for these items – switch their AWD replenishment to AUTO:\n\n"
                + "\n".join(f"• {s} ({a}) – was {q} at LG" for s, a, q in rows))
        if not dry:
            notify(JOB, "AmzBot", text, subject=f"LG stock now 0: {', '.join(zero[:5])}{' …' if len(zero) > 5 else ''}")
    return json_response({"zero": zero, "text": text})
