"""Stored weekly + monthly check reports for the /checks page (per the user
2026-11-02: "the Monday file is unreadable - make it a page, store every week
and month, formatted, collapsible sections").

`check_reports` = one row per (kind, period, job): kind weekly (period = the
Monday, YYYY-MM-DD) or monthly (period = the month the check is about,
YYYY-MM), label, text, flags ({"🔴": n, ...}). Weekly rows come from the
Monday capture folder (MondayDigest stores them when it runs); monthly rows
from NotificationRouting.notify for the jobs in MONTHLY_JOBS.
Endpoint GetCheckReports?kind=&period= (local server, read only)."""
import json
from datetime import date, datetime, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all
from UpdateSkuSalesMonth import pb_batch

COLLECTION = "check_reports"
MARKERS = ("🔴", "🟠", "⚠️", "🚨", "🚩", "❌")
MONTHLY_JOBS = {  # notify job name -> label
    "monthly-profit": "💰 Monthly profit",
    "reimbursement-check": "💸 Reimbursements (lost / damaged)",
    "storage-check": "🏬 FBA storage / aged stock",
    "competitor-price-check": "🏷 Competitor prices",
    "accounting-check": "🧾 Accounting / VAT",
    "content-gap-check": "🖼 Content gaps (A+ / video / images)",
    "returns-monthly-check": "↩️ Returns",
}


def flags_of(text):
    lines = [ln.strip() for ln in (text or "").splitlines()]
    return {m: n for m in MARKERS if (n := sum(1 for ln in lines if ln.startswith(m)))}


def save_report(kind, period, job, label, text, token=None):
    token = token or pb_authenticate()
    old = pb_list_all(token, COLLECTION, f'kind = "{kind}" && period = "{period}" && job = "{job}"', fields="id")
    ops = [{"method": "DELETE", "url": f"/api/collections/{COLLECTION}/records/{r['id']}"} for r in old]
    ops.append({"method": "POST", "url": f"/api/collections/{COLLECTION}/records",
                "body": {"kind": kind, "period": period, "job": job, "label": label, "text": text,
                         "flags": flags_of(text), "ran_at": datetime.now().isoformat(timespec="seconds")}})
    pb_batch(token, ops)


def save_monthly_from_notify(job_name, text):
    """Called by NotificationRouting.notify; the check is about last month."""
    if job_name not in MONTHLY_JOBS:
        return
    first = date.today().replace(day=1)
    prev = first - timedelta(days=1)
    save_report("monthly", f"{prev.year:04d}-{prev.month:02d}", job_name, MONTHLY_JOBS[job_name], text)


def GetCheckReports(request):
    args = getattr(request, "args", {}) or {}
    try:
        token = pb_authenticate()
        rows = pb_list_all(token, COLLECTION, "", fields="kind,period")
        periods = {"weekly": sorted({r["period"] for r in rows if r["kind"] == "weekly"}, reverse=True),
                   "monthly": sorted({r["period"] for r in rows if r["kind"] == "monthly"}, reverse=True)}
        kind = args.get("kind") or "weekly"
        period = args.get("period") or (periods[kind][0] if periods.get(kind) else "")
        reports = pb_list_all(token, COLLECTION, f'kind = "{kind}" && period = "{period}"',
                              fields="job,label,text,flags,ran_at") if period else []
        from MondayDigest import LABELS
        order = {name: i for i, (name, _l) in enumerate(LABELS)}
        order.update({j: i for i, j in enumerate(MONTHLY_JOBS)})
        reports.sort(key=lambda r: (order.get(r["job"], 99), r["label"]))
        return json.dumps({"kind": kind, "period": period, "periods": periods, "reports": reports}), 200, \
            {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}
