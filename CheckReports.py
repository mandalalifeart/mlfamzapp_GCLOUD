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
    "accounting-check": "🧾 Accounting (Wise, invoices, suppliers)",
    "content-gap-check": "🖼 Content gaps (A+ / video / images)",
    "returns-monthly-check": "↩️ Returns",
}
# Written by hand (Claude) when the monthly reviews are done in the chat:
#   save_report("monthly", "YYYY-MM", "ads-monthly-review", "📊 Ads monthly review", text)
#   save_report("monthly", "YYYY-MM", "losing-products", "💸 Products losing money", text)
MANUAL_MONTHLY = {"ads-monthly-review": "📊 Ads monthly review", "ads-monthly-auto": "🤖 Ads monthly review (automatic)",
                  "losing-products": "💸 Products losing money"}


def flags_of(text):
    lines = [ln.strip() for ln in (text or "").splitlines()]
    return {m: n for m in MARKERS if (n := sum(1 for ln in lines if ln.startswith(m)))}


def save_report(kind, period, job, label, text, token=None, proposals=None):
    """proposals: [{id, section, title, detail, action?, status}] - on a re-run,
    a proposal with the same title keeps the status the user already gave it."""
    token = token or pb_authenticate()
    old = pb_list_all(token, COLLECTION, f'kind = "{kind}" && period = "{period}" && job = "{job}"', fields="id,proposals")
    if proposals is not None:
        before = {p["title"]: p for r in old for p in (r.get("proposals") or [])}
        for p in proposals:
            o = before.get(p["title"])
            if o and o.get("status") != "proposed":
                p.update({k: o[k] for k in ("status", "result", "decided_at") if k in o})
    elif old and old[0].get("proposals"):
        proposals = old[0]["proposals"]
    ops = [{"method": "DELETE", "url": f"/api/collections/{COLLECTION}/records/{r['id']}"} for r in old]
    ops.append({"method": "POST", "url": f"/api/collections/{COLLECTION}/records",
                "body": {"kind": kind, "period": period, "job": job, "label": label, "text": text,
                         "flags": flags_of(text), "ran_at": datetime.now().isoformat(timespec="seconds"),
                         "proposals": proposals or []}})
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
                              fields="id,job,label,text,flags,ran_at,proposals") if period else []
        for r in reports:  # the page doesn't need the executable payloads
            for p in r.get("proposals") or []:
                p["hasAction"] = bool(p.pop("action", None))
        from MondayDigest import LABELS
        order = {name: i for i, (name, _l) in enumerate(LABELS)}
        order.update({j: i for i, j in enumerate(list(MANUAL_MONTHLY) + list(MONTHLY_JOBS))})
        reports.sort(key=lambda r: (order.get(r["job"], 99), r["label"]))
        return json.dumps({"kind": kind, "period": period, "periods": periods, "reports": reports}), 200, \
            {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}


# ---------------- proposals: approve on the page, apply by cron ----------------

def _cors_post():
    return {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type", "Content-Type": "application/json"}


def SetCheckProposal(request):
    """POST {reportId, id, status: approved|rejected|proposed} + ?key=PROPOSAL_KEY.
    Only records the decision - ApplyApprovedProposals does the Amazon writes."""
    import os
    if request.method == "OPTIONS":
        return "", 204, _cors_post()
    if (getattr(request, "args", {}) or {}).get("key") != os.environ.get("PROPOSAL_KEY", "~"):
        return json.dumps({"error": "Unauthorized"}), 401, _cors_post()
    try:
        body = request.get_json(silent=True) or {}
        status = body.get("status")
        if status not in ("approved", "rejected", "proposed"):
            return json.dumps({"error": "bad status"}), 400, _cors_post()
        token = pb_authenticate()
        rec = pb_list_all(token, COLLECTION, f'id = "{body.get("reportId")}"')
        if not rec:
            return json.dumps({"error": "report not found"}), 404, _cors_post()
        rec = rec[0]
        props = rec.get("proposals") or []
        p = next((x for x in props if x.get("id") == body.get("id")), None)
        if not p:
            return json.dumps({"error": "proposal not found"}), 404, _cors_post()
        if p.get("status") in ("applied", "applying"):
            return json.dumps({"error": "already applied"}), 409, _cors_post()
        p["status"], p["decided_at"] = status, datetime.now().isoformat(timespec="seconds")
        pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{COLLECTION}/records/{rec['id']}",
                          "body": {"proposals": props}}])
        return json.dumps({"ok": True, "status": status}), 200, _cors_post()
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, _cors_post()


def _apply_action(pb, action, label):
    from datetime import date as _d
    from datetime import timedelta as _td
    done, failed = [], []
    if action["type"] == "pause_product_ads":
        from JobState import get_job_state, set_job_state
        from ProductAdPauses import _set_state
        pauses = json.loads(get_job_state(pb, "product_ad_pauses") or "[]")
        today = _d.today()
        for x in action["items"]:
            try:
                ad_id = _set_state(pb, x, "PAUSED", f"{label}: approved on the /checks page")
                pauses = [p for p in pauses if p["key"] != x["key"]]
                pauses.append({**{k: x[k] for k in ("key", "profile_id", "campaign_id", "campaign", "ad_group_id", "asin",
                                                    "sku", "country", "history")},
                               "ad_id": ad_id, "paused_on": today.isoformat(),
                               "unpause_on": (today + _td(days=x["pause_days"])).isoformat(), "status": "active"})
                done.append(f"{x['campaign']} {x['sku'] or x['asin']}")
            except Exception as exc:
                failed.append(f"{x['campaign']} {x['sku'] or x['asin']}: {str(exc)[:120]}")
        set_job_state(pb, "product_ad_pauses", json.dumps(pauses))
    elif action["type"] == "pause_targets":
        from AdsBidWriter import find_profile_connection, record_bid_change, refresh_access_token, write_bid_or_state
        tokens = {}
        for x in action["items"]:
            try:
                conn = find_profile_connection(pb, x["profile_id"])
                if x["profile_id"] not in tokens:
                    tokens[x["profile_id"]] = refresh_access_token(conn["profile_key"], conn["refresh_token"])
                is_kw = x["match_type"] in ("EXACT", "PHRASE", "BROAD")
                resp = write_bid_or_state(conn, tokens[x["profile_id"]], x["profile_id"], x["ad_product"], is_kw,
                                          x["target_id"], x["campaign_id"], x["ad_group_id"], x["bid"], state="PAUSED")
                record_bid_change(pb, {"target_id": x["target_id"], "campaign_id": x["campaign_id"], "campaign_name": x["campaign"],
                                       "target_text": x["text"], "ad_product": x["ad_product"], "country_code": x["country"],
                                       "profile_id": x["profile_id"], "old_bid": x["bid"], "new_bid": x["bid"],
                                       "changed_at": _d.today().isoformat(), "action": "disable", "status": "applied",
                                       "reason": f"{label}: no sale ever, approved on the /checks page",
                                       "amazon_response": json.dumps(resp)[:2000]})
                done.append(x["text"])
            except Exception as exc:
                failed.append(f"{x['text']}: {str(exc)[:120]}")
    else:
        raise ValueError(f"unknown action {action['type']}")
    return done, failed


def ApplyApprovedProposals(request):
    """Cron every 10 min: run approved proposals that have an action."""
    from NotificationRouting import notify
    token = pb_authenticate()
    msgs = []
    for rec in pb_list_all(token, COLLECTION, 'proposals ~ "approved"'):
        props = rec.get("proposals") or []
        changed = False
        for p in props:
            if p.get("status") != "approved" or not p.get("action"):
                continue
            p["status"] = "applying"
            pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{COLLECTION}/records/{rec['id']}",
                              "body": {"proposals": props}}])
            try:
                done, failed = _apply_action(token, p["action"], f"{rec['label']} {rec['period']} #{p['id']}")
                p["status"] = "applied" if not failed else ("failed" if not done else "applied")
                p["result"] = f"done {len(done)}" + (f", failed {len(failed)}: " + "; ".join(failed[:5]) if failed else "")
            except Exception as exc:
                p["status"], p["result"] = "failed", str(exc)[:300]
            changed = True
            msgs.append(f"{'✅' if p['status'] == 'applied' else '❌'} {rec['period']} #{p['id']} {p['title']} - {p['result']}")
        if changed:
            pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{COLLECTION}/records/{rec['id']}",
                              "body": {"proposals": props}}])
    manual = [f"• {r['period']} #{p['id']} {p['title']}" for r in pb_list_all(token, COLLECTION, 'proposals ~ "approved"')
              for p in r.get("proposals") or [] if p.get("status") == "approved" and not p.get("action")]
    if msgs:
        text = "📋 Approved proposals applied:\n" + "\n".join(msgs)
        if manual:
            text += "\n\nApproved, to be done by Claude (tell me \"do the approved proposals\"):\n" + "\n".join(manual)
        notify("proposals-apply", "amzbot", text)
    return json.dumps({"applied": len(msgs)}), 200, {"Content-Type": "application/json"}
