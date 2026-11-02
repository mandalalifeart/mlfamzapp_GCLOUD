"""Ads reviews page backend (2026-10-18, per the user: "update the reviews to
dedicated page"). Every non-dry Monday review (WeeklyChangesReview,
WeeklyBidChangeSummary) is also saved to PocketBase ads_reviews (one row per
job + run date, re-runs replace it) with its Telegram text and structured data
(TACOS / segment tables). GetAdsReviews lists them for /ads-reviews; read-only,
ungated (stored report data only, no Amazon calls).
"""
import json
import os

import requests

from AsinAnalytics import POCKETBASE_URL, pb_authenticate, pb_list_all

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
COLLECTION = "ads_reviews"


def save_review(pb, job, run_date, text, data=None):
    body = {"job": job, "run_date": run_date, "text": text, "data": data or {}}
    found = pb_list_all(pb, COLLECTION, f'job = "{job}" && run_date = "{run_date}"', fields="id")
    url = f"{POCKETBASE_URL}/api/collections/{COLLECTION}/records"
    if found:
        resp = requests.patch(f"{url}/{found[0]['id']}", headers={"Authorization": pb}, json=body, timeout=30)
    else:
        resp = requests.post(url, headers={"Authorization": pb}, json=body, timeout=30)
    resp.raise_for_status()


def _cors():
    return {"Access-Control-Allow-Origin": ALLOWED_ORIGIN, "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type", "Content-Type": "application/json"}


def GetAdsReviews(request):
    if request.method == "OPTIONS":
        return "", 204, _cors()
    try:
        pb = pb_authenticate()
        rows = pb_list_all(pb, COLLECTION, 'job != ""', fields="job,run_date,text,data", sort="-run_date")
        return json.dumps({"reviews": rows[:200]}), 200, _cors()
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, _cors()


# ---- Ads review checklists (2026-11-02, per the user: weekly + monthly ads
# review check lists, approved list). Ticks are stored in job_state
# `ads_checklists` as {"weekly|<Monday YYYY-MM-DD>": {"done": [idx...], "notes": {idx: text}},
# "monthly|<reviewed YYYY-MM>": ...}. Ungated: UI state only, no Amazon calls.
CHECKLISTS = {
    "weekly": [
        "Read the Monday digest headline - any 🔴 is handled first",
        "TACOS per marketplace vs target (US/DE 20%, others 25%)",
        "🔴 campaigns (spend with no sales / ACOS >= 70%): check 60-day history -> cut bid or pause",
        "🟠 spend doubled vs last week: find why (new keyword? Amazon placement?)",
        "🟡 hitting budget 4+ days with good ACOS -> raise budget",
        "Approve or reject the 🌡 temporary bid-cut proposals",
        "Bid changes from 2 weeks ago: keep the winners, undo the losers",
        "Lost exposure: good keywords that lost impressions -> raise bid",
        "Off-Amazon: any 🔴 campaign -> Minimize spend",
        "Stock: no ads on products with low FBA stock (stock guard + low-inventory warning)",
        "Every 2nd week: search-term harvest -> add exact keywords / negatives",
        "New or changed campaigns are on the watch list",
        "Every 2nd week: products losing money (💸 in the review) -> 🔴 approve ad pauses, 🟠 decide price / coupon",
    ],
    "monthly": [
        "Last month per marketplace: spend, ad sales, ACOS, TACOS vs the month before and last year",
        "Home decor vs pareo: TACOS by department + profit after ads (Best & Worst -> margin after ads)",
        "Products: worst margin after ads / high ACOS -> cut or stop ads; best -> push more",
        "Campaign types: SP vs SB vs SB video vs SD ACOS -> shift budget to what works",
        "Placements: top of search / product pages / off-Amazon -> adjust placement bids",
        "Budgets: re-split by season (pareo off-season until Feb 20, Q4 home decor)",
        "Keyword clean-up: all targets with 60 days and no sales",
        "Competitors and prices: monthly competitor price check, coupons and promotions ending",
        "New products without ads + products with stock but no campaign",
        "USA home-decor ads plan: next step",
    ],
}
CHECKLIST_KEY = "ads_checklists"


def current_periods(today=None):
    from datetime import date, timedelta
    today = today or date.today()
    monday = today - timedelta(days=today.weekday())
    first = today.replace(day=1)
    prev = first - timedelta(days=1)
    return {"weekly": monday.isoformat(), "monthly": f"{prev.year:04d}-{prev.month:02d}"}


def _load_checklists(pb):
    from JobState import get_job_state
    v = get_job_state(pb, CHECKLIST_KEY)
    if isinstance(v, str):
        v = json.loads(v or "{}")
    return v or {}


def _post_cors():
    return {**_cors(), "Access-Control-Allow-Methods": "GET, POST, OPTIONS"}


def GetAdsChecklist(request):
    if request.method == "OPTIONS":
        return "", 204, _post_cors()
    try:
        pb = pb_authenticate()
        state = _load_checklists(pb)
        return json.dumps({"items": CHECKLISTS, "current": current_periods(), "state": state}), 200, _post_cors()
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, _post_cors()


def SetAdsChecklistItem(request):
    """POST {kind, period, index, done?, note?}."""
    if request.method == "OPTIONS":
        return "", 204, _post_cors()
    body = request.get_json(silent=True) or {}
    kind, period = body.get("kind"), str(body.get("period") or "")
    try:
        idx = int(body.get("index"))
    except (TypeError, ValueError):
        return json.dumps({"error": "bad index"}), 400, _post_cors()
    if kind not in CHECKLISTS or not 0 <= idx < len(CHECKLISTS[kind]) or not period or len(period) > 10:
        return json.dumps({"error": "bad kind/period/index"}), 400, _post_cors()
    try:
        from JobState import set_job_state
        pb = pb_authenticate()
        state = _load_checklists(pb)
        entry = state.setdefault(f"{kind}|{period}", {"done": [], "notes": {}})
        if body.get("done") is not None:
            done = set(entry.get("done") or [])
            (done.add if body["done"] else done.discard)(idx)
            entry["done"] = sorted(done)
        if body.get("note") is not None:
            note = str(body["note"])[:1000]
            if note:
                entry.setdefault("notes", {})[str(idx)] = note
            else:
                entry.get("notes", {}).pop(str(idx), None)
        set_job_state(pb, CHECKLIST_KEY, json.dumps(state))
        return json.dumps({"ok": True, "entry": entry}), 200, _post_cors()
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, _post_cors()
