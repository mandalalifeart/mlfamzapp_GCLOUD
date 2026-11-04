"""Sales & profit growth page (/growth, per the user 2026-11-04: "make a page
for sales and profit grow so i can follow up").

Per month (orders placed, LA dates, from 2025-01), per marketplace and per
department (pareo / home decor): units, sales, sales w/o VAT, ads (SP+SD + SB
share) and profit - the same product formula as the product page, Best & Worst
ranking and the monthly profit on /checks (ProductCost.product_ranking, USD at
today's rates). Months are computed by UpdateGrowthData (daily cron) and
stored in job_state growth_monthly; finished months are kept, the current
month and the previous one (storage fees land on the 10th) are recomputed.
GetGrowthData returns them for the page."""
import json
from collections import defaultdict
from datetime import date

from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state, set_job_state

STATE_KEY = "growth_monthly"
FIRST = (2025, 1)
MPS = ["usa", "ca", "mex", "uk", "de", "fr", "it", "es", "nl", "be", "ie", "pl", "se"]
KEYS = ("units", "sales", "netSales", "fees", "productCost", "ads", "sbAds", "returns", "storage", "awd")
PLAN_URL = "https://claude.ai/artifact/1eT9w8dYyoLA9mHwERSj2v"
# Product-level ad data (ads_advertised_product_stats) starts 2026-01; before
# that the page uses the marketplace's total ad spend from country_sales.
PRODUCT_ADS_FROM = "2026-01"
CURRENCY = {"usa": "USD", "ca": "CAD", "mex": "MXN", "uk": "GBP", "se": "SEK", "pl": "PLN"}  # others EUR


def _json(body, status=200):
    return json.dumps(body), status, {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}


def _months(start, end):
    y, m = start
    while (y, m) <= end:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def month_totals(pb, y, m, mapping):
    """{mp: {dept: totals}} for orders of y-m."""
    import calendar
    from ProductCost import _section_of_group, product_ranking
    start, end = f"{y}-{m:02d}-01", f"{y}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}"
    out = {}
    for mp in MPS:
        rows = product_ranking(start, end, atomic=[mp], _token=pb)["byAsin"]
        by = defaultdict(lambda: defaultdict(float))
        for asin, r in rows.items():
            dept = _section_of_group(mapping.get(asin)) or "other"
            t = by[dept]
            for k in KEYS:
                t[k] += r.get(k) or 0
            if r.get("profit") is None:
                t["noCostUnits"] += r["units"]
        if by:
            out[mp] = {d: {k: round(v, 2) for k, v in t.items()} for d, t in by.items()}
    return out


def update(today=None, force=None):
    today = today or date.today()
    pb = pb_authenticate()
    state = json.loads(get_job_state(pb, STATE_KEY) or "{}")
    months = state.get("months", {})
    mapping = {r["asin"]: r.get("group") for r in pb_list_all(pb, "asin_group_mapping", 'asin != ""', fields="asin,group")}
    prev = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    redo = {f"{today.year}-{today.month:02d}"}
    if today.day <= 12:  # FBA storage for last month comes on the 10th
        redo.add(f"{prev[0]}-{prev[1]:02d}")
    done = []
    for y, m in _months(FIRST, (today.year, today.month)):
        key = f"{y}-{m:02d}"
        if key in months and key not in redo and key != force:
            continue
        months[key] = month_totals(pb, y, m, mapping)
        done.append(key)
        state.update(months=months, updated=today.isoformat())
        set_job_state(pb, STATE_KEY, json.dumps(state))  # per month, so a crash keeps what is done
    state["ppc"] = ppc_monthly(pb)
    set_job_state(pb, STATE_KEY, json.dumps(state))
    return {"computed": done, "months": len(months)}


def ppc_monthly(pb):
    """{'YYYY-MM': {mp: ad spend USD}} from country_sales (all ad types, own currency -> USD)."""
    from ProductCost import usd
    out = defaultdict(dict)
    for r in pb_list_all(pb, "country_sales", f"year >= {FIRST[0]}", fields="marketplace,year,month,ppc_spend"):
        mp = r["marketplace"]
        if mp in MPS and r.get("ppc_spend") and r.get("month"):
            out[f"{r['year']}-{r['month']:02d}"][mp] = round(usd(r["ppc_spend"], CURRENCY.get(mp, "EUR")), 2)
    return dict(out)


def UpdateGrowthData(request):
    args = getattr(request, "args", {}) or {}
    try:
        return _json(update(date.fromisoformat(args["today"]) if args.get("today") else None, args.get("month")))
    except Exception as exc:
        return _json({"error": str(exc)[:500]}, 500)


def GetGrowthData(request):
    try:
        pb = pb_authenticate()
        state = json.loads(get_job_state(pb, STATE_KEY) or "{}")
        return _json({"months": state.get("months", {}), "updated": state.get("updated"), "planUrl": PLAN_URL,
                      "ppc": state.get("ppc", {}), "productAdsFrom": PRODUCT_ADS_FROM,
                      "today": date.today().isoformat()})
    except Exception as exc:
        return _json({"error": str(exc)[:500]}, 500)
