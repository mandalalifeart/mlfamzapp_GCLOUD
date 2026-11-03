"""Stock valuation for the accounts (per the user 2026-11-03: the accountant
never asked for the stock value, so the year-end stock in the annual report was
a balancing number - "prepare the report as part of our accounting, another
section on the wise page").

One snapshot per date in PocketBase `stock_valuations` (as_of, method, eur_rate,
totals json, rows json). Units per product (ASIN) at the date:
  method "backcalc" (a past date): units now (sku_statistics: FBA + AWD +
    Lagerpark + on the way, every region) + units sold after the date
    (order_items, every channel incl. Etsy MCF, not cancelled) - units on
    supplier invoices dated after the date. Below 0 -> 0 (flagged).
  method "snapshot" (today, e.g. the cron on the last day of each month): units
    now from sku_statistics.
  FBA units at the date from Amazon's Inventory Ledger (US + Pan-EU/UK) are
  shown next to it as a check (Amazon keeps ~18 months of ledger).
Value = units x purchase price on the last FOB supplier invoice dated on or
before the date (else the first later one, else the median of its section).
EUR at the ECB rate of the date. "With duty + freight" adds ProductCost's
REGION_IMPORT per region, split by where the product's stock is now.
Stock held at Malani (not invoiced yet) is not ours and not counted."""
import csv
import io
import json
import os
import statistics
from collections import defaultdict
from datetime import date, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all, run_reports
from ProductCost import REGION_IMPORT, _section_of_group, invoice_lines
from UpdateSkuSalesMonth import pb_batch

COLLECTION = "stock_valuations"
STOCK_FIELDS = {  # sku_statistics field -> import region
    "usa_balance": "usa", "usa_on_the_way": "usa", "de_balance": "eu", "de_on_the_way": "eu",
    "uk_balance": "uk", "uk_on_the_way": "uk", "au_balance": "other", "au_on_the_way": "other",
    "jp_balance": "other", "jp_on_the_way": "other",
}
LEDGER_SOURCES = {"usa": ("usa", "ATVPDKIKX0DER"), "eu": ("de", "A1PA6795UKMFR9")}


def _cors(methods="GET, POST, OPTIONS"):
    return {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": methods,
            "Access-Control-Allow-Headers": "Content-Type", "Content-Type": "application/json"}


def usd_to_eur(day):
    """EUR per USD on `day` (ECB, last business day on or before)."""
    from WiseLedger import _EUR_RATE_CACHE, eur_rates_for
    start = (date.fromisoformat(day) - timedelta(days=7)).isoformat()
    eur_rates_for(["USD"], start, day)
    rate = _EUR_RATE_CACHE.get(("USD", day))
    return round(1 / rate, 6) if rate else None


def fba_ledger_units(as_of):
    """{asin: {"US"|"EU"|"UK": units}} ending balance (+ in transit between FCs) on as_of."""
    specs = [{"mp": src, "region": cred, "report_type": "GET_LEDGER_SUMMARY_VIEW_DATA",
              "start": f"{as_of}T00:00:00Z", "end": f"{as_of}T23:59:59Z", "marketplace_id": mid,
              "options": {"aggregateByLocation": "COUNTRY", "aggregatedByTimePeriod": "DAILY"}}
             for src, (cred, mid) in LEDGER_SOURCES.items()]
    out = defaultdict(lambda: defaultdict(int))
    errors = []

    def on_done(job, payload, error):
        if error:
            errors.append(f"{job['mp']}: {error}")
            return
        for row in csv.DictReader(io.StringIO(payload or ""), delimiter="\t"):
            loc = (row.get("Location") or "").upper()
            reg = "US" if job["mp"] == "usa" else ("UK" if loc in ("GB", "UK") else "EU")
            try:
                n = int(float(row.get("Ending Warehouse Balance") or 0)) + int(float(row.get("In Transit Between Warehouses") or 0))
            except ValueError:
                continue
            if row.get("ASIN"):
                out[row["ASIN"]][reg] += n
    run_reports(specs, on_done, raw_text=True)
    return {a: dict(v) for a, v in out.items()}, errors


def compute(as_of, method="backcalc", with_ledger=True, token=None):
    token = token or pb_authenticate()
    mapping = pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,sku,ean,group,end_of_life")
    info, sku2asin, ean2asin = {}, {}, {}
    for r in mapping:
        if (r.get("group") or "").upper() == "IGNORE":
            continue
        info.setdefault(r["asin"], r)
        sku2asin[r["sku"]] = r["asin"]
        if r.get("ean"):
            ean2asin[r["ean"].lstrip("0")] = r["asin"]

    now_units = defaultdict(float)
    now_region = defaultdict(lambda: defaultdict(float))
    for r in pb_list_all(token, "sku_statistics", ""):
        a = sku2asin.get(r["sku"])
        if not a:
            continue
        for f, reg in STOCK_FIELDS.items():
            v = float(r.get(f) or 0)
            if v > 0:
                now_units[a] += v
                now_region[a][reg] += v

    sold, invoiced = defaultdict(float), defaultdict(float)
    if method == "backcalc":
        for ln in pb_list_all(token, "order_items", f'purchase_date > "{as_of}" && order_status != "Cancelled"',
                              fields="asin,qty"):
            if ln.get("asin"):
                sold[ln["asin"]] += ln.get("qty") or 0
    lines = sorted(invoice_lines(token), key=lambda ln: (ln["date"], ln["invoice"]))
    price_before, price_after = {}, {}
    for ln in lines:
        a = ean2asin.get(ln["ean"].lstrip("0"))
        if not a:
            continue
        if method == "backcalc" and ln["date"] > as_of:
            invoiced[a] += ln["qty"]
        if ln["cif"]:
            continue
        if ln["date"] <= as_of:
            price_before[a] = (ln["price"], ln["invoice"])
        elif a not in price_after:
            price_after[a] = (ln["price"], ln["invoice"])
    known = defaultdict(list)
    for a, (p, _inv) in {**price_after, **price_before}.items():
        known[_section_of_group(info.get(a, {}).get("group")) or "other"].append(p)
    median = {k: statistics.median(v) for k, v in known.items()}
    all_median = statistics.median([p for v in known.values() for p in v]) if known else 0

    ledger, ledger_errors = (fba_ledger_units(as_of) if with_ledger else ({}, []))
    eur = usd_to_eur(as_of) or 0.0
    rows = []
    for a in sorted(set(now_units) | set(sold) | set(invoiced) | set(ledger)):
        if a not in info:
            continue
        units = now_units[a] + sold[a] - invoiced[a]
        clipped = units < 0
        units = max(units, 0)
        sec = _section_of_group(info[a].get("group")) or "other"
        if a in price_before:
            price, source = price_before[a][0], price_before[a][1]
        elif a in price_after:
            price, source = price_after[a][0], f"{price_after[a][1]} (later invoice)"
        else:
            price, source = median.get(sec, all_median), f"median {sec}"
        regs = now_region.get(a) or {}
        tot = sum(regs.values())
        if tot:
            uplift = sum(v / tot * (REGION_IMPORT[r][0] * price + REGION_IMPORT[r][1]) for r, v in regs.items())
        else:
            uplift = REGION_IMPORT["other"][0] * price + REGION_IMPORT["other"][1]
        fba = ledger.get(a, {})
        if not units and not sum(fba.values()):
            continue
        rows.append({
            "asin": a, "sku": info[a]["sku"], "group": info[a].get("group"), "section": sec,
            "endOfLife": bool(info[a].get("end_of_life")),
            "units": round(units), "unitsNow": round(now_units[a]), "soldSince": round(sold[a]),
            "invoicedSince": round(invoiced[a]), "clipped": clipped,
            "fbaUS": fba.get("US", 0), "fbaEU": fba.get("EU", 0), "fbaUK": fba.get("UK", 0),
            "price": price, "priceSource": source,
            "valueUsd": round(units * price, 2), "valueEur": round(units * price * eur, 2),
            "landedUsd": round(units * (price + uplift), 2), "landedEur": round(units * (price + uplift) * eur, 2),
        })
    rows.sort(key=lambda r: -r["valueUsd"])
    totals = {k: round(sum(r[k] for r in rows), 2) for k in ("units", "valueUsd", "valueEur", "landedUsd", "landedEur",
                                                              "fbaUS", "fbaEU", "fbaUK")}
    totals["bySection"] = {}
    for r in rows:
        s = totals["bySection"].setdefault(r["section"], {"units": 0, "valueEur": 0.0})
        s["units"] += r["units"]
        s["valueEur"] = round(s["valueEur"] + r["valueEur"], 2)
    totals["clippedProducts"] = sum(1 for r in rows if r["clipped"])
    totals["medianPriced"] = sum(r["units"] for r in rows if r["priceSource"].startswith("median"))
    return {"asOf": as_of, "method": method, "eurPerUsd": eur, "computedAt": date.today().isoformat(),
            "ledgerErrors": ledger_errors, "totals": totals, "rows": rows}


def save(snapshot, token=None):
    token = token or pb_authenticate()
    old = pb_list_all(token, COLLECTION, f'as_of = "{snapshot["asOf"]}" && method = "{snapshot["method"]}"')
    body = {"as_of": snapshot["asOf"], "method": snapshot["method"], "eur_rate": snapshot["eurPerUsd"],
            "totals": snapshot["totals"], "rows": snapshot["rows"],
            "meta": {"computedAt": snapshot["computedAt"], "ledgerErrors": snapshot["ledgerErrors"]}}
    if old:
        pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{COLLECTION}/records/{old[0]['id']}", "body": body}])
    else:
        pb_batch(token, [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": body}])


def GetStockValuations(request):
    """?as_of=YYYY-MM-DD -> that snapshot with rows; without -> list of snapshots (totals only)."""
    if request.method == "OPTIONS":
        return "", 204, _cors()
    try:
        token = pb_authenticate()
        as_of = (request.args.get("as_of") or "").strip()
        if as_of:
            recs = pb_list_all(token, COLLECTION, f'as_of = "{as_of}"')
            if not recs:
                return json.dumps({"error": "no stock valuation for this date"}), 404, _cors()
            r = sorted(recs, key=lambda x: x["method"] != "snapshot")[0]
            return json.dumps({"asOf": r["as_of"], "method": r["method"], "eurPerUsd": r.get("eur_rate"),
                               "totals": r.get("totals"), "rows": r.get("rows"), "meta": r.get("meta")}), 200, _cors()
        recs = pb_list_all(token, COLLECTION, "", fields="as_of,method,eur_rate,totals,meta")
        recs.sort(key=lambda x: x["as_of"], reverse=True)
        return json.dumps({"snapshots": [{"asOf": r["as_of"], "method": r["method"], "eurPerUsd": r.get("eur_rate"),
                                          "totals": r.get("totals"), "meta": r.get("meta")} for r in recs]}), 200, _cors()
    except Exception as exc:
        return json.dumps({"error": str(exc)}), 500, _cors()


def UpdateStockValuation(request):
    """Cron (daily 23:40): on the last day of a month, save a "snapshot" of
    today's stock. ?as_of=YYYY-MM-DD&method=backcalc recomputes a past date."""
    args = getattr(request, "args", {}) or {}
    today = date.today()
    as_of = args.get("as_of")
    method = args.get("method") or ("backcalc" if as_of and as_of < today.isoformat() else "snapshot")
    if not as_of:
        if (today + timedelta(days=1)).day != 1 and not args.get("force"):
            return json.dumps({"skipped": "not the last day of the month"}), 200, _cors()
        as_of = today.isoformat()
    snap = compute(as_of, method, with_ledger=args.get("ledger", "1") != "0")
    save(snap)
    return json.dumps({"asOf": as_of, "method": method, "totals": snap["totals"],
                       "ledgerErrors": snap["ledgerErrors"]}), 200, _cors()
