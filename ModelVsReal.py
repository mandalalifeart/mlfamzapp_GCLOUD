"""Model vs real profit check (per the user 2026-11-03, "D": the product pages
show ~20% margin per product but the real result is far lower - find where the
difference goes). Monthly on the 2nd (/checks, "model-vs-real"), for last month
and for the year to date. Everything in USD at today's rate.

Model = ProductCost.product_ranking over every Amazon store (order dates):
  sales, VAT removed, Amazon fees (referral % + FBA fee estimates), product cost
  (purchase + duty + freight by weight), ads (SP+SD + SB share), returns,
  storage (FBA + Lagerpark), AWD.
Real =
  Amazon settlements of the period (amazon_expense_events by settlement month):
    sales, refunds, referral + FBA fees, storage + AWD, returns processing,
    import duty/freight billed by Amazon (AGL), every other Amazon fee;
    "Advertising" and "Tax Collected" left out (ads come from country_sales
    ppc_spend = everything Amazon billed for ads, card or payout).
  + Etsy payouts on Wise (Etsy orders are not in the model; their FBA (MCF)
    fulfilment fees are inside the settlements' other fees).
  Invoices on /wise dated in the period (booked_date for supplier invoices):
    supplier purchases, Unicargo / Xhipment (freight + duty, VAT excluded),
    Lagerpark (VAT excluded), VAT returns paid, other overheads; the owner's
    consulting receipts are shown apart (not a cost of the products).
  + stock change = stock value (with duty + freight) at the end - at the start,
    from StockValuation (worked back if no month-end snapshot is saved).
Real profit = settlements (without ads/tax) - ppc ads + Etsy - purchases - freight
  - Lagerpark - VAT - overheads + stock change."""
import calendar
import json
from collections import defaultdict
from datetime import date, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all
from ProductCost import ATOMIC, landed_cost, product_ranking, usd

ALL_MPS = sorted(ATOMIC)
SALES_CATS = ("Sales Revenue", "Shipping Charged to Buyer", "FBA Liquidation Revenue")
FEE_CATS = ("Referral Fee", "FBA Fulfillment Fee")
STORAGE_CATS = ("FBA Storage Fee", "AWD Fee")
SKIP_CATS = ("Tax Collected", "Advertising", "Other: DebtRecovery")
CUR_OF_MP = {"usa": "USD", "ca": "CAD", "mex": "MXN", "uk": "GBP", "pl": "PLN", "se": "SEK"}
VAT_OFFICES = ("Finanzamt", "Directorate General", "Direction Générale", "Naczelnik", "NBP", "Czech Tax", "Finanční",
               "Revenue Commis", "Agenzia", "Agencia", "Belastingdienst", "Rahandus", "Maksu")


def _month_end(y, m):
    return date(y, m, calendar.monthrange(y, m)[1])


def stock_value(token, as_of):
    """Landed stock value (USD) on as_of: a saved valuation, else worked back now."""
    from StockValuation import compute, save
    recs = pb_list_all(token, "stock_valuations", f'as_of = "{as_of}"', fields="as_of,method,totals")
    if recs:
        rec = sorted(recs, key=lambda r: r["method"] != "snapshot")[0]
        return rec["totals"]["landedUsd"], rec["method"]
    snap = compute(as_of, "backcalc", with_ledger=False, token=token)
    save(snap, token)
    return snap["totals"]["landedUsd"], "backcalc"


def model_side(token, start, end):
    rk = product_ranking(start, end, atomic=ALL_MPS, _token=token)["byAsin"].values()
    t = defaultdict(float)
    for r in rk:
        for k in ("units", "sales", "netSales", "fees", "productCost", "ads", "sbAds", "returns", "storage", "awd"):
            t[k] += r.get(k) or 0
        if r.get("profit") is None:
            t["noCostUnits"] += r.get("units") or 0
    # split product cost into purchase vs duty + freight
    mapping = {r["asin"]: r for r in pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,ean")}
    flt = " || ".join(f'marketplace = "{m}"' for m in ALL_MPS)
    cache = {}
    for ln in pb_list_all(token, "order_items", f'purchase_date >= "{start}" && purchase_date <= "{end}" && ({flt}) '
                                                f'&& order_status != "Cancelled"', fields="asin,qty"):
        ean = (mapping.get(ln.get("asin")) or {}).get("ean")
        if not ean or not ln.get("qty"):
            continue
        if ean not in cache:
            cache[ean] = landed_cost(token, ean)
        if cache[ean].get("purchase"):
            t["purchase"] += cache[ean]["purchase"] * ln["qty"]
    t["importCost"] = t["productCost"] - t["purchase"]
    t["vat"] = t["sales"] - t["netSales"]
    t["profit"] = (t["netSales"] - t["fees"] - t["productCost"] - t["ads"] - t["sbAds"] - t["returns"]
                   - t["storage"] - t["awd"])
    return dict(t)


def _months(start, end):
    y, m = int(start[:4]), int(start[5:7])
    while (y, m) <= (int(end[:4]), int(end[5:7])):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def real_side(token, start, end):
    t = defaultdict(float)
    other = defaultdict(float)
    months = list(_months(start, end))
    mflt = " || ".join(f"(year = {y} && month = {m})" for y, m in months)
    for e in pb_list_all(token, "amazon_expense_events", f"({mflt})", fields="category,amount,marketplace,currency"):
        cat = e["category"]
        if cat in SKIP_CATS:
            continue
        v = usd(e.get("amount") or 0, e.get("currency") or CUR_OF_MP.get(e.get("marketplace"), "EUR"))
        if cat in SALES_CATS:
            t["sales"] += v
        elif cat == "Refunds":
            t["refunds"] += v
        elif cat in FEE_CATS:
            t["fees"] += v
        elif cat in STORAGE_CATS:
            t["storage"] += v
        elif cat == "FBA Returns Processing Fee":
            t["returnsFee"] += v
        elif "GlobalInboundTransportation" in cat:
            t["importAmazon"] += v
        else:
            t["otherFees"] += v
            other[cat] += v
    for r in pb_list_all(token, "country_sales", f"({mflt})", fields="marketplace,ppc_spend"):
        if r["marketplace"] != "eu" and r.get("ppc_spend"):
            t["ads"] -= usd(r["ppc_spend"], CUR_OF_MP.get(r["marketplace"], "EUR"))
    # Etsy payouts (in) minus Etsy card charges (out) on Wise
    for r in pb_list_all(token, "wise_ledger", f'counterparty ~ "etsy" && occurred_at >= "{start} 00:00:00" '
                                               f'&& occurred_at <= "{end} 23:59:59"',
                         fields="direction,amount,currency"):
        t["etsy"] += usd(r.get("amount") or 0, r.get("currency") or "USD") * (1 if r["direction"] == "in" else -1)
    # invoices on /wise
    for d in pb_list_all(token, "wise_documents", 'kind = "invoice"', fields="group_key,extracted,month"):
        ex = d.get("extracted") or {}
        day = ex.get("booked_date") or ex.get("invoice_date") or ""
        if not (start <= day <= end):
            continue
        g = (d.get("group_key") or "").split("|")
        vendor = ex.get("vendor") or ex.get("supplier") or ""
        cur = (ex.get("currency") or "EUR").upper()
        try:
            total = float(ex.get("total") or 0)
            vat = float(ex.get("tax_amount") or 0) if (ex.get("tax_type") or "").upper() == "VAT" else 0.0
        except (TypeError, ValueError):
            continue
        if g[0] in ("amazon", "etsy"):
            continue
        if (g[0] == "supplier" and len(g) > 1 and g[1] == "owner") or "Tamir" in vendor:
            t["owner"] -= usd(total, cur)
        elif g[0] == "supplier":
            t["purchases"] -= usd(total, cur)
        elif g[0] == "supplier-separate" or "nicargo" in vendor:
            t["importInvoices"] -= usd(total - vat, cur)
        elif "agerpark" in vendor:
            t["lagerpark"] -= usd(total - vat, cur)
        elif any(o in vendor for o in VAT_OFFICES) or (vat and abs(vat) >= abs(total) * 0.99):
            t["vatPaid"] -= usd(total, cur)
        else:
            t["overheads"] -= usd(total, cur)
    s_start = (date.fromisoformat(start) - timedelta(days=1)).isoformat()
    v0, m0 = stock_value(token, s_start)
    v1, m1 = stock_value(token, end)
    t["stockChange"] = v1 - v0
    t["profit"] = (t["sales"] + t["refunds"] + t["fees"] + t["storage"] + t["returnsFee"] + t["importAmazon"]
                   + t["otherFees"] + t["ads"] + t["etsy"] + t["purchases"] + t["importInvoices"] + t["lagerpark"]
                   + t["vatPaid"] + t["overheads"] + t["stockChange"])
    t["stockMethods"] = f"{s_start}: {m0}, {end}: {m1}"
    return t, dict(other)


def compare(start, end, token=None):
    token = token or pb_authenticate()
    mo = model_side(token, start, end)
    re_, other = real_side(token, start, end)
    rows = [  # label, model (cost negative), real, note
        ("Sales after VAT", mo["netSales"], re_["sales"] + re_["vatPaid"],
         "model: order dates, VAT taken out of EU/UK prices; real: settlement periods - VAT returns paid "
         "(UK VAT Amazon collects itself is already outside the settlement sales)"),
        ("Refunds / returns", -mo["returns"], re_["refunds"] + re_["returnsFee"],
         "model: lost value of returns (unit back in stock counted); real: money refunded + processing fee"),
        ("Referral + FBA fees", -mo["fees"], re_["fees"], "model: today's fee estimates"),
        ("Other Amazon fees", 0.0, re_["otherFees"], "not in the model - see the list below"),
        ("Storage + AWD + Lagerpark", -(mo["storage"] + mo["awd"]), re_["storage"] + re_["lagerpark"], ""),
        ("Ads", -(mo["ads"] + mo["sbAds"]), re_["ads"], "real: all ad spend Amazon billed"),
        ("Product cost (purchase)", -mo["purchase"], re_["purchases"] + re_["stockChange"],
         "real: supplier invoices + stock change (stock valued with duty + freight)"),
        ("Import duty + freight", -mo["importCost"], re_["importAmazon"] + re_["importInvoices"],
         "real: Amazon AGL + Unicargo + Xhipment (VAT excluded)"),
        ("Etsy (net payouts)", 0.0, re_["etsy"], "Etsy is not in the model"),
        ("Overheads", 0.0, re_["overheads"], "accounting, Taxually, EPR, software, travel..."),
    ]
    out = [{"label": l, "model": round(a, 0), "real": round(b, 0), "diff": round(b - a, 0), "note": n}
           for l, a, b, n in rows]
    return {"start": start, "end": end, "rows": out, "modelProfit": round(mo["profit"]),
            "realProfit": round(re_["profit"]), "owner": round(re_["owner"]),
            "otherFees": {k: round(v) for k, v in sorted(other.items(), key=lambda x: x[1])},
            "stockMethods": re_["stockMethods"], "modelUnits": round(mo["units"]),
            "noCostUnits": round(mo.get("noCostUnits", 0))}


def text_of(c, title):
    lines = [f"{title} ({c['start']} → {c['end']}, USD)",
             f"Model profit ${c['modelProfit']:,} vs real ${c['realProfit']:,} → difference ${c['realProfit'] - c['modelProfit']:,}"]
    for r in sorted(c["rows"], key=lambda r: r["diff"]):
        mark = "🔴 " if r["diff"] <= -2000 else ""
        lines.append(f"{mark}{r['label']}: model {r['model']:,.0f} · real {r['real']:,.0f} · diff {r['diff']:,.0f}")
    worst = list(c["otherFees"].items())[:8]
    if worst:
        lines.append("Other Amazon fees: " + ", ".join(f"{k} {v:,}" for k, v in worst))
    lines.append(f"Your consulting receipts (not counted above): ${-c['owner']:,}")
    lines.append(f"Stock: {c['stockMethods']}")
    return "\n".join(lines)


def SendModelVsReal(request):
    from MonthlyChecks import last_month
    from WeeklyChecks import _json, _send
    from NotificationRouting import notify
    args = getattr(request, "args", {}) or {}
    try:
        y, m = (int(args["year"]), int(args["month"])) if args.get("year") else last_month()
        token = pb_authenticate()
        end = _month_end(y, m).isoformat()
        month = compare(f"{y}-{m:02d}-01", end, token)
        ytd = compare(f"{y}-01-01", end, token)
        text = "🔍 Model vs real profit\n\n" + text_of(month, f"{y}-{m:02d}") + "\n\n" + text_of(ytd, f"Year to date {y}")
        tables = [{"title": f"{title} – model vs real (USD)", "columns": ["Item", "Model", "Real", "Difference", "Note"],
                   "rows": [[r["label"], r["model"], r["real"], r["diff"], r["note"]] for r in c["rows"]]
                   + [["Profit", c["modelProfit"], c["realProfit"], c["realProfit"] - c["modelProfit"], ""]]}
                  for title, c in ((f"{y}-{m:02d}", month), (f"Year to date {y}", ytd))]
        res = _send("model-vs-real", text, args)
        if str(args.get("dry")) != "1":
            from CheckReports import MONTHLY_JOBS, save_report
            save_report("monthly", f"{y}-{m:02d}", "model-vs-real", MONTHLY_JOBS["model-vs-real"], text, token=token,
                        tables=tables)
        return res
    except Exception as exc:
        notify("model-vs-real", "amzbot", f"🚨 Model vs real check failed: {exc}", is_error=True)
        return (json.dumps({"error": str(exc)}), 500, {"Content-Type": "application/json"})
