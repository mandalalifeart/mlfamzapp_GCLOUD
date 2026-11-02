"""Monthly checks added 2026-10-25 (per the user: "build it all" from
plans/recurring-checks.md). Report-only, @baba_update_bot, each takes dry=1.
Cron: the 3rd of each month (finished month + settlements mostly in).

  SendMonthlyProfit          - per marketplace: sales, refunds, Amazon fees, ads, net payout (no COGS)
  SendReimbursementCheck     - FBA lost/damaged units not reimbursed + refunds without a return
  SendStorageCheck           - FBA aged inventory / storage fee risk (FBA only - AWD pareo stock is fine, per the user)
  SendAccountingCheck        - Wise statement + missing invoices, VAT files/deadlines, supplier balances
  SendCompetitorPriceCheck   - our prices vs other sellers on our main searches per marketplace
  SendContentGapCheck        - products with stock missing A+ / video / enough images per marketplace
"""
import csv
import io
import json
import os
import re
import statistics
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

import requests
from sp_api.api import AplusContent, CatalogItems, Finances, Products
from sp_api.base import Marketplaces

from AsinAnalytics import pb_authenticate, pb_list_all
from NotificationRouting import notify
from WeeklyChecks import _json, _send, creds, listings, run_report, sym

USD_FALLBACK = {"EUR": 1.12, "GBP": 1.30, "CAD": 0.72, "MXN": 0.054, "PLN": 0.26, "SEK": 0.10, "AED": 0.2723, "SAR": 0.2667}


def usd_rates():
    rates = dict(USD_FALLBACK)
    try:
        r = requests.get("https://api.frankfurter.dev/v1/latest?from=USD&to=EUR,GBP,CAD,MXN,PLN,SEK", timeout=15).json()["rates"]
        rates.update({k: 1 / v for k, v in r.items()})
    except Exception:
        pass
    return rates


def last_month(today=None):
    today = today or date.today()
    first = today.replace(day=1)
    lm = first - timedelta(days=1)
    return lm.year, lm.month


def _ym_minus(y, m, k):
    m -= k
    while m <= 0:
        m += 12
        y -= 1
    return y, m


# --------------------------------------------------------------- monthly profit
REGIONS = {  # report row -> (amazon_expense_events marketplaces, currency)
    "USA": (("usa",), "USD"), "Canada": (("ca",), "CAD"), "Mexico": (("mex",), "MXN"), "UK": (("uk",), "GBP"),
    "EU (EUR)": (("de", "fr", "it", "es", "nl", "be", "ie", "eu"), "EUR"), "Poland": (("pl",), "PLN"),
    "Sweden": (("se",), "SEK"), "UAE": (("aed",), "AED"),
}
SALES_CATS = ("Sales Revenue", "Shipping Charged to Buyer", "FBA Liquidation Revenue")
SKIP_CATS = ("Tax Collected",)  # pass-through VAT/sales tax


def profit_block(rows):
    t = defaultdict(float)
    for r in rows:
        cat, amt = r["category"], r.get("amount") or 0
        if cat in SKIP_CATS:
            continue
        if cat in SALES_CATS:
            t["sales"] += amt
        elif cat == "Refunds":
            t["refunds"] += amt
        elif cat == "Advertising":
            t["ads"] += amt
        elif cat.startswith("Other: "):
            t["other"] += amt
        else:
            t["fees"] += amt
    t["net"] = t["sales"] + t["refunds"] + t["fees"] + t["ads"] + t["other"]
    return t


def monthly_profit_text(pb, y, m):
    periods = [(y, m), _ym_minus(y, m, 1), (y - 1, m)]
    data = {}
    for (yy, mm) in periods:
        rows = pb_list_all(pb, "amazon_expense_events", f"year = {yy} && month = {mm}")
        for name, (mps, cur) in REGIONS.items():
            data[(name, yy, mm)] = profit_block([r for r in rows if r["marketplace"] in mps])
    # Ads charged to a card (not taken from the payout) are missing above -
    # take ad spend from country_sales.ppc_spend instead, per country.
    ads_rows = pb_list_all(pb, "country_sales", f"year = {y} && month = {m}")
    ads_by = defaultdict(float)
    mp_region = {"usa": "USA", "ca": "Canada", "mex": "Mexico", "uk": "UK", "pl": "Poland", "se": "Sweden",
                 "de": "EU (EUR)", "fr": "EU (EUR)", "it": "EU (EUR)", "es": "EU (EUR)", "nl": "EU (EUR)", "be": "EU (EUR)", "ie": "EU (EUR)"}
    for r in ads_rows:
        if r["marketplace"] in mp_region:
            ads_by[mp_region[r["marketplace"]]] += r.get("ppc_spend") or 0
    rates = usd_rates()
    out = [f"💰 Profit per marketplace – {y}-{m:02d} (before product cost; Amazon payouts by settlement month, VAT/sales tax excluded)"]
    tot = defaultdict(float)
    for name, (mps, cur) in REGIONS.items():
        t = data[(name, y, m)]
        if not t["sales"]:
            continue
        ads = -ads_by[name] if ads_by[name] else t["ads"]
        net = t["sales"] + t["refunds"] + t["fees"] + ads + t["other"]
        prev = data[(name,) + periods[1]]
        ly = data[(name,) + periods[2]]
        rate = 1 if cur == "USD" else rates.get(cur, 0)
        for k, v in (("sales", t["sales"]), ("net", net), ("ads", ads), ("fees", t["fees"]), ("refunds", t["refunds"])):
            tot[k] += v * rate
        margin = 100 * net / t["sales"] if t["sales"] else 0
        out.append(f"\n{name} ({cur})\n"
                   f"  sales {t['sales']:,.0f} · refunds {t['refunds']:,.0f} · Amazon fees {t['fees']:,.0f} · ads {ads:,.0f}"
                   f"{f' · other {t['other']:,.0f}' if abs(t['other']) >= 1 else ''}\n"
                   f"  = net {net:,.0f} ({margin:.0f}% of sales)"
                   f" | prev month sales {prev['sales']:,.0f} · last year {ly['sales']:,.0f}")
    if tot["sales"]:
        out.append(f"\nALL in USD: sales ${tot['sales']:,.0f} · refunds ${tot['refunds']:,.0f} · fees ${tot['fees']:,.0f}"
                   f" · ads ${tot['ads']:,.0f} → net ${tot['net']:,.0f} ({100 * tot['net'] / tot['sales']:.0f}%)")
    out.append("Product cost (Malani/Acacia) and shipping to Amazon are not included yet.")
    return "\n".join(out)


def SendMonthlyProfit(request):
    args = getattr(request, "args", {}) or {}
    try:
        y, m = (int(args["year"]), int(args["month"])) if args.get("year") else last_month()
        return _send("monthly-profit", monthly_profit_text(pb_authenticate(), y, m), args)
    except Exception as exc:
        notify("monthly-profit", "amzbot", f"🚨 Monthly profit report failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


# --------------------------------------------------------------- reimbursements
LOST_CODES = ("M", "F", "O", "5")
DAMAGED_CODES = ("E", "6", "7", "H", "K", "U")


def _tsv(text):
    return list(csv.DictReader(io.StringIO(text or ""), delimiter="\t"))


def refunds_without_return(mp, days_from=120, days_to=45):
    """Refunded orders posted 45-120 days ago with no return received
    (amazon_returns) - Amazon should reimburse these after 60 days."""
    cred, _ = creds(mp)
    fin = Finances(credentials=cred, marketplace=getattr(Marketplaces, mp))
    now = datetime.now(timezone.utc)
    kw = {"PostedAfter": (now - timedelta(days=days_from)).strftime("%Y-%m-%dT%H:%M:%SZ"),
          "PostedBefore": (now - timedelta(days=days_to)).strftime("%Y-%m-%dT%H:%M:%SZ"), "MaxResultsPerPage": 100}
    refunds = []
    while True:
        for attempt in range(5):
            try:
                resp = fin.list_financial_events(**kw)
                break
            except Exception as exc:
                if attempt == 4:
                    raise
                time.sleep(5 * (attempt + 1))
        ev = resp.payload.get("FinancialEvents", {})
        for e in ev.get("RefundEventList", []):
            mname = e.get("MarketplaceName", "")
            for it in e.get("ShipmentItemAdjustmentList", []):
                principal = sum(c["ChargeAmount"]["CurrencyAmount"] for c in it.get("ItemChargeAdjustmentList", [])
                                if c.get("ChargeType") == "Principal")
                refunds.append({"order": e.get("AmazonOrderId"), "sku": it.get("SellerSKU"), "qty": it.get("QuantityShipped") or 1,
                                "date": e.get("PostedDate", "")[:10], "amount": -principal, "marketplace": mname})
        token = resp.payload.get("NextToken") or resp.next_token
        if not token:
            break
        kw = {"NextToken": token}
        time.sleep(2.1)
    return refunds


def reimbursement_text(pb):
    end = datetime.now(timezone.utc) - timedelta(days=3)
    start = end - timedelta(days=540)
    fmt = lambda d: d.strftime("%Y-%m-%dT00:00:00Z")  # noqa: E731
    out = ["🧾 FBA reimbursement check (lost/damaged in Amazon warehouses, refunds without a return)"]
    returned = {r["order_id"] for r in pb_list_all(pb, "amazon_returns", 'order_id != ""', fields="order_id")}
    total_claims = 0
    for mp in ("US", "DE"):
        reimb = _tsv(run_report(mp, "GET_FBA_REIMBURSEMENTS_DATA", start=fmt(start), end=fmt(end), wait_minutes=25))
        ledger = _tsv(run_report(mp, "GET_LEDGER_DETAIL_VIEW_DATA", start=fmt(start), end=fmt(end), wait_minutes=40))
        r_lost, r_dmg, r_orders, unit_val = Counter(), Counter(), set(), {}
        for r in reimb:
            q = int(r.get("quantity-reimbursed-total") or 0)
            reason = r.get("reason", "")
            if reason.startswith("Lost_"):
                r_lost[r["fnsku"]] += q
            elif reason.startswith("Damaged_"):
                r_dmg[r["fnsku"]] += q
            if r.get("amazon-order-id"):
                r_orders.add(r["amazon-order-id"])
            try:
                unit_val[r["fnsku"]] = abs(float(r.get("amount-per-unit") or 0)) or unit_val.get(r["fnsku"], 0)
            except ValueError:
                pass
        lost, dmg, last_evt, sku_of = Counter(), Counter(), {}, {}
        recent = (date.today() - timedelta(days=60)).strftime("%m/%d/%Y")
        for r in ledger:
            if r.get("Event Type") != "Adjustments" or (r.get("MSKU") or "").startswith("amzn.gr."):
                continue
            f = r["FNSKU"]
            q = int(r.get("Quantity") or 0)
            sku_of[f] = r.get("MSKU")
            if r.get("Reason") in LOST_CODES:
                lost[f] += q
            elif r.get("Reason") in DAMAGED_CODES:
                dmg[f] += q
            if r.get("Reason") in ("M",) + DAMAGED_CODES:
                d = datetime.strptime(r["Date"], "%m/%d/%Y").date()
                last_evt[f] = max(last_evt.get(f, d), d)
        cut_old, cut_new = date.today() - timedelta(days=60), date.today() - timedelta(days=21)
        claims = []
        for f in set(lost) | set(dmg):
            l_open = -lost[f] - r_lost[f]
            d_open = -dmg[f] - r_dmg[f]
            when = last_evt.get(f)
            if (l_open > 0 or d_open > 0) and when and cut_old <= when <= cut_new:
                claims.append((sku_of.get(f) or f, f, max(l_open, 0), max(d_open, 0), when, unit_val.get(f)))
        out.append(f"\n{mp}: {len(claims)} warehouse lost/damaged case(s) to check (last event 21-60 days ago, net units not reimbursed)")
        for sku, f, l, d, when, val in sorted(claims, key=lambda c: -(c[2] + c[3]))[:15]:
            out.append(f"   {sku} ({f}): {l} lost, {d} damaged, last {when}{f', ~{sym(mp)}{val:.2f}/unit' if val else ''}")
        total_claims += len(claims)
        try:
            refunds = refunds_without_return("US" if mp == "US" else "DE")
        except Exception as exc:
            out.append(f"   (refund check failed: {str(exc)[:80]})")
            continue
        missing = [r for r in refunds if r["order"] not in returned and r["order"] not in r_orders and r["amount"] > 0]
        out.append(f"{mp}: {len(missing)} refund(s) 45-120 days old with no return and no reimbursement")
        for r in sorted(missing, key=lambda r: -r["amount"])[:15]:
            out.append(f"   {r['date']} {r['order']} {r['sku']} – refunded {r['amount']:.2f} ({r['marketplace']})")
        total_claims += len(missing)
    out.append("\nOpen a case in Seller Central → Help → Get support → FBA → Lost/damaged inventory or Customer return not received."
               " Amazon pays most of these automatically – check the dates first.")
    out[0] += f" – {total_claims} to check" if total_claims else " – nothing open"
    return "\n".join(out)


def SendReimbursementCheck(request):
    args = getattr(request, "args", {}) or {}
    try:
        return _send("reimbursement-check", reimbursement_text(pb_authenticate()), args)
    except Exception as exc:
        notify("reimbursement-check", "amzbot", f"🚨 Reimbursement check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


# --------------------------------------------------------------- storage (FBA only)
def _f(row, key):
    try:
        return float(row.get(key) or 0)
    except ValueError:
        return 0.0


def storage_text(pb):
    out = ["📦 FBA storage check (aged stock and storage fees - FBA only; pareo stock in AWD is fine)"]
    for mp in ("US", "DE", "UK"):
        rows = _tsv(run_report(mp, "GET_FBA_INVENTORY_PLANNING_DATA", wait_minutes=25))
        if mp != "US":
            rows = [r for r in rows if (r.get("marketplace") or "").upper() in (mp, "GB" if mp == "UK" else mp)] or rows
        s = sym(mp)
        storage = sum(_f(r, "estimated-storage-cost-next-month") for r in rows)
        ais_keys = [k for k in (rows[0].keys() if rows else []) if k.startswith("estimated-ais-")]
        aged = []
        for r in rows:
            old = sum(_f(r, k) for k in ("inv-age-181-to-270-days", "inv-age-271-to-365-days", "inv-age-366-to-455-days", "inv-age-456-plus-days"))
            ais = sum(_f(r, k) for k in ais_keys)
            if old >= 5 or ais > 0:
                aged.append((r.get("sku"), int(old), ais, int(_f(r, "available")), int(_f(r, "units-shipped-t90")),
                             r.get("recommended-action") or ""))
        out.append(f"\n{mp}: estimated storage next month {s}{storage:,.2f} · {len(rows)} SKUs")
        if aged:
            out.append(f"🟠 {len(aged)} SKU(s) with stock older than 180 days or an aged-inventory surcharge:")
            for sku, old, ais, avail, t90, act in sorted(aged, key=lambda a: (-a[2], -a[1]))[:12]:
                out.append(f"   {sku}: {old} units >180d of {avail}, sold {t90} in 90d"
                           f"{f', surcharge ~{s}{ais:.2f}' if ais else ''}{f' – Amazon: {act}' if act else ''}")
        else:
            out.append("✅ nothing older than 180 days")
    return "\n".join(out)


def SendStorageCheck(request):
    args = getattr(request, "args", {}) or {}
    try:
        return _send("storage-check", storage_text(pb_authenticate()), args)
    except Exception as exc:
        notify("storage-check", "amzbot", f"🚨 Storage check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


# --------------------------------------------------------------- accounting
# Filing rhythms per the user (2026-10-25): DE, FR, EE monthly; NL, UK
# quarterly; IT, ES, PL, CZ, IE are filed by Taxually - not tracked here.
# DE UStVA is due the 10th (later if a Dauerfristverlaengerung is set), FR
# CA3 for non-established companies around the 19th.
VAT_CALENDAR = [
    ("DE UStVA (Germany, monthly)", "monthly", 10),
    ("FR CA3 (France, monthly)", "monthly", 19),
    ("EE KMD (Estonia, monthly)", "monthly", 20),
    ("OSS return (via Estonia, quarterly)", "quarterly_end", None),
    ("NL VAT (quarterly)", "quarterly_end", None),
    ("UK VAT (quarters Feb-Apr, May-Jul, Aug-Oct, Nov-Jan)", "uk", None),
]


def vat_due_this_month(today):
    y, m = today.year, today.month
    due = []
    for name, kind, day in VAT_CALENDAR:
        if kind == "monthly":
            due.append(f"{name}: due {y}-{m:02d}-{day:02d} (for {_ym_minus(y, m, 1)[0]}-{_ym_minus(y, m, 1)[1]:02d})")
        elif kind == "quarterly_end" and m in (1, 4, 7, 10):
            last = (date(y, m % 12 + 1, 1) if m < 12 else date(y + 1, 1, 1)) - timedelta(days=1)
            due.append(f"{name}: due {last} (for the quarter that just ended)")
        elif kind == "uk" and m in (6, 9, 12, 3):
            due.append(f"{name}: due {y}-{m:02d}-07 (for the quarter that ended 5 weeks ago)")
    return due


def accounting_text(pb, today=None):
    import main
    from run_local import FakeRequest
    today = today or date.today()
    y, m = last_month(today)
    out = [f"🧮 Accounting check – {y}-{m:02d}"]
    body = json.loads(main.GetWiseLedger(FakeRequest({"year": str(y), "month": str(m)}))[0])
    rows = body.get("rows") or []
    stmts = body.get("statements") or []
    conf = body.get("confirmation") or {}
    curs = sorted({r["currency"] for r in rows})
    unconfirmed = [c for c in curs if m not in ((conf.get(c) or {}).get("confirmedMonths") or [])]
    out.append(f"{'✅' if stmts and not unconfirmed else '🔴'} Wise statement for {y}-{m:02d}: "
               f"{'uploaded' if stmts else 'NOT uploaded – download it from Wise and send it to me'}"
               f"{f' (not confirmed: {', '.join(unconfirmed)})' if stmts and unconfirmed else ''}")
    missing = [r for r in rows if r.get("invoiceRequired") and not r.get("invoices") and not r.get("noDocument")
               and not r.get("invoiceGroup")]
    if missing:
        out.append(f"🔴 {len(missing)} payment(s) without an invoice:")
        for r in sorted(missing, key=lambda r: -abs(r.get("amount") or 0))[:15]:
            out.append(f"   {r['date'][:10]} {r['counterparty'] or r['description'][:30]} {r['amount']:.2f} {r['currency']} ({r['classification']})")
    else:
        out.append("✅ every payment has its invoice")
    groups = {r["invoiceGroup"] for r in rows if r.get("invoiceGroup")}
    month_key = f"{y}-{m:02d}"
    have = {g["groupKey"] for g in body.get("groupDocs") or [] if g.get("month") == month_key}
    need = sorted(g for g in groups if g not in have)
    out.append(f"{'🔴' if need else '✅'} monthly sales invoices (Amazon/Etsy per currency): "
               + (f"missing {', '.join(need)}" if need else "all there"))
    # VAT (file, departure check, return calendar) is left out - the user runs it by hand (2026-11-02).
    sup = json.loads(main.GetWiseSupplierBalances(FakeRequest({}))[0]).get("suppliers") or []
    lines = []
    for s in sup:
        if s["id"] not in ("malani", "acacia"):
            continue
        for yr in s.get("years") or []:
            if yr["year"] == str(today.year):
                bal = yr["closing"]
                lines.append(f"   {s['name']}: {bal:+,.2f} {yr['currency']} ({'paid in advance' if bal >= 0 else 'we owe'}) – "
                             f"{today.year}: paid {yr['paid']:,.0f}, invoiced {yr['invoiced']:,.0f}")
    if lines:
        out.append("🏭 Supplier balances:\n" + "\n".join(lines))
    return "\n".join(out)


def SendAccountingCheck(request):
    args = getattr(request, "args", {}) or {}
    try:
        return _send("accounting-check", accounting_text(pb_authenticate()), args)
    except Exception as exc:
        notify("accounting-check", "amzbot", f"🚨 Accounting check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


# --------------------------------------------------------------- competitor prices
FALLBACK_QUERIES = {
    "US": ["pareo", "sarong", "pouf cover", "moroccan pouf", "floor cushion cover"],
    "UK": ["sarong", "pareo", "pouffe cover", "moroccan pouffe", "floor cushion cover"],
    "DE": ["pareo", "sarong tuch", "pouf bezug", "bodenkissen", "marokkanischer pouf"],
    "FR": ["paréo", "pareo femme", "housse pouf", "pouf marocain", "coussin de sol"],
    "IT": ["pareo", "pareo donna", "copri pouf", "pouf marocchino", "cuscino da pavimento"],
    "ES": ["pareo", "pareo playa", "funda puf", "puf marroquí", "cojín suelo"],
    "AE": ["pareo", "sarong", "pouf cover", "moroccan pouf"],
    "SA": ["pareo", "sarong", "pouf cover", "moroccan pouf"],
}
PAREO_WORDS = ("pareo", "paréo", "sarong", "tuch", "playa", "beach", "plage", "mare")
OUR_BRANDS = ("mandala life art", "boho paradise")
SQP_MP = {"US": "us", "UK": "uk", "DE": "de", "FR": "fr", "IT": "it", "ES": "es"}


def top_queries(pb, mp, n=4):
    """[(query, "pareo"|"home")]: our best searches from Brand Analytics
    (last ~4 months, by our purchases then clicks), half pareo / half home
    decor; the category comes from which of OUR products got the clicks."""
    code = SQP_MP.get(mp)
    if not code:
        return [(q, "pareo" if any(w in q for w in PAREO_WORDS) else "home") for q in FALLBACK_QUERIES.get(mp, [])[:n]]
    group = {r["asin"]: r["group"] for r in pb_list_all(pb, "asin_group_mapping", 'asin != ""', fields="asin,group")}
    since = (date.today() - timedelta(days=120)).isoformat()
    score, cat = Counter(), defaultdict(Counter)
    for r in pb_list_all(pb, "asin_search_queries", f'marketplace = "{code}" && period = "MONTH" && start_date >= "{since}"',
                         fields="search_query,asin,asin_purchases,asin_clicks,asin_impressions"):
        q = r["search_query"]
        score[q] += (r.get("asin_purchases") or 0) * 10 + (r.get("asin_clicks") or 0)
        kind = "pareo" if group.get(r["asin"], "").startswith("PAREO") else "home"
        cat[q][kind] += (r.get("asin_clicks") or 0) + (r.get("asin_impressions") or 0) / 100
    best = [q for q, _ in score.most_common(40) if len(q) <= 40]
    kind_of = {q: (cat[q].most_common(1)[0][0] if cat[q] else "home") for q in best}
    pareo = [q for q in best if kind_of[q] == "pareo"][: n // 2]
    home = [q for q in best if kind_of[q] == "home"][: n - len(pareo)]
    out = [(q, kind_of[q]) for q in pareo + home]
    return out or [(q, "pareo" if any(w in q for w in PAREO_WORDS) else "home") for q in FALLBACK_QUERIES.get(mp, [])[:n]]


def competitor_text(pb):
    hist = defaultdict(dict)
    for r in pb_list_all(pb, "price_history", 'since != ""', fields="marketplace,sku,price,since"):
        cur = hist[r["marketplace"]].get(r["sku"])
        if not cur or r["since"] > cur["since"]:
            hist[r["marketplace"]][r["sku"]] = r
    out = ["🏁 Competitor prices on our main searches (other sellers' top 20 vs our prices)"]
    for mp in ("US", "UK", "DE", "FR", "IT", "ES", "AE", "SA"):
        cred, _ = creds(mp)
        m = getattr(Marketplaces, mp)
        ci = CatalogItems(credentials=cred, marketplace=m, version="2022-04-01")
        pr = Products(credentials=cred, marketplace=m)
        ours = hist.get(mp, {})
        ours_p = [v["price"] for k, v in ours.items() if "PAREO" in k.upper() or "SARONG" in k.upper()]
        ours_h = [v["price"] for k, v in ours.items() if k.upper().startswith(("COVER", "STOOL", "STUFFED"))]
        s = sym(mp)
        out.append(f"\n{mp} – our median: pareo {s}{statistics.median(ours_p):.2f}" if ours_p else f"\n{mp}")
        if ours_h:
            out[-1] += f", pouf covers {s}{statistics.median(ours_h):.2f}"
        for q, kind in top_queries(pb, mp):
            try:
                items = ci.search_catalog_items(keywords=[q], marketplaceIds=[m.marketplace_id],
                                                includedData=["summaries"], pageSize=20).payload.get("items", [])
            except Exception as exc:
                out.append(f"   {q}: search failed ({str(exc)[:60]})")
                continue
            brand = {i["asin"]: ((i.get("summaries") or [{}])[0].get("brand") or "").lower() for i in items}
            prices = {}
            asins = list(brand)
            for k in range(0, len(asins), 20):
                try:
                    for c in pr.get_competitive_pricing_for_asins(asins[k:k + 20]).payload:
                        lp = [x["Price"]["LandedPrice"]["Amount"] for x in
                              ((c.get("Product") or {}).get("CompetitivePricing") or {}).get("CompetitivePrices", [])
                              if (x.get("Price") or {}).get("LandedPrice")]
                        if lp:
                            prices[c["ASIN"]] = min(lp)
                except Exception:
                    pass
                time.sleep(1.1)
            theirs = [p for a, p in prices.items() if not brand.get(a, "").startswith(OUR_BRANDS)]
            mine = sum(1 for a in brand if brand[a].startswith(OUR_BRANDS))
            if not theirs:
                continue
            ref = ours_p if kind == "pareo" else ours_h
            ref_med = statistics.median(ref) if ref else None
            med = statistics.median(theirs)
            flag = ""
            if ref_med:
                diff = 100 * (ref_med / med - 1)
                flag = f" → ours {diff:+.0f}%" + (" 🔴" if diff > 60 else (" 🟢" if diff < -10 else ""))
            out.append(f"   '{q}': others median {s}{med:.2f} (range {min(theirs):.2f}–{max(theirs):.2f}),"
                       f" we have {mine} of top {len(items)}{flag}")
            time.sleep(1)
    out.append("\n🔴 = our median more than 60% above the market median for that search")
    return "\n".join(out)


def SendCompetitorPriceCheck(request):
    args = getattr(request, "args", {}) or {}
    try:
        return _send("competitor-price-check", competitor_text(pb_authenticate()), args)
    except Exception as exc:
        notify("competitor-price-check", "amzbot", f"🚨 Competitor price check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)


# --------------------------------------------------------------- content gaps
STOCK_BY_MP = {"US": "usa_balance_fba", "UK": "uk_balance_fba", "DE": "de_balance_fba", "FR": "de_balance_fba",
               "IT": "de_balance_fba", "ES": "de_balance_fba"}
UPLOADS = os.path.expanduser("~/videos/amazon/UPLOADS.md")


def video_asins():
    """{marketplace: set(asins)} from ~/videos/amazon/UPLOADS.md (published or
    submitted rows). Rows that only say 'N <clip> ASINs' reuse the ASINs of
    the same clip from another row."""
    by_clip, rows = {}, []
    try:
        text = open(UPLOADS, encoding="utf-8").read()
    except OSError:
        return {}
    for line in text.splitlines():
        cols = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cols) < 7 or not re.match(r"\d{4}-\d{2}-\d{2}", cols[0]):
            continue
        mp, clip, asins = cols[1], cols[2].split("_")[0], re.findall(r"B0[0-9A-Z]{8}", cols[5])
        if asins:
            by_clip.setdefault(clip, set()).update(asins)
        rows.append((mp, clip, asins))
    out = defaultdict(set)
    for mp, clip, asins in rows:
        out[mp].update(asins or by_clip.get(clip, set()))
    return out


def catalog_image_count(ci, marketplace_id, asin):
    """Images actually shown on the product page (Catalog Items API, largest
    variant per slot)."""
    for attempt in range(3):
        try:
            p = ci.get_catalog_item(asin, marketplaceIds=[marketplace_id], includedData=["images"]).payload
            imgs = next((x.get("images") or [] for x in p.get("images", []) if x.get("marketplaceId") == marketplace_id), [])
            return len({i.get("variant") for i in imgs})
        except Exception:
            time.sleep(2 * (attempt + 1))
    return None


def _aplus(ap, marketplace_id, asin):
    for attempt in range(3):
        try:
            recs = ap.search_content_publish_records(marketplaceId=marketplace_id, asin=asin).payload.get("publishRecordList", [])
            return any(r.get("contentType") == "EBC" for r in recs)
        except Exception:
            time.sleep(3 * (attempt + 1))
    return None


def content_gap_text(pb, marketplaces=("US", "UK", "DE", "FR", "IT", "ES")):
    stats = {r["sku"]: r for r in pb_list_all(pb, "sku_statistics", 'sku != ""')}
    videos = video_asins()
    out = ["🖼 Content gaps – products with stock but missing A+ / video / images (<6)"]
    for mp in marketplaces:
        lst = listings(mp, "summaries")
        cred, _ = creds(mp)
        m = getattr(Marketplaces, mp)
        ap = AplusContent(credentials=cred, marketplace=m)
        ci = CatalogItems(credentials=cred, marketplace=m, version="2022-04-01")
        field = STOCK_BY_MP[mp]
        items = [(sku, l) for sku, l in lst.items() if (stats.get(sku) or {}).get(field, 0) > 0
                 and "BUYABLE" in l["status"] and l["asin"]]
        no_aplus, no_video, few_img = [], [], []
        aplus_fail = 0
        seen_asin = set()
        for sku, l in items:
            if l["asin"] in seen_asin:
                continue
            seen_asin.add(l["asin"])
            imgs = catalog_image_count(ci, m.marketplace_id, l["asin"])
            if imgs is not None and imgs < 6:
                few_img.append(f"{sku} ({imgs})")
            if l["asin"] not in videos.get(mp, set()):
                no_video.append(sku)
            has = _aplus(ap, m.marketplace_id, l["asin"])
            if has is None:
                aplus_fail += 1
            elif not has:
                no_aplus.append(sku)
            time.sleep(0.5)
        out.append(f"\n{mp}: {len(seen_asin)} products with stock")
        if aplus_fail:
            out.append(f"   (A+ check failed for {aplus_fail} products)")
        for title, lst_ in (("no A+ content", no_aplus), ("no video (per UPLOADS.md)", no_video), ("fewer than 6 images", few_img)):
            if lst_:
                out.append(f"   {title} ({len(lst_)}): {', '.join(lst_[:20])}{' …' if len(lst_) > 20 else ''}")
    return "\n".join(out)


def SendContentGapCheck(request):
    args = getattr(request, "args", {}) or {}
    try:
        mps = tuple(args["mp"].split(",")) if args.get("mp") else ("US", "UK", "DE", "FR", "IT", "ES")
        return _send("content-gap-check", content_gap_text(pb_authenticate(), mps), args)
    except Exception as exc:
        notify("content-gap-check", "amzbot", f"🚨 Content gap check failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)
