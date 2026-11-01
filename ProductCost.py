"""Product cost + profit per unit for the product page (per the user 2026-11-01).

Formula (agreed with the user):
  purchase price = unit price on the LATEST Malani/Acacia invoice for the SKU
                   (invoice lines matched by EAN = "BUYER CODE" against
                   asin_group_mapping.ean; CIF invoices, where freight is inside
                   the price, are skipped)
  shipping       = $1.00 per unit
  import tax     = 10% x (purchase price + shipping)
  landed cost    = purchase + shipping + import tax                   (USD)

Per marketplace (US/UK/DE/FR/IT/ES), at the current price (price_history):
  net price  = price / (1 + VAT)       (US 0%, UK 20, DE 19, FR 20, IT 22, ES 21)
  Amazon fees = referral + FBA fee     (Product Fees API estimate at that price)
  ad cost    = TACOS x price, two ways:
     product TACOS    = SP+SD ad spend on this ASIN / this ASIN's sales
     department TACOS = all ad spend of the department (pareo vs home decor,
                        incl. SB) / all sales of the department
     both over the last 3 full months (ads_advertised_product_stats /
     ads_campaign_stats, profile-deduped; sales from asin_traffic_monthly)
  profit     = net price - fees - landed cost (converted) - ad cost
  margin     = profit / net price

Endpoint: GetProductCost?sku=<SKU> (or asin=) - local server, read only."""
import csv
import io
import json
import os
import re
import subprocess
import time
from collections import defaultdict
from datetime import date

import requests
from sp_api.api import ProductFees
from sp_api.base import Marketplaces

from AsinAnalytics import pb_authenticate, pb_list_all

SHIPPING_USD = 1.0
IMPORT_TAX = 0.10
MARKETS = [  # (price_history code, traffic code, ads country, VAT, currency, creds)
    ("US", "us", "US", 0.0, "USD", "USA"),
    ("UK", "uk", "UK", 0.20, "GBP", "EU"),
    ("DE", "de", "DE", 0.19, "EUR", "EU"),
    ("FR", "fr", "FR", 0.20, "EUR", "EU"),
    ("IT", "it", "IT", 0.22, "EUR", "EU"),
    ("ES", "es", "ES", 0.21, "EUR", "EU"),
]
PAREO_WORDS = ("PAREO", "SARONG", "BEACH")
HERE = os.path.dirname(os.path.abspath(__file__))
SHEET2CSV = os.path.join(HERE, "tools", "sheet2csv", "sheet2csv.mjs")
BUN = os.path.expanduser("~/.bun/bin/bun")
CACHE_FILE = os.path.join(HERE, "logs", "supplier_invoice_lines.json")
_mem = {}


def _json(body, status=200):
    return json.dumps(body, default=str), status, {"Content-Type": "application/json",
                                                   "Access-Control-Allow-Origin": "*"}


def _cached(key, hours, fn):
    hit = _mem.get(key)
    if hit and time.time() - hit[0] < hours * 3600:
        return hit[1]
    value = fn()
    _mem[key] = (time.time(), value)
    return value


# ---------- supplier invoices -> unit prices ----------

def _num(s):
    s = (s or "").replace(",", "").strip()
    return float(s) if re.fullmatch(r"\d+(\.\d+)?", s) else None


def parse_invoice(path):
    """-> (is_cif, [{ean, code, qty, price}]) from a Malani/Acacia invoice spreadsheet.
    A product line has an EAN (12-13 digits) and three numbers qty, rate, amount
    with qty x rate = amount (column positions differ per supplier)."""
    text = subprocess.run([BUN, SHEET2CSV, path], capture_output=True, text=True, timeout=60).stdout
    up = text.upper()
    is_cif = "CIF" in up and "FOB" not in up  # e.g. MI/91 DE AIR CIF
    lines = []
    for row in csv.reader(io.StringIO(text)):
        cells = [c.strip() for c in row]
        ean = next((c for c in cells if re.fullmatch(r"\d{12,13}", c)), None)
        if not ean:
            continue
        nums = [_num(c) for c in cells]
        for i in range(len(nums) - 2):
            q, p, a = nums[i:i + 3]
            if q and p and a and q == int(q) and abs(q * p - a) < 0.06 * max(a, 1):
                j = cells.index(ean) + 1
                lines.append({"ean": ean, "code": cells[j] if j < len(cells) else "", "qty": q, "price": p})
                break
    return is_cif, lines


def invoice_lines(token):
    """Every product line of every supplier invoice on /wise, cached on disk by document id."""
    docs = pb_list_all(token, "wise_documents", 'group_key = "supplier|malani" || group_key = "supplier|acacia"')
    try:
        cache = json.load(open(CACHE_FILE))
    except (OSError, ValueError):
        cache = {}
    changed = False
    out = []
    for d in docs:
        path = d.get("stored_path") or ""
        if not path.lower().endswith((".xls", ".xlsx")) or not os.path.exists(path):
            continue
        key = f"{d['id']}|{os.path.getmtime(path)}"
        if key not in cache:
            is_cif, lines = parse_invoice(path)
            cache[key] = {"cif": is_cif, "lines": lines}
            changed = True
        e = d.get("extracted") or {}
        for ln in cache[key]["lines"]:
            out.append({**ln, "invoice": e.get("invoice_number") or "", "date": e.get("invoice_date") or "",
                        "supplier": d["group_key"].split("|")[1], "cif": cache[key]["cif"]})
    if changed:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        json.dump(cache, open(CACHE_FILE, "w"))
    return out


def landed_cost(token, ean):
    ean = (ean or "").strip().lstrip("0")
    hist = sorted((ln for ln in invoice_lines(token) if ln["ean"].lstrip("0") == ean and ean),
                  key=lambda ln: (ln["date"], ln["invoice"]))
    fob = [ln for ln in hist if not ln["cif"]]
    if not fob:
        return {"history": hist}
    last = fob[-1]
    cutoff = f"{date.today().year - 1}{date.today().isoformat()[4:]}"
    recent = [ln for ln in fob if ln["date"] >= cutoff]
    qty = sum(ln["qty"] for ln in recent)
    purchase = last["price"]
    tax = round(IMPORT_TAX * (purchase + SHIPPING_USD), 2)
    return {
        "purchase": purchase, "invoice": last["invoice"], "invoiceDate": last["date"], "supplier": last["supplier"],
        "supplierCode": last["code"], "shipping": SHIPPING_USD, "importTax": tax,
        "landed": round(purchase + SHIPPING_USD + tax, 2),
        "avg12m": round(sum(ln["qty"] * ln["price"] for ln in recent) / qty, 2) if qty else None,
        "history": hist,
    }


# ---------- prices, fees, FX ----------

def current_prices(token, asin):
    latest = {}
    for r in pb_list_all(token, "price_history", f'asin = "{asin}"', fields="marketplace,price,currency,since,sku"):
        k = r["marketplace"]
        if k not in latest or r["since"] >= latest[k]["since"]:
            latest[k] = r
    return latest


def fx_to(cur):
    if cur == "USD":
        return 1.0
    def get():
        r = requests.get(f"https://api.frankfurter.dev/v1/latest?from=USD&to={cur}", timeout=15)
        r.raise_for_status()
        return r.json()["rates"][cur]
    try:
        return _cached(f"fx|{cur}", 12, get)
    except Exception:
        return {"EUR": 0.92, "GBP": 0.78, "CAD": 1.38, "MXN": 18.5, "SEK": 10.6, "PLN": 3.95}.get(cur, 1.0)


def amazon_fees(mp, cred, asin, price, cur):
    def get():
        c = dict(refresh_token=os.environ[f"REFRESH_TOKEN_{cred}"], lwa_app_id=os.environ[f"CLIENT_ID_{cred}"],
                 lwa_client_secret=os.environ[f"CLIENT_SECRET_{cred}"])
        for attempt in range(4):  # ~1 request/s quota - QuotaExceeded comes back in the payload
            r = ProductFees(credentials=c, marketplace=getattr(Marketplaces, mp)).get_product_fees_estimate_for_asin(
                asin, price=price, currency=cur, is_fba=True).payload["FeesEstimateResult"]
            if r.get("Status") == "Success" or "QuotaExceeded" not in str(r.get("Error")) or attempt == 3:
                break
            time.sleep(2 * (attempt + 1))
        if r.get("Status") != "Success":
            raise RuntimeError(str(r.get("Error"))[:200])
        fees = {f["FeeType"]: f["FinalFee"]["Amount"] for f in r["FeesEstimate"]["FeeDetailList"]}
        return {"referral": fees.get("ReferralFee", 0) + fees.get("VariableClosingFee", 0) + fees.get("PerItemFee", 0),
                "fba": fees.get("FBAFees", 0)}
    return _cached(f"fees|{mp}|{asin}|{price}", 24, get)


# ---------- TACOS ----------

def last_full_months(n=3):
    y, m = date.today().year, date.today().month
    out = []
    for _ in range(n):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append((y, m))
    return out


def _month_filter(months):
    return "(" + " || ".join(f"(year = {y} && month = {m})" for y, m in months) + ")"


def product_tacos(token, asin, months):
    best = {}
    for r in pb_list_all(token, "ads_advertised_product_stats", f'asin = "{asin}" && {_month_filter(months)}',
                         fields="campaign_id,ad_group_id,date,country_code,profile_id,spend"):
        k = (r["campaign_id"], r["ad_group_id"], r["date"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    spend = defaultdict(float)
    for r in best.values():
        spend[r["country_code"]] += r.get("spend") or 0
    sales = defaultdict(float)
    for r in pb_list_all(token, "asin_traffic_monthly", f'asin = "{asin}" && {_month_filter(months)}',
                         fields="marketplace,sales"):
        sales[r["marketplace"]] += r.get("sales") or 0
    return spend, sales


def department_tacos(token, months):
    def compute():
        groups = {m["asin"]: m["group"] for m in pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,group")}
        ports = {p["portfolio_id"]: p["name"] for p in pb_list_all(token, "ads_portfolios", 'portfolio_id != ""',
                                                                   fields="portfolio_id,name")}
        camp_port = {c["campaign_id"]: ports.get(c.get("portfolio_id"), "")
                     for c in pb_list_all(token, "ads_campaigns", 'campaign_id != ""', fields="campaign_id,portfolio_id")}
        best = {}
        for r in pb_list_all(token, "ads_campaign_stats", _month_filter(months),
                             fields="campaign_id,campaign_name,country_code,profile_id,date,spend"):
            k = (r["campaign_id"], r["date"])
            if k not in best or r["profile_id"] < best[k]["profile_id"]:
                best[k] = r
        spend = defaultdict(float)
        for r in best.values():
            text = f"{camp_port.get(r['campaign_id'], '')} {r['campaign_name']}".upper()
            seg = "pareo" if any(w in text for w in PAREO_WORDS) else "home"
            spend[(r["country_code"].upper(), seg)] += r.get("spend") or 0
        sales = defaultdict(float)
        for r in pb_list_all(token, "asin_traffic_monthly", _month_filter(months), fields="asin,marketplace,sales"):
            g = (groups.get(r["asin"]) or "").upper()
            if g and not g.startswith("IGNORE"):
                sales[(r["marketplace"], "pareo" if "PAREO" in g else "home")] += r.get("sales") or 0
        return spend, sales
    return _cached(f"dept|{months}", 6, compute)


# ---------- endpoint ----------

def product_cost(sku="", asin=""):
    token = pb_authenticate()
    mapping = pb_list_all(token, "asin_group_mapping", f"sku = {json.dumps(sku)}" if sku else f"asin = {json.dumps(asin)}")
    if not mapping:
        return {"error": f"{sku or asin} not in asin_group_mapping"}
    row = mapping[0]
    asin = row["asin"]
    dept = "pareo" if "PAREO" in (row.get("group") or "").upper() else "home"
    cost = landed_cost(token, row.get("ean"))
    months = last_full_months(3)
    p_spend, p_sales = product_tacos(token, asin, months)
    d_spend, d_sales = department_tacos(token, months)
    prices = current_prices(token, asin)
    markets = []
    for mp, tcode, acode, vat, cur, cred in MARKETS:
        pr = prices.get(mp)
        if not pr:
            continue
        price = pr["price"]
        fx = fx_to(cur)
        m = {"mp": mp, "currency": cur, "price": price, "vatRate": vat, "fx": fx,
             "netPrice": round(price / (1 + vat), 2)}
        m["vat"] = round(price - m["netPrice"], 2)
        try:
            m.update(amazon_fees(mp, cred, asin, price, cur))
        except Exception as exc:
            m["feesError"] = str(exc)[:200]
        m["landed"] = round(cost["landed"] * fx, 2) if cost.get("landed") else None
        ps, psales = p_spend.get(acode, 0), p_sales.get(tcode, 0)
        ds, dsales = d_spend.get((acode, dept), 0), d_sales.get((tcode, dept), 0)
        m["productTacos"] = {"spend": round(ps, 2), "sales": round(psales, 2),
                             "pct": round(ps / psales * 100, 1) if psales else None}
        m["deptTacos"] = {"spend": round(ds, 2), "sales": round(dsales, 2),
                          "pct": round(ds / dsales * 100, 1) if dsales else None}
        base = None
        if m["landed"] is not None and "referral" in m:
            base = m["netPrice"] - m["referral"] - m["fba"] - m["landed"]
            m["profitBeforeAds"] = round(base, 2)
        for key in ("productTacos", "deptTacos"):
            pct = m[key]["pct"]
            if base is None:
                continue
            if pct is None and key == "productTacos" and not ps:
                pct = 0.0  # no ads on this product = no ad cost
            if pct is None:
                continue
            ad = round(price * pct / 100, 2)
            m[key]["adCost"] = ad
            m[key]["profit"] = round(base - ad, 2)
            m[key]["margin"] = round((base - ad) / m["netPrice"] * 100, 1) if m["netPrice"] else None
        markets.append(m)
    return {"sku": row["sku"], "asin": asin, "ean": row.get("ean"), "department": dept,
            "months": [f"{y}-{m:02d}" for y, m in reversed(months)],
            "cost": cost, "markets": markets,
            "assumptions": {"shippingUsd": SHIPPING_USD, "importTaxPct": IMPORT_TAX * 100}}


def GetProductCost(request):
    args = getattr(request, "args", {}) or {}
    try:
        res = product_cost((args.get("sku") or "").strip(), (args.get("asin") or "").strip())
        return _json(res, 404 if "error" in res else 200)
    except Exception as exc:
        return _json({"error": str(exc)}, 500)


# ---------- totals per period (per the user 2026-11-01, formula OK'd) ----------
# Sales/units from asin_traffic_monthly (calendar months), everything in USD at
# today's ECB rate. Fees = referral % x sales + FBA fee x units at today's
# fee estimate (marketplaces without their own estimate borrow US for CA/MX and
# DE for the other EU ones). Ads = the product's real SP+SD spend; the extra
# "dept" profit uses the department TACOS of the same months instead.
OTHER_VAT = {"nl": 0.21, "be": 0.21, "ie": 0.23, "se": 0.25, "pl": 0.23, "ca": 0.0, "mx": 0.16}
FEE_PROXY = {"ca": "US", "mx": "US", "nl": "DE", "be": "DE", "ie": "DE", "se": "DE", "pl": "DE"}
ADS_CODE = {"us": "US", "uk": "UK", "de": "DE", "fr": "FR", "it": "IT", "es": "ES", "nl": "NL", "be": "BE",
            "se": "SE", "pl": "PL", "ie": "IE", "ca": "CA", "mx": "MX"}
PERIODS = {"last_month": "Last month", "last_3m": "Last 3 months", "ytd": "This year",
           "last_12m": "Last 12 months", "last_year": "Last year"}


def period_months(period):
    today = date.today()
    if period == "last_month":
        return last_full_months(1)
    if period == "last_3m":
        return last_full_months(3)
    if period == "last_12m":
        return last_full_months(12)
    if period == "last_year":
        return [(today.year - 1, m) for m in range(1, 13)]
    return [(today.year, m) for m in range(1, today.month + 1)]  # ytd incl. this month so far


def usd(amount, cur):
    return amount / fx_to(cur) if amount else 0.0


def fee_rates(token, asin):
    """mp code (lower) -> {referralPct, fbaUsd} from today's fee estimate at the current price."""
    prices = current_prices(token, asin)
    out = {}
    for mp, tcode, _a, _v, cur, cred in MARKETS:
        pr = prices.get(mp)
        if not pr:
            continue
        try:
            f = amazon_fees(mp, cred, asin, pr["price"], cur)
        except Exception:
            continue
        out[tcode] = {"referralPct": f["referral"] / pr["price"] if pr["price"] else 0.15,
                      "fbaUsd": usd(f["fba"], cur)}
    return out


def period_totals(sku="", asin="", period="last_3m"):
    token = pb_authenticate()
    mapping = pb_list_all(token, "asin_group_mapping", f"sku = {json.dumps(sku)}" if sku else f"asin = {json.dumps(asin)}")
    if not mapping:
        return {"error": f"{sku or asin} not in asin_group_mapping"}
    row = mapping[0]
    asin = row["asin"]
    dept = "pareo" if "PAREO" in (row.get("group") or "").upper() else "home"
    landed = landed_cost(token, row.get("ean")).get("landed")
    months = period_months(period)
    fees = fee_rates(token, asin)
    vat = {t: v for _m, t, _a, v, _c, _cr in MARKETS} | OTHER_VAT

    rows = defaultdict(lambda: defaultdict(float))
    for r in pb_list_all(token, "asin_traffic_monthly", f'asin = "{asin}" && {_month_filter(months)}',
                         fields="marketplace,units,sales,units_refunded,currency"):
        x = rows[r["marketplace"]]
        x["units"] += r.get("units") or 0
        x["refunded"] += r.get("units_refunded") or 0
        x["sales"] += usd(r.get("sales") or 0, r.get("currency") or "USD")
        x["salesLocal"] += r.get("sales") or 0
    best = {}
    for r in pb_list_all(token, "ads_advertised_product_stats", f'asin = "{asin}" && {_month_filter(months)}',
                         fields="campaign_id,ad_group_id,date,country_code,profile_id,spend,currency_code"):
        k = (r["campaign_id"], r["ad_group_id"], r["date"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    code_to_mp = {v: k for k, v in ADS_CODE.items()}
    for r in best.values():
        mp = code_to_mp.get(r["country_code"].upper())
        if mp:
            rows[mp]["ads"] += usd(r.get("spend") or 0, r.get("currency_code") or "USD")
    d_spend, d_sales = department_tacos(token, months)

    out, tot = [], defaultdict(float)
    for mp, x in rows.items():
        if not (x["units"] or x["ads"]):
            continue
        f = fees.get(mp) or fees.get((FEE_PROXY.get(mp) or "").lower()) or {"referralPct": 0.15, "fbaUsd": 0}
        v = vat.get(mp, 0.2)
        net = x["sales"] / (1 + v)
        fee = f["referralPct"] * x["sales"] + f["fbaUsd"] * x["units"]
        cost = (landed or 0) * x["units"]
        profit = net - fee - cost - x["ads"]
        ds, dsl = d_spend.get((ADS_CODE.get(mp, ""), dept), 0), d_sales.get((mp, dept), 0)
        dept_tacos = ds / dsl if dsl else None
        dept_profit = net - fee - cost - dept_tacos * x["sales"] if dept_tacos is not None else None
        line = {"mp": mp.upper(), "units": x["units"], "refunded": x["refunded"], "sales": round(x["sales"], 2),
                "vat": round(x["sales"] - net, 2), "netSales": round(net, 2), "fees": round(fee, 2),
                "productCost": round(cost, 2), "ads": round(x["ads"], 2),
                "tacos": round(x["ads"] / x["sales"] * 100, 1) if x["sales"] else None,
                "profit": round(profit, 2), "margin": round(profit / net * 100, 1) if net else None,
                "profitPerUnit": round(profit / x["units"], 2) if x["units"] else None,
                "deptTacos": round(dept_tacos * 100, 1) if dept_tacos is not None else None,
                "deptProfit": round(dept_profit, 2) if dept_profit is not None else None}
        out.append(line)
        for k in ("units", "refunded", "sales", "vat", "netSales", "fees", "productCost", "ads", "profit"):
            tot[k] += line[k]
        tot["deptProfit"] += line["deptProfit"] if line["deptProfit"] is not None else line["profit"]
    out.sort(key=lambda r: -r["sales"])
    total = {k: round(v, 2) for k, v in tot.items()}
    if out:
        total.update(mp="TOTAL", tacos=round(tot["ads"] / tot["sales"] * 100, 1) if tot["sales"] else None,
                     margin=round(tot["profit"] / tot["netSales"] * 100, 1) if tot["netSales"] else None,
                     profitPerUnit=round(tot["profit"] / tot["units"], 2) if tot["units"] else None)
    return {"sku": row["sku"], "asin": asin, "period": period, "periods": PERIODS, "landed": landed,
            "months": [f"{y}-{m:02d}" for y, m in sorted(months)], "rows": out, "total": total if out else None}


def GetProductCostPeriod(request):
    args = getattr(request, "args", {}) or {}
    try:
        period = args.get("period") or "last_3m"
        if period not in PERIODS:
            return _json({"error": f"period must be one of {list(PERIODS)}"}, 400)
        res = period_totals((args.get("sku") or "").strip(), (args.get("asin") or "").strip(), period)
        return _json(res, 404 if "error" in res else 200)
    except Exception as exc:
        return _json({"error": str(exc)}, 500)
