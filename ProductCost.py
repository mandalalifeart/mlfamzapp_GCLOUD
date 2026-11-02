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
import calendar
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


def amazon_fees_many(mp, cred, asin, prices, cur):
    """Fees at several prices in ONE batch call (getMyFeesEstimates) - the
    single-item endpoint throttles after a few calls. -> {price: fees}."""
    todo = [p for p in prices if f"fees|{mp}|{asin}|{p}" not in _mem]
    if todo:
        c = dict(refresh_token=os.environ[f"REFRESH_TOKEN_{cred}"], lwa_app_id=os.environ[f"CLIENT_ID_{cred}"],
                 lwa_client_secret=os.environ[f"CLIENT_SECRET_{cred}"])
        reqs = [{"price": p, "currency": cur, "is_fba": True, "id_type": "ASIN", "id_value": asin,
                 "identifier": f"{asin}-{p}"} for p in todo]
        for attempt in range(5):
            try:
                res = ProductFees(credentials=c, marketplace=getattr(Marketplaces, mp)).get_product_fees_estimate(reqs).payload
                break
            except Exception as exc:
                if "QuotaExceeded" not in str(exc) or attempt == 4:
                    raise
                time.sleep(5 * (attempt + 1))
        for r in res:
            p = float(r["FeesEstimateIdentifier"]["PriceToEstimateFees"]["ListingPrice"]["Amount"])
            if r.get("Status") != "Success":
                continue
            fees = {f["FeeType"]: f["FinalFee"]["Amount"] for f in r["FeesEstimate"]["FeeDetailList"]}
            _mem[f"fees|{mp}|{asin}|{p}"] = (time.time(), {
                "referral": fees.get("ReferralFee", 0) + fees.get("VariableClosingFee", 0) + fees.get("PerItemFee", 0),
                "fba": fees.get("FBAFees", 0)})
    return {p: _mem[f"fees|{mp}|{asin}|{p}"][1] for p in prices if f"fees|{mp}|{asin}|{p}" in _mem}


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


# ---------- SB spend split over units (per the user 2026-11-02) ----------
# SB ads aren't reported per product, so each product carries a share:
#   SB cost of an ASIN = SB spend of its section (home decor / pareo) in that
#                        marketplace and month / all units the section sold
#                        there that month x units of the ASIN.
# Section of an SB campaign = portfolio/name contains PAREO/SARONG/BEACH ->
# pareo, else home decor (same as department_tacos). Shares add up to the SB
# spend exactly. SB data starts Jan 2026 (none stored for 2025).

def _section_of_group(group):
    g = (group or "").upper()
    if not g or g.startswith("IGNORE"):
        return None
    return "pareo" if "PAREO" in g else "home"


def sb_spend_monthly(token, months):
    """(ads COUNTRY, section, 'YYYY-MM') -> SB spend in USD."""
    def compute():
        ports = {p["portfolio_id"]: p["name"] for p in pb_list_all(token, "ads_portfolios", 'portfolio_id != ""',
                                                                   fields="portfolio_id,name")}
        camp_port = {c["campaign_id"]: ports.get(c.get("portfolio_id"), "")
                     for c in pb_list_all(token, "ads_campaigns", 'campaign_id != ""', fields="campaign_id,portfolio_id")}
        best = {}
        for r in pb_list_all(token, "ads_campaign_stats", f'ad_product = "SPONSORED_BRANDS" && {_month_filter(months)}',
                             fields="campaign_id,campaign_name,country_code,profile_id,date,spend,currency_code"):
            k = (r["campaign_id"], r["date"])
            if k not in best or r["profile_id"] < best[k]["profile_id"]:
                best[k] = r
        out = defaultdict(float)
        for r in best.values():
            text = f"{camp_port.get(r['campaign_id'], '')} {r['campaign_name']}".upper()
            seg = "pareo" if any(w in text for w in PAREO_WORDS) else "home"
            out[(r["country_code"].upper(), seg, r["date"][:7])] += usd(r.get("spend") or 0, r.get("currency_code") or "USD")
        return dict(out)
    return _cached(f"sbspend|{sorted(months)}", 6, compute)


def section_units_monthly(token, months):
    """(ads COUNTRY, section, 'YYYY-MM') -> all units sold (asin_traffic_monthly)."""
    def compute():
        groups = {m["asin"]: m["group"] for m in pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,group")}
        out = defaultdict(float)
        for r in pb_list_all(token, "asin_traffic_monthly", _month_filter(months), fields="asin,marketplace,year,month,units"):
            seg = _section_of_group(groups.get(r["asin"]))
            ads = ADS_CODE.get(r["marketplace"])
            if seg and ads:
                out[(ads, seg, f"{int(r['year']):04d}-{int(r['month']):02d}")] += r.get("units") or 0
        return dict(out)
    return _cached(f"secunits|{sorted(months)}", 6, compute)


def sb_per_unit_monthly(token, months):
    """(ads COUNTRY, section, 'YYYY-MM') -> SB USD per unit sold."""
    spend, units = sb_spend_monthly(token, months), section_units_monthly(token, months)
    return {k: v / units[k] for k, v in spend.items() if units.get(k)}


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
    # Returns / FBA storage / AWD per unit sold, last 3 full months, per marketplace (USD).
    from ProductExtraCosts import extra_costs
    first, last = min(months), max(months)
    x_start = f"{first[0]:04d}-{first[1]:02d}-01"
    x_end = f"{last[0]:04d}-{last[1]:02d}-{calendar.monthrange(*last)[1]:02d}"
    fees_now = {}
    for mp_, tcode_, _a, _v, cur_, cred_ in MARKETS:
        pr_ = prices.get(mp_)
        if pr_:
            try:
                f_ = amazon_fees(mp_, cred_, asin, pr_["price"], cur_)
                fees_now[tcode_] = {"referralPct": f_["referral"] / pr_["price"], "fbaUsd": usd(f_["fba"], cur_)}
            except Exception:
                pass
    to_traffic = {"usa": "us", "mex": "mx"}
    extra = extra_costs(token, x_start, x_end, set(ATOMIC),
                        lambda mp_, _a: fees_now.get(to_traffic.get(mp_, mp_)) or fees_now.get("de") or {"referralPct": 0.15, "fbaUsd": 0},
                        lambda _a: cost.get("landed"), usd, lambda mp_: ATOMIC.get(mp_, (None, None, 0.2))[2], by_mp=True)
    extra_mp = defaultdict(lambda: defaultdict(float))
    for (a, mp_), x in extra.items():
        if a == asin:
            for k, v in x.items():
                extra_mp[to_traffic.get(mp_, mp_)][k] += v
    units_mp = defaultdict(float)
    for r in pb_list_all(token, "asin_traffic_monthly", f'asin = "{asin}" && {_month_filter(months)}', fields="marketplace,units"):
        units_mp[r["marketplace"]] += r.get("units") or 0
    # SB share per unit (USD) over the same 3 months: section SB spend / section units.
    sb_sp, sb_un = sb_spend_monthly(token, months), section_units_monthly(token, months)
    sb_unit = {}
    for (country, seg, _ym), v in sb_sp.items():
        if seg == dept:
            sb_unit.setdefault(country, [0.0, 0.0])[0] += v
    for (country, seg, _ym), v in sb_un.items():
        if seg == dept and country in sb_unit:
            sb_unit[country][1] += v
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
        sbs, sbu = sb_unit.get(acode, (0.0, 0.0))
        m["sbPerUnit"] = round(sbs / sbu * fx, 2) if sbu else 0.0
        m["productTacos"] = {"spend": round(ps, 2), "sales": round(psales, 2),
                             "pct": round(ps / psales * 100, 1) if psales else None,
                             "sbPerUnit": m["sbPerUnit"]}
        m["deptTacos"] = {"spend": round(ds, 2), "sales": round(dsales, 2),
                          "pct": round(ds / dsales * 100, 1) if dsales else None}
        ex, u = extra_mp.get(tcode, {}), units_mp.get(tcode, 0)
        per = lambda k: round(ex.get(k, 0) / u * fx, 2) if u else 0.0  # noqa: E731
        m["returnsPerUnit"], m["storagePerUnit"], m["awdPerUnit"] = per("returnsCost"), per("storage"), per("awd")
        m["returnRate"] = round(ex.get("returnedUnits", 0) / u * 100, 1) if u else None
        m["extraPerUnit"] = round(m["returnsPerUnit"] + m["storagePerUnit"] + m["awdPerUnit"], 2)
        base = None
        if m["landed"] is not None and "referral" in m:
            base = m["netPrice"] - m["referral"] - m["fba"] - m["landed"] - m["extraPerUnit"]
            m["profitBeforeAds"] = round(base, 2)
        for key in ("productTacos", "deptTacos"):
            pct = m[key]["pct"]
            if base is None:
                continue
            if pct is None and key == "productTacos" and not ps:
                pct = 0.0  # no SP/SD ads on this product = only its SB share
            if pct is None:
                continue
            ad = round(price * pct / 100 + (m["sbPerUnit"] if key == "productTacos" else 0), 2)
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


def period_totals(sku="", asin="", period="last_3m", months=None, _token=None):
    token = _token or pb_authenticate()
    mapping = pb_list_all(token, "asin_group_mapping", f"sku = {json.dumps(sku)}" if sku else f"asin = {json.dumps(asin)}")
    if not mapping:
        return {"error": f"{sku or asin} not in asin_group_mapping"}
    row = mapping[0]
    asin = row["asin"]
    dept = "pareo" if "PAREO" in (row.get("group") or "").upper() else "home"
    landed = landed_cost(token, row.get("ean")).get("landed")
    months = months or period_months(period)
    fees = fee_rates(token, asin)
    vat = {t: v for _m, t, _a, v, _c, _cr in MARKETS} | OTHER_VAT

    rows = defaultdict(lambda: defaultdict(float))
    sb_rate = sb_per_unit_monthly(token, months)
    for r in pb_list_all(token, "asin_traffic_monthly", f'asin = "{asin}" && {_month_filter(months)}',
                         fields="marketplace,units,sales,units_refunded,currency,year,month"):
        x = rows[r["marketplace"]]
        ym = f"{int(r['year']):04d}-{int(r['month']):02d}"
        x["sb"] += sb_rate.get((ADS_CODE.get(r["marketplace"], ""), dept, ym), 0) * (r.get("units") or 0)
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

    # Returns / FBA storage / AWD per marketplace (ProductExtraCosts).
    from ProductExtraCosts import extra_costs
    first, last = min(months), max(months)
    start = f"{first[0]:04d}-{first[1]:02d}-01"
    end = min(f"{last[0]:04d}-{last[1]:02d}-{calendar.monthrange(*last)[1]:02d}", date.today().isoformat())
    to_traffic = {"usa": "us", "mex": "mx"}

    def fee_lookup(mp, _asin):
        t = to_traffic.get(mp, mp)
        return fees.get(t) or fees.get((FEE_PROXY.get(t) or "").lower()) or {"referralPct": 0.15, "fbaUsd": 0}
    extra = extra_costs(token, start, end, set(ATOMIC), fee_lookup, lambda _a: landed, usd,
                        lambda mp: ATOMIC.get(mp, (None, None, 0.2))[2], by_mp=True)
    for (a, mp), x in extra.items():
        if a == asin:
            r = rows[to_traffic.get(mp, mp)]
            for k in ("returnedUnits", "returnsCost", "storage", "awd"):
                r[k] += x.get(k, 0)

    out, tot = [], defaultdict(float)
    for mp, x in rows.items():
        if not (x["units"] or x["ads"] or x["storage"] or x["returnsCost"]):
            continue
        f = fees.get(mp) or fees.get((FEE_PROXY.get(mp) or "").lower()) or {"referralPct": 0.15, "fbaUsd": 0}
        v = vat.get(mp, 0.2)
        net = x["sales"] / (1 + v)
        fee = f["referralPct"] * x["sales"] + f["fbaUsd"] * x["units"]
        cost = (landed or 0) * x["units"]
        extras = x["returnsCost"] + x["storage"] + x["awd"]
        profit = net - fee - cost - x["ads"] - x["sb"] - extras
        ds, dsl = d_spend.get((ADS_CODE.get(mp, ""), dept), 0), d_sales.get((mp, dept), 0)
        dept_tacos = ds / dsl if dsl else None
        dept_profit = net - fee - cost - extras - dept_tacos * x["sales"] if dept_tacos is not None else None
        line = {"mp": mp.upper(), "units": x["units"], "refunded": x["refunded"], "sales": round(x["sales"], 2),
                "vat": round(x["sales"] - net, 2), "netSales": round(net, 2), "fees": round(fee, 2),
                "productCost": round(cost, 2), "ads": round(x["ads"], 2), "sbAds": round(x["sb"], 2),
                "returnedUnits": x["returnedUnits"], "returns": round(x["returnsCost"], 2),
                "storage": round(x["storage"], 2), "awd": round(x["awd"], 2),
                "tacos": round((x["ads"] + x["sb"]) / x["sales"] * 100, 1) if x["sales"] else None,
                "profit": round(profit, 2), "margin": round(profit / net * 100, 1) if net else None,
                "profitPerUnit": round(profit / x["units"], 2) if x["units"] else None,
                "deptTacos": round(dept_tacos * 100, 1) if dept_tacos is not None else None,
                "deptProfit": round(dept_profit, 2) if dept_profit is not None else None}
        out.append(line)
        for k in ("units", "refunded", "returnedUnits", "sales", "vat", "netSales", "fees", "productCost", "ads",
                  "sbAds", "returns", "storage", "awd", "profit"):
            tot[k] += line[k]
        tot["deptProfit"] += line["deptProfit"] if line["deptProfit"] is not None else line["profit"]
    out.sort(key=lambda r: -r["sales"])
    total = {k: round(v, 2) for k, v in tot.items()}
    if out:
        total.update(mp="TOTAL", tacos=round((tot["ads"] + tot["sbAds"]) / tot["sales"] * 100, 1) if tot["sales"] else None,
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


ADS_DATA_FROM = (2026, 1)  # per-product ad spend (SP/SD/SB) is stored from Jan 2026


def profit_monthly(sku="", asin="", n=12):
    """Profitability graph (per the user 2026-11-02): the period totals for each
    of the last n months incl. the current one (all marketplaces, USD)."""
    token = pb_authenticate()
    y, m = date.today().year, date.today().month
    months = []
    for _ in range(n):
        months.append((y, m))
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    out, info = [], {}
    for ym in reversed(months):
        res = period_totals(sku, asin, months=[ym], _token=token)
        if "error" in res:
            return res
        info = {"sku": res["sku"], "asin": res["asin"], "landed": res["landed"]}
        t = res.get("total") or {}
        out.append({"month": f"{ym[0]:04d}-{ym[1]:02d}", "partial": ym == (date.today().year, date.today().month),
                    "adsData": ym >= ADS_DATA_FROM,
                    **{k: t.get(k, 0) for k in ("units", "sales", "netSales", "fees", "productCost", "ads", "sbAds",
                                                "returns", "storage", "awd", "profit")},
                    "margin": t.get("margin"), "tacos": t.get("tacos")})
    return {**info, "months": out}


def GetProductProfitMonthly(request):
    args = getattr(request, "args", {}) or {}
    try:
        res = profit_monthly((args.get("sku") or "").strip(), (args.get("asin") or "").strip(),
                             min(int(args.get("months") or 12), 24))
        return _json(res, 404 if "error" in res else 200)
    except Exception as exc:
        return _json({"error": str(exc)}, 500)


# ---------- price scenarios (per the user 2026-11-01) ----------
# Click a price on the product page -> the same per-unit lines at -20/-10/0/+10/+20%.
# Fees are re-estimated by Amazon at each price; the AD COST PER UNIT stays at
# today's level (current TACOS x current price) - we can't know how sales
# respond to the price, so ads are not scaled with it.
SCENARIO_STEPS = (-20, -10, 0, 10, 20)


def price_scenarios(sku, mp):
    base = product_cost(sku=sku)
    if "error" in base:
        return base
    m = next((x for x in base["markets"] if x["mp"] == mp.upper()), None)
    if not m:
        return {"error": f"no current price for {sku} in {mp}"}
    cfg = next(c for c in MARKETS if c[0] == mp.upper())
    _mp, _t, _a, vat, cur, cred = cfg
    out = []
    prices = [round(m["price"] * (1 + step / 100), 2) for step in SCENARIO_STEPS]
    try:
        fees = amazon_fees_many(mp.upper(), cred, base["asin"], prices, cur)
        fee_error = ""
    except Exception as exc:
        fees, fee_error = {}, str(exc)[:200]
    for step, price in zip(SCENARIO_STEPS, prices):
        row = {"step": step, "price": price, "netPrice": round(price / (1 + vat), 2)}
        row["vat"] = round(price - row["netPrice"], 2)
        if price in fees:
            row.update(fees[price])
        else:
            row["feesError"] = fee_error or "no fee estimate"
        if m.get("landed") is None or "referral" not in row:
            out.append(row)
            continue
        before = row["netPrice"] - row["referral"] - row["fba"] - m["landed"]
        row["profitBeforeAds"] = round(before, 2)
        for key in ("productTacos", "deptTacos"):
            ad = m[key].get("adCost")
            if ad is None:
                continue
            row[key] = {"adCost": ad, "profit": round(before - ad, 2),
                        "margin": round((before - ad) / row["netPrice"] * 100, 1) if row["netPrice"] else None}
        out.append(row)
    return {"sku": base["sku"], "mp": mp.upper(), "currency": cur, "landed": m.get("landed"),
            "productTacosPct": m["productTacos"]["pct"], "deptTacosPct": m["deptTacos"]["pct"], "rows": out}


def GetProductCostScenarios(request):
    args = getattr(request, "args", {}) or {}
    try:
        res = price_scenarios((args.get("sku") or "").strip(), (args.get("mp") or "").strip())
        return _json(res, 404 if "error" in res else 200)
    except Exception as exc:
        return _json({"error": str(exc)}, 500)


# ---------- Best & Worst sellers by profit / ACOS / TACOS (per the user 2026-11-01) ----------
# Any date range, from order_items (real price paid after promotions) and the
# daily ads_advertised_product_stats. Per ASIN, in USD at today's rate:
#   sales      = net_amount of non-cancelled order lines (incl. VAT in EU/UK/MX)
#   net sales  = sales / (1 + VAT)
#   fees       = referral % x sales + FBA fee x units (today's estimate per
#                marketplace; CA/MX borrow US, other EU borrow DE)
#   cost       = units x landed cost
#   profit     = net sales - fees - cost - ad spend;  margin = profit / net sales
#   ACOS       = ad spend / ad-attributed sales;  TACOS = ad spend / sales
ATOMIC = {  # order_items marketplace -> (fees market, ads country, VAT)
    "usa": ("US", "US", 0.0), "ca": ("US", "CA", 0.0), "mex": ("US", "MX", 0.16), "uk": ("UK", "UK", 0.20),
    "de": ("DE", "DE", 0.19), "fr": ("FR", "FR", 0.20), "it": ("IT", "IT", 0.22), "es": ("ES", "ES", 0.21),
    "nl": ("DE", "NL", 0.21), "be": ("DE", "BE", 0.21), "ie": ("DE", "IE", 0.23), "se": ("DE", "SE", 0.25),
    "pl": ("DE", "PL", 0.23),
}
FEE_CACHE = os.path.join(HERE, "logs", "fee_estimates.json")
FEE_CACHE_DAYS = 7


def fee_table(token, asins):
    """(fees market, asin) -> {referralPct, fbaUsd}, from batch fee estimates at
    each ASIN's current price; cached on disk for FEE_CACHE_DAYS."""
    try:
        cache = json.load(open(FEE_CACHE))
    except (OSError, ValueError):
        cache = {}
    now = time.time()
    latest = {}
    need = defaultdict(list)
    for r in pb_list_all(token, "price_history", "", fields="marketplace,asin,price,since"):
        k = (r["marketplace"], r["asin"])
        if k not in latest or r["since"] >= latest[k]["since"]:
            latest[k] = r
    for mp, _t, _a, _v, cur, cred in MARKETS:
        for asin in asins:
            key = f"{mp}|{asin}"
            pr = latest.get((mp, asin))
            if not pr:
                continue
            hit = cache.get(key)
            if hit and hit.get("price") == pr["price"] and now - hit["at"] < FEE_CACHE_DAYS * 86400:
                continue
            need[(mp, cur, cred)].append((asin, pr["price"]))
    for (mp, cur, cred), items in need.items():
        c = dict(refresh_token=os.environ[f"REFRESH_TOKEN_{cred}"], lwa_app_id=os.environ[f"CLIENT_ID_{cred}"],
                 lwa_client_secret=os.environ[f"CLIENT_SECRET_{cred}"])
        client = ProductFees(credentials=c, marketplace=getattr(Marketplaces, mp))
        for i in range(0, len(items), 20):
            chunk = items[i:i + 20]
            reqs = [{"price": p, "currency": cur, "is_fba": True, "id_type": "ASIN", "id_value": a,
                     "identifier": f"{a}|{p}"} for a, p in chunk]
            res = None
            for attempt in range(6):
                try:
                    res = client.get_product_fees_estimate(reqs).payload
                    break
                except Exception as exc:
                    if "QuotaExceeded" not in str(exc) or attempt == 5:
                        break
                    time.sleep(5 * (attempt + 1))
            for r in res or []:
                if r.get("Status") != "Success":
                    continue
                a, p = r["FeesEstimateIdentifier"]["SellerInputIdentifier"].split("|")
                fees = {f["FeeType"]: f["FinalFee"]["Amount"] for f in r["FeesEstimate"]["FeeDetailList"]}
                ref = fees.get("ReferralFee", 0) + fees.get("VariableClosingFee", 0) + fees.get("PerItemFee", 0)
                cache[f"{mp}|{a}"] = {"price": float(p), "at": now, "referralPct": ref / float(p) if float(p) else 0.15,
                                      "fbaUsd": usd(fees.get("FBAFees", 0), cur)}
            time.sleep(2.1)
    if need:
        os.makedirs(os.path.dirname(FEE_CACHE), exist_ok=True)
        json.dump(cache, open(FEE_CACHE, "w"))
    return {tuple(k.split("|")): v for k, v in cache.items()}


def extra_costs_for(token, start, end, atomic, fees, landed_of, by_mp=False):
    """Returns / FBA storage / AWD per ASIN (ProductExtraCosts) with this module's fee table."""
    from ProductExtraCosts import extra_costs

    def fee_lookup(mp, asin):
        fmk = ATOMIC.get(mp, ("DE",))[0]
        return fees.get((fmk, asin)) or {"referralPct": 0.15, "fbaUsd": 0.0}
    return extra_costs(token, start, end, set(atomic), fee_lookup, landed_of, usd,
                       lambda mp: ATOMIC.get(mp, (None, None, 0.2))[2], by_mp=by_mp)


def product_ranking(start_date, end_date, marketplaces=None):
    from GetSalesDepartmentReport import expand_marketplaces
    token = pb_authenticate()
    atomic = expand_marketplaces(marketplaces or [])
    mapping = {r["asin"]: r for r in pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,sku,ean,group")}
    mp_filter = " || ".join(f'marketplace = "{m}"' for m in sorted(atomic) if m in ATOMIC)
    if not mp_filter:
        return {"byAsin": {}}
    lines = pb_list_all(token, "order_items",
                        f'purchase_date >= "{start_date}" && purchase_date <= "{end_date}" && ({mp_filter}) '
                        f'&& order_status != "Cancelled"',
                        fields="asin,marketplace,qty,net_amount,currency,purchase_date")
    sec_units, asin_units = defaultdict(float), defaultdict(float)
    fees = fee_table(token, sorted({l["asin"] for l in lines if l.get("asin")}))
    landed_by_ean = {}
    agg = defaultdict(lambda: defaultdict(float))
    for l in lines:
        asin, mp = l.get("asin"), l["marketplace"]
        if not asin or not l.get("qty"):
            continue
        fmk, _ads, vat = ATOMIC[mp]
        sales = usd(l.get("net_amount") or 0, l.get("currency") or "USD")
        f = fees.get((fmk, asin)) or {"referralPct": 0.15, "fbaUsd": 0.0}
        a = agg[asin]
        seg = _section_of_group((mapping.get(asin) or {}).get("group"))
        if seg:
            sec_units[(ATOMIC[mp][1], seg, (l.get("purchase_date") or "")[:7])] += l["qty"]
            asin_units[(asin, ATOMIC[mp][1], seg, (l.get("purchase_date") or "")[:7])] += l["qty"]
        a["units"] += l["qty"]
        a["sales"] += sales
        a["netSales"] += sales / (1 + vat)
        a["fees"] += f["referralPct"] * sales + f["fbaUsd"] * l["qty"]
        if f is fees.get((fmk, asin)) and not f["fbaUsd"]:
            a["missingFees"] = 1
    countries = {ATOMIC[m][1] for m in atomic if m in ATOMIC}
    best = {}
    for r in pb_list_all(token, "ads_advertised_product_stats", f'date >= "{start_date}" && date <= "{end_date}"',
                         fields="asin,campaign_id,ad_group_id,date,country_code,profile_id,spend,sales,currency_code"):
        if r["country_code"].upper() not in countries:
            continue
        k = (r["campaign_id"], r["ad_group_id"], r["date"], r["asin"])
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    for r in best.values():
        a = agg[r["asin"]]
        a["ads"] += usd(r.get("spend") or 0, r.get("currency_code") or "USD")
        a["adSales"] += usd(r.get("sales") or 0, r.get("currency_code") or "USD")
    # SB share: section SB spend per country-month (prorated to the days of the
    # month inside the range) / section units in the range that month.
    from ProductExtraCosts import _overlap
    rng = [(int(start_date[:4]), int(start_date[5:7]))]
    while rng[-1] < (int(end_date[:4]), int(end_date[5:7])):
        y, m = rng[-1]
        rng.append((y + 1, 1) if m == 12 else (y, m + 1))
    sb_sp = sb_spend_monthly(token, rng)
    sb_unit = {}
    for (country, seg, ym), v in sb_sp.items():
        share = _overlap(int(ym[:4]), int(ym[5:7]), start_date, end_date)
        if sec_units.get((country, seg, ym)):
            sb_unit[(country, seg, ym)] = v * share / sec_units[(country, seg, ym)]
    for (asin, country, seg, ym), q in asin_units.items():
        agg[asin]["sbAds"] += sb_unit.get((country, seg, ym), 0) * q

    def landed_of(asin):
        ean = (mapping.get(asin) or {}).get("ean")
        if ean not in landed_by_ean:
            landed_by_ean[ean] = landed_cost(token, ean).get("landed") if ean else None
        return landed_by_ean[ean]

    extra = extra_costs_for(token, start_date, end_date, atomic, fees, landed_of)
    out = {}
    for asin, a in agg.items():
        landed = landed_of(asin)
        cost = (landed or 0) * a["units"]
        x = extra.get(asin, {})
        returns, storage, awd = x.get("returnsCost", 0), x.get("storage", 0), x.get("awd", 0)
        profit = a["netSales"] - a["fees"] - cost - a["ads"] - a["sbAds"] - returns - storage - awd
        out[asin] = {
            "units": a["units"], "sales": round(a["sales"], 2), "netSales": round(a["netSales"], 2),
            "fees": round(a["fees"], 2), "productCost": round(cost, 2), "landed": landed,
            "ads": round(a["ads"], 2), "sbAds": round(a["sbAds"], 2), "adSales": round(a["adSales"], 2),
            "returnedUnits": x.get("returnedUnits", 0), "returns": round(returns, 2),
            "returnsPct": round(x.get("returnedUnits", 0) / a["units"] * 100, 1) if a["units"] else None,
            "storage": round(storage, 2), "awd": round(awd, 2),
            "profit": round(profit, 2) if landed else None,
            "margin": round(profit / a["netSales"] * 100, 1) if landed and a["netSales"] else None,
            "acos": round(a["ads"] / a["adSales"] * 100, 1) if a["adSales"] else None,
            "tacos": round((a["ads"] + a["sbAds"]) / a["sales"] * 100, 1) if a["sales"] else None,
        }
    return {"start": start_date, "end": end_date, "byAsin": out}


def GetProductRanking(request):
    if getattr(request, "method", "GET") == "OPTIONS":
        return "", 204, {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST, OPTIONS",
                         "Access-Control-Allow-Headers": "Content-Type"}
    body = {}
    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        pass
    args = getattr(request, "args", {}) or {}
    start = body.get("start_date") or args.get("start_date")
    end = body.get("end_date") or args.get("end_date")
    if not start or not end:
        return _json({"error": "start_date and end_date required"}, 400)
    try:
        return _json(product_ranking(start, end, body.get("marketplaces")))
    except Exception as exc:
        return _json({"error": str(exc)}, 500)
