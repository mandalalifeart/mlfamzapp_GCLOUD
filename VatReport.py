"""VAT page (/vat) - Amazon "VAT Transactions Report" -> per-country VAT
returns + the OSS return (filed via Estonia).

Port of the user's own vat_parser_yearly_v5.py (pandas/openpyxl, produced a
spreadsheet) to the stdlib so it runs inside the local server, built
2026-10-02 at the user's request. The classification rules (buckets,
marketplace-collected detection, false-IOSS safeguard, "reportable 0%" lines
for local + export sales) are kept exactly as in that script.

The report can't be pulled via SP-API (GET_VAT_TRANSACTION_DATA returns
"Access to the resource is forbidden" - the app lacks the restricted Tax
Remittance role, checked 2026-10-02), so the user uploads the monthly files
from Seller Central (Reports -> Tax Document Library). Files are kept on disk
in one folder per month (<VAT_DOCS_DIR>/YYYY-MM/) and parsed on read, cached
by file mtime/size.
"""
import base64
import csv
import io
import json
import os
import re
import subprocess
import tempfile
import zipfile
from collections import Counter, defaultdict
from datetime import datetime

from WiseFinances import WISE_REFRESH_KEY, cors_headers, json_response

VAT_DOCS_DIR = os.environ.get("VAT_DOCS_DIR", "/home/bababot/accounting/vat")

# Countries where the business holds a local VAT registration (per the user,
# 2026-10-02). The report uses GB for the UK.
# Display order per the user (2026-10-02).
VAT_COUNTRIES = ["DE", "FR", "EE", "NL", "GB", "IT", "ES", "PL", "CZ", "IE"]

NON_EUR_OSS_COUNTRIES = {"PL": "PLN", "SE": "SEK"}
EU = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HU", "IE", "IT",
    "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK",
}
VAT_RELEVANT_TYPES = {"SALE", "REFUND", "LIQUIDATION_SALE"}
IGNORE_DEPARTURE_TYPES = {"FC_TRANSFER", "RETURN"}
COMPONENTS = [
    ("ITEMS", "TOTAL_PRICE_OF_ITEMS_AMT_VAT_EXCL", "TOTAL_PRICE_OF_ITEMS_VAT_AMT", "PRICE_OF_ITEMS_VAT_RATE_PERCENT"),
    ("SHIP", "TOTAL_SHIP_CHARGE_AMT_VAT_EXCL", "TOTAL_SHIP_CHARGE_VAT_AMT", "SHIP_CHARGE_VAT_RATE_PERCENT"),
    ("GIFT", "TOTAL_GIFT_WRAP_AMT_VAT_EXCL", "TOTAL_GIFT_WRAP_VAT_AMT", "GIFT_WRAP_VAT_RATE_PERCENT"),
]
COUNTRY_NAME_MAP = {
    "CZECH REPUBLIC": "CZ", "CZECHIA": "CZ", "GERMANY": "DE", "FRANCE": "FR", "POLAND": "PL",
    "SPAIN": "ES", "IRELAND": "IE", "ITALY": "IT", "SWITZERLAND": "CH", "UNITED KINGDOM": "GB",
    "UK": "GB", "ISRAEL": "IL", "NETHERLANDS": "NL", "ESTONIA": "EE",
}
COUNTRY_NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "HR": "Croatia", "CY": "Cyprus",
    "CZ": "Czechia", "DE": "Germany", "DK": "Denmark", "EE": "Estonia", "ES": "Spain",
    "FI": "Finland", "FR": "France", "GR": "Greece", "HU": "Hungary", "IE": "Ireland",
    "IT": "Italy", "LT": "Lithuania", "LU": "Luxembourg", "LV": "Latvia", "MT": "Malta",
    "NL": "Netherlands", "PL": "Poland", "PT": "Portugal", "RO": "Romania", "SE": "Sweden",
    "SI": "Slovenia", "SK": "Slovakia", "GB": "United Kingdom",
}
DEPARTURE_COLUMNS_CANDIDATES = ["DEPARTURE_COUNTRY", "SALE_DEPART_COUNTRY", "SELLER_DEPART_VAT_NUMBER_COUNTRY"]
ORDER_ID_COLUMNS_CANDIDATES = ["TRANSACTION_EVENT_ID", "AMAZON_ORDER_ID", "ORDER_ID", "ORDERID", "ORDER-ID"]
IMPORT_SCHEME_VALUES = {"IOSS", "IMPORT_OSS", "IMPORT_IOSS", "IMPORT"}
UNION_OSS_VALUES = {"UNION_IOSS", "UNION_OSS", "UNION-OSS", "UNION-IOSS"}
MARKETPLACE_COLLECTOR_MARKERS = ("MARKETPLACE", "AMAZON", "DEEMED")
SUPPORTED_EXTS = {".csv", ".txt", ".xlsx", ".xls"}
SHEET2CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools", "sheet2csv")
BUN = os.environ.get("BUN_PATH", os.path.expanduser("~/.bun/bin/bun"))
MONTHS = {m: i for i, m in enumerate(["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}


# ---------------------------------------------------------------- parsing

def normalize_country(value):
    s = str(value or "").upper().strip()
    if s in ("", "NAN", "NONE"):
        return None
    return COUNTRY_NAME_MAP.get(s, s)


def _num(value):
    try:
        return float(str(value).replace(",", "").strip()) if str(value or "").strip() not in ("", "nan", "NaN") else None
    except ValueError:
        return None


def _iter_xlsx(path):
    """Streaming .xlsx reader (zip + iterparse, one row in memory at a time).
    A 35MB monthly report made SheetJS (full in-memory load) use 3GB RAM and
    several minutes on this box (2026-10-02) - this stays small and fast."""
    import xml.etree.ElementTree as ET
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

    def col_index(ref):
        n = 0
        for ch in re.match(r"[A-Z]+", ref).group(0):
            n = n * 26 + ord(ch) - 64
        return n - 1

    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        shared = []
        if "xl/sharedStrings.xml" in names:
            with zf.open("xl/sharedStrings.xml") as fh:
                for event, el in ET.iterparse(fh, events=("end",)):
                    if el.tag == ns + "si":
                        shared.append("".join(t.text or "" for t in el.iter(ns + "t")))
                        el.clear()
        sheet = sorted(n for n in names if re.match(r"xl/worksheets/sheet\d+\.xml$", n))[0]
        with zf.open(sheet) as fh:
            for event, el in ET.iterparse(fh, events=("end",)):
                if el.tag != ns + "row":
                    continue
                cells = {}
                for c in el.findall(ns + "c"):
                    t, v = c.get("t"), c.find(ns + "v")
                    if t == "s" and v is not None:
                        val = shared[int(v.text)]
                    elif t == "inlineStr":
                        val = "".join(x.text or "" for x in c.iter(ns + "t"))
                    else:
                        val = v.text if v is not None else ""
                    cells[col_index(c.get("r"))] = val
                el.clear()
                if cells:
                    yield [cells.get(i, "") for i in range(max(cells) + 1)]


def iter_rows(path):
    """Yields dict rows (header = first row, upper-cased). CSV/TXT via csv
    (delimiter sniffed), XLSX streamed, legacy XLS via SheetJS (small files)."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsx":
        it = _iter_xlsx(path)
        header = [str(h or "").strip().upper() for h in next(it, [])]
        for values in it:
            yield {header[i]: (values[i] if i < len(values) else "") for i in range(len(header))}
        return
    if ext == ".xls":
        out = subprocess.run([BUN, os.path.join(SHEET2CSV, "sheet2csv.mjs"), path],
                             capture_output=True, text=True, timeout=600, cwd=SHEET2CSV)
        if out.returncode != 0:
            raise RuntimeError(f"Could not read {os.path.basename(path)}: {out.stderr[:200]}")
        text = "\n".join(l for l in out.stdout.splitlines() if not l.startswith("### Sheet:"))
    else:
        raw = open(path, "rb").read()
        for enc in ("utf-8-sig", "cp1252", "latin-1"):
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
    first = text.split("\n", 1)[0]
    delimiter = max([",", "\t", ";"], key=first.count)
    for row in csv.DictReader(io.StringIO(text), delimiter=delimiter):
        yield {(k or "").strip().upper(): v for k, v in row.items()}


def read_rows(path):
    return list(iter_rows(path))


def period_from_filename(filename):
    name = os.path.splitext(os.path.basename(filename))[0]
    m = re.search(r"(20\d{2})(\d{2})(\d{2})?", name) or re.search(r"(20\d{2})[-_](\d{2})", name)
    if m and 1 <= int(m.group(2)) <= 12:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"(?<!\d)(\d{2})[-_](\d{2})(?!\d)", name)
    if m and 1 <= int(m.group(1)) <= 12:
        return f"20{m.group(2)}-{m.group(1)}"
    return None


def is_marketplace_collected(value):
    s = str(value or "").upper().strip()
    if s in ("", "NAN", "NONE"):
        return False
    return any(marker in s for marker in MARKETPLACE_COLLECTOR_MARKERS)


def bucket_for(row, vat_countries):
    dest = row["dest"]
    if is_marketplace_collected(row["tax_resp"]):
        return "MARKETPLACE_COLLECTED"
    scheme = row["scheme"]
    if scheme in IMPORT_SCHEME_VALUES:
        return "IOSS_RETURN"
    if scheme in UNION_OSS_VALUES:
        return "OSS_RETURN"
    if dest in vat_countries:
        return "DOMESTIC_RETURN"
    if dest in EU:
        return "OSS_RETURN"
    return "OUTSIDE_EU"


def _row_period(r):
    ap = re.match(r"(20\d{2})-([A-Z]{3})", (r.get("ACTIVITY_PERIOD") or "").strip().upper())
    return f"{ap.group(1)}-{MONTHS[ap.group(2)]:02d}" if ap and ap.group(2) in MONTHS else None


def normalize_file(path, vat_countries, display_name=None):
    """One streaming pass over the report: departure warnings (the script's
    check in "warn" mode), period detection, and the normalized VAT lines.
    -> (file period YYYY-MM, lines, warnings)."""
    offenders = defaultdict(lambda: {"count": 0, "orders": []})
    activity, dates = Counter(), Counter()
    out, pending = [], []
    for r in iter_rows(path):
        ttype = (r.get("TRANSACTION_TYPE") or "").strip().upper()
        rp = _row_period(r)
        if rp:
            activity[rp] += 1
        elif len(dates) < 50:
            for col in ("TRANSACTION_COMPLETE_DATE", "TAX_CALCULATION_DATE", "TRANSACTION_DEPART_DATE"):
                v = (r.get(col) or "").strip()[:10]
                for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%m/%d/%Y"):
                    try:
                        dates[datetime.strptime(v, fmt).strftime("%Y-%m")] += 1
                        break
                    except ValueError:
                        continue
        if ttype not in IGNORE_DEPARTURE_TYPES:
            for col in DEPARTURE_COLUMNS_CANDIDATES:
                if col not in r:
                    continue
                dep_c = normalize_country(r.get(col))
                if dep_c and dep_c not in vat_countries:
                    o = offenders[(col, dep_c)]
                    o["count"] += 1
                    oid = next((r.get(c) for c in ORDER_ID_COLUMNS_CANDIDATES if r.get(c)), None)
                    if oid and len(o["orders"]) < 20 and oid not in o["orders"]:
                        o["orders"].append(oid)
        if ttype not in VAT_RELEVANT_TYPES:
            continue
        dep = next((normalize_country(r.get(c)) for c in DEPARTURE_COLUMNS_CANDIDATES if c in r), None)
        dest = normalize_country(r.get("SALE_ARRIVAL_COUNTRY"))
        collector = (r.get("TAX_COLLECTION_RESPONSIBILITY") or "").upper().strip() or None
        scheme = (r.get("TAX_REPORTING_SCHEME") or "").upper().strip() or None
        if scheme in IMPORT_SCHEME_VALUES and dep in EU:
            scheme = None  # the script's default false-IOSS safeguard
        base = {
            # Quarterly/multi-month files (the 2021 reports cover a quarter
            # each) - every row keeps its own ACTIVITY_PERIOD month.
            "period": rp, "source_file": display_name or os.path.basename(path), "dest": dest,
            "tax_resp": collector, "scheme": scheme, "transaction_type": ttype,
            "tx_id": next(((r.get(c) or "").strip() for c in ORDER_ID_COLUMNS_CANDIDATES if (r.get(c) or "").strip()), None),
            "sale_depart": normalize_country(r.get("SALE_DEPART_COUNTRY") or r.get("DEPARTURE_COUNTRY")),
            "tax_calc_date": (r.get("TAX_CALCULATION_DATE") or "").strip() or None,
            "currency": (r.get("TRANSACTION_CURRENCY_CODE") or "").upper().strip() or None,
            # Buyer details for the KMD invoice list - Amazon only fills
            # BUYER_NAME / BUYER_VAT_NUMBER for business (B2B) buyers.
            "buyer_name": (r.get("BUYER_NAME") or "").strip() or None,
            "buyer_vat": (r.get("BUYER_VAT_NUMBER") or "").strip() or None,
            "arrival_city": (r.get("ARRIVAL_CITY") or "").strip() or None,
        }
        for comp, netc, vatc, ratec in COMPONENTS:
            net, vat, rate = _num(r.get(netc)) or 0.0, _num(r.get(vatc)) or 0.0, _num(r.get(ratec))
            if net == 0 and vat == 0:
                continue
            row = dict(base, component=comp, net=net, vat=vat, gross=net + vat,
                       rate_percent=round(rate * 100, 2) if rate is not None else None)
            row["bucket"] = bucket_for(row, vat_countries)
            out.append(row)
    period = (activity.most_common(1)[0][0] if activity else None) \
        or period_from_filename(display_name or path) or (dates.most_common(1)[0][0] if dates else None)
    for row in out:
        if not row["period"]:
            row["period"] = period
    warnings = [{"column": c, "country": d, **v} for (c, d), v in sorted(offenders.items())]
    return period, out, warnings


_CACHE = {}
PARSE_VERSION = 2  # bump when normalize_file's output fields change (invalidates the disk cache)
_PARSE_LOCK = __import__("threading").Lock()
CACHE_DIR = os.path.join(VAT_DOCS_DIR, ".cache")


def parsed(path, vat_countries):
    """normalize_file result, cached in memory and on disk (JSON next to the
    files, keyed by name/mtime/size) so a server restart doesn't re-parse
    every year's spreadsheets."""
    st = os.stat(path)
    key = (path, st.st_mtime, st.st_size, tuple(vat_countries))
    if key in _CACHE:
        return _CACHE[key]
    stamp = f"v{PARSE_VERSION}-{int(st.st_mtime)}-{st.st_size}-{'_'.join(vat_countries)}"
    cache_file = os.path.join(CACHE_DIR, os.path.basename(os.path.dirname(path)) + "__" + os.path.basename(path) + ".json")
    with _PARSE_LOCK:  # one big parse at a time - this box has ~6GB RAM
        if key in _CACHE:
            return _CACHE[key]
        try:
            with open(cache_file) as fh:
                cached = json.load(fh)
            if cached.get("stamp") == stamp:
                _CACHE[key] = (cached["period"], cached["rows"], cached["warnings"])
                return _CACHE[key]
        except (OSError, ValueError):
            pass
        result = normalize_file(path, set(vat_countries))
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_file, "w") as fh:
            json.dump({"stamp": stamp, "period": result[0], "rows": result[1], "warnings": result[2]}, fh)
        _CACHE[key] = result
        return result


def load_year(year, vat_countries):
    files = []
    if os.path.isdir(VAT_DOCS_DIR):
        for month_dir in sorted(os.listdir(VAT_DOCS_DIR)):
            if not month_dir.startswith(str(year)):
                continue
            folder = os.path.join(VAT_DOCS_DIR, month_dir)
            for f in sorted(os.listdir(folder)):
                if os.path.splitext(f)[1].lower() in SUPPORTED_EXTS:
                    files.append(os.path.join(folder, f))
    rows, warnings, sources = [], [], []
    for path in files:
        period, nrows, warns = parsed(path, vat_countries)
        rows.extend(nrows)
        warnings.extend(dict(w, file=os.path.basename(path), period=period) for w in warns)
        sources.append({"file": os.path.basename(path), "period": period, "rows": len(nrows),
                        "month": os.path.basename(os.path.dirname(path))})
    return rows, warnings, sources


# ---------------------------------------------------------------- returns

def reportable_zero_rows(rows, country):
    """The script's reportable 0% lines: local 0% sales departing the country
    + 0% exports to non-EU destinations departing the country; never
    marketplace-collected, sales/liquidations only."""
    out = []
    for r in rows:
        if is_marketplace_collected(r["tax_resp"]) or r["transaction_type"] not in ("SALE", "LIQUIDATION_SALE"):
            continue
        if r["rate_percent"] != 0 or r["sale_depart"] != country:
            continue
        if r["bucket"] == "DOMESTIC_RETURN" and r["dest"] == country:
            out.append(r)
        elif r["bucket"] == "OUTSIDE_EU" and r["dest"] not in EU:
            out.append(r)
    return out


def country_return(rows, country):
    # Per the user (2026-10-02): lines the report flags as Union-OSS but whose
    # destination is one of YOUR VAT countries belong on that country's
    # return (shown with the country), not on the OSS return.
    domestic = [r for r in rows if r["bucket"] in ("DOMESTIC_RETURN", "OSS_RETURN") and r["dest"] == country
                and not is_marketplace_collected(r["tax_resp"])]
    zero = reportable_zero_rows(rows, country)
    reporting = [r for r in domestic if r["rate_percent"] != 0] + zero
    # Grouped by (rate, currency): a country can have sales in two currencies
    # (e.g. PL: PLN on amazon.pl + EUR) - never summed together.
    by = defaultdict(lambda: defaultdict(lambda: {"net": 0.0, "vat": 0.0, "gross": 0.0, "count": 0}))
    for r in reporting:
        cell = by[(r["rate_percent"], r["currency"] or "")][r["period"]]
        cell["net"] += r["net"]
        cell["vat"] += r["vat"]
        cell["gross"] += r["gross"]
        cell["count"] += 1
    rates = []
    for key in sorted(by, key=lambda x: (x[1], x[0] is None, x[0] or 0)):
        rate, cur = key
        months = {p: {k: round(v, 2) if isinstance(v, float) else v for k, v in c.items()} for p, c in by[key].items()}
        total = {k: round(sum(c[k] for c in by[key].values()), 2) for k in ("net", "vat", "gross")}
        rates.append({"rate": rate, "currency": cur, "months": months, "total": total})
    currencies = sorted({r["currency"] for r in reporting if r["currency"]})
    details = sorted(
        [{k: r[k] for k in ("period", "tx_id", "transaction_type", "component", "dest", "sale_depart", "tax_resp",
                            "tax_calc_date", "net", "vat", "gross", "currency", "bucket", "source_file")} for r in zero],
        key=lambda d: (d["period"] or "", d["net"]),
    )
    oss_flagged = [r for r in domestic if r["bucket"] == "OSS_RETURN"]
    # All lines with this destination, by bucket (the script's "VAT COUNTRIES"
    # section of Monthly_Net_ByDestRate) - incl. marketplace-collected.
    bk = defaultdict(lambda: defaultdict(lambda: {"net": 0.0, "vat": 0.0, "count": 0}))
    for r in rows:
        if r["dest"] != country:
            continue
        c = bk[(r["bucket"], r["rate_percent"], r["currency"] or "")][r["period"]]
        c["net"] += r["net"]
        c["vat"] += r["vat"]
        c["count"] += 1
    buckets = []
    for (bucket, rate, cur) in sorted(bk, key=lambda k: (k[0], k[2], k[1] is None, k[1] or 0)):
        months = {p: {"net": round(v["net"], 2), "vat": round(v["vat"], 2), "count": v["count"]} for p, v in bk[(bucket, rate, cur)].items()}
        buckets.append({"bucket": bucket, "rate": rate, "currency": cur, "months": months,
                        "total": {"net": round(sum(v["net"] for v in months.values()), 2),
                                  "vat": round(sum(v["vat"] for v in months.values()), 2)}})
    return {"country": country, "name": COUNTRY_NAMES.get(country, country), "currencies": currencies,
            "buckets": buckets,
            "ossFlagged": {"count": len(oss_flagged), "net": round(sum(r["net"] for r in oss_flagged), 2),
                           "vat": round(sum(r["vat"] for r in oss_flagged), 2)},
            "rates": rates, "zeroRated": details, "transactions": len(reporting),
            "marketplaceCollected": sum(1 for r in rows if r["dest"] == country and r["bucket"] == "MARKETPLACE_COLLECTED")}


def quarter_of(period):
    y, m = period.split("-")
    return f"{y}-Q{(int(m) - 1) // 3 + 1}"


def oss_return(rows):
    oss = [r for r in rows if r["bucket"] in ("OSS_RETURN", "IOSS_RETURN") and (r["net"] or r["vat"]) and r["period"]
           and r["dest"] not in VAT_COUNTRIES]
    groups = defaultdict(lambda: {"net": 0.0, "vat": 0.0, "gross": 0.0, "currencies": set(), "count": 0})
    for r in oss:
        scheme = "Union OSS" if r["bucket"] == "OSS_RETURN" else "Import IOSS"
        g = groups[(quarter_of(r["period"]), scheme, r["dest"], r["rate_percent"])]
        g["net"] += r["net"]
        g["vat"] += r["vat"]
        g["gross"] += r["gross"]
        g["count"] += 1
        if r["currency"]:
            g["currencies"].add(r["currency"])
    lines = []
    for (q, scheme, dest, rate), g in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1], COUNTRY_NAMES.get(kv[0][2] or "", kv[0][2] or ""), kv[0][3] or 0)):
        cur = ", ".join(sorted(g["currencies"]))
        lines.append({
            "quarter": q, "scheme": scheme, "countryCode": dest, "country": COUNTRY_NAMES.get(dest or "", dest),
            "rate": rate, "net": round(g["net"], 2), "vat": round(g["vat"], 2), "gross": round(g["gross"], 2),
            "currency": cur, "count": g["count"],
            "fxNote": "" if cur in ("", "EUR") else f"Convert {cur} to EUR (ECB rate, last day of quarter) before filing",
        })
    return lines


def VatReportSummary(request):
    """GET ?year=YYYY - per VAT-country returns + OSS return for that year."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    args = getattr(request, "args", {}) or {}
    try:
        available = sorted({d[:4] for d in os.listdir(VAT_DOCS_DIR) if re.match(r"20\d{2}-\d{2}$", d)}
                           if os.path.isdir(VAT_DOCS_DIR) else set(), reverse=True)
        year = str(args.get("year") or (available[0] if available else datetime.now().year))
        if year == "all":
            return json_response({"year": "all", "years": available, **all_years_payable(available)})
        rows, warnings, sources = load_year(year, VAT_COUNTRIES)
        periods = sorted({r["period"] for r in rows if r["period"]})
        try:
            deductible, deductible_invoices = deductible_vat_by_country(year)
        except Exception as exc:  # Wise data is optional for this page
            deductible, deductible_invoices = {}, {"_error": str(exc)}
        bucket_totals = Counter()
        for r in rows:
            bucket_totals[r["bucket"]] += 1
        return json_response({
            "year": year, "years": available, "periods": periods, "files": sources,
            "vatCountries": VAT_COUNTRIES,
            "countries": [country_return(rows, c) for c in VAT_COUNTRIES],
            "deductible": deductible,
            "deductibleInvoices": deductible_invoices,
            "submittedReturns": submitted_returns(),
            "manualEntries": manual_entries(year),
            "oss": oss_return(rows),
            "departureWarnings": warnings,
            "bucketCounts": dict(bucket_totals),
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def all_years_payable(available):
    """Per the user (2026-11-02): "all till now" = VAT to pay per country per
    year. Per year and currency: VAT on the return lines (same lines as the
    country tables) - deductible input VAT from /wise invoices - typed-in
    import VAT; OSS = VAT on the OSS return. Rows are filed by their own
    ACTIVITY_PERIOD year (a folder can hold a neighbouring month)."""
    rows = []
    for y in sorted(available):
        rows.extend(load_year(y, VAT_COUNTRIES)[0])
    by_year = defaultdict(list)
    for r in rows:
        if r.get("period"):
            by_year[r["period"][:4]].append(r)
    years = sorted(set(by_year) | set(available))
    try:
        deductible, _inv = deductible_vat_by_country(r"20\d{2}")
    except Exception:  # Wise data is optional
        deductible = {}
    table = {c: defaultdict(lambda: defaultdict(lambda: {"vat": 0.0, "deductible": 0.0})) for c in VAT_COUNTRIES}
    oss = defaultdict(lambda: defaultdict(float))
    for y in years:
        yr = by_year.get(y, [])
        for c in VAT_COUNTRIES:
            for rate in country_return(yr, c)["rates"]:
                if rate["rate"] and rate["currency"]:
                    table[c][y][rate["currency"]]["vat"] += rate["total"]["vat"]
        for line in oss_return(yr):
            oss[y][line["currency"] or "EUR"] += line["vat"]
        for c, types in manual_entries(y).items():
            for e in (types.get("import_vat") or {}).values():
                if c in table and e.get("amount"):
                    table[c][y][e.get("currency") or "EUR"]["deductible"] += e["amount"]
    for c, months in deductible.items():
        if c not in table:
            continue
        for month, curs in months.items():
            for cur, amt in curs.items():
                table[c][month[:4]][cur]["deductible"] += amt
    out = []
    for c in VAT_COUNTRIES:
        ys = {}
        for y, curs in table[c].items():
            ys[y] = {cur: {"vat": round(v["vat"], 2), "deductible": round(v["deductible"], 2),
                           "payable": round(v["vat"] - v["deductible"], 2)}
                     for cur, v in curs.items() if v["vat"] or v["deductible"]}
        out.append({"country": c, "name": COUNTRY_NAMES.get(c, c), "years": ys})
    return {"allYears": {"years": years, "countries": out,
                         "oss": {y: {cur: round(v, 2) for cur, v in curs.items()} for y, curs in oss.items()}}}


def UploadVatReport(request):
    """POST JSON {filename, contentBase64} (+ key) - one monthly VAT
    Transactions Report (csv/txt/xlsx) or a zip of several. Each file is
    stored in <VAT_DOCS_DIR>/<YYYY-MM>/ by the period it covers; a new
    upload for the same month and file name replaces the old one."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    args = getattr(request, "args", {}) or {}
    body = request.get_json(silent=True) or {}
    if WISE_REFRESH_KEY and (args.get("key") or body.get("key")) != WISE_REFRESH_KEY:
        return json_response({"error": "Unauthorized"}, 401)
    try:
        data = base64.b64decode(body.get("contentBase64") or "")
        filename = os.path.basename(body.get("filename") or "vat_report.csv")
        files = []
        if zipfile.is_zipfile(io.BytesIO(data)) and not filename.lower().endswith((".xlsx",)):
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                for n in zf.namelist():
                    if os.path.splitext(n)[1].lower() in SUPPORTED_EXTS and not os.path.basename(n).startswith("."):
                        files.append((os.path.basename(n), zf.read(n)))
        else:
            files.append((filename, data))
        stored = []
        for name, content in files:
            ext = os.path.splitext(name)[1].lower()
            if ext not in SUPPORTED_EXTS:
                stored.append({"file": name, "error": "unsupported file type"})
                continue
            safe = re.sub(r"[^A-Za-z0-9._-]+", "-", name)
            period = period_from_filename(name)
            if period:
                # Fast path: month known from the file name - store now, parse
                # (and cache) in the background so a big file never times out
                # the upload request.
                folder = os.path.join(VAT_DOCS_DIR, period)
                os.makedirs(folder, exist_ok=True)
                dest = os.path.join(folder, safe)
                with open(dest, "wb") as fh:
                    fh.write(content)
                __import__("threading").Thread(target=parsed, args=(dest, VAT_COUNTRIES), daemon=True).start()
                stored.append({"file": name, "period": period, "rows": None, "processing": True})
                continue
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            try:
                period, nrows, _w = normalize_file(tmp_path, set(VAT_COUNTRIES), display_name=name)
            except Exception as exc:
                os.unlink(tmp_path)
                stored.append({"file": name, "error": f"not a VAT Transactions Report? {exc}"})
                continue
            if not period:
                os.unlink(tmp_path)
                stored.append({"file": name, "error": "could not determine the month of this report"})
                continue
            folder = os.path.join(VAT_DOCS_DIR, period)
            os.makedirs(folder, exist_ok=True)
            dest = os.path.join(folder, safe)
            os.replace(tmp_path, dest)
            stored.append({"file": name, "period": period, "rows": len(nrows)})
        return json_response({"stored": stored})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


# ---------------------------------------------------------------- deductible (input) VAT from the Wise invoices

def deductible_vat_by_country(year):
    """Input VAT per (country, YYYY-MM, currency) from the invoices uploaded on
    /wise (Claude-read tax_amount / tax_country), per the user (2026-10-02).
    Excludes sales invoices to Amazon/Etsy and documents attached to a
    "Taxes & VAT" payment (those are VAT paid to a tax office, not input VAT).
    Month = the invoice date's month (deduction period), else the document's."""
    import WiseLedger as W
    token = W.pb_authenticate()
    rules = {r["rule_key"]: r["classification"] for r in W.pb_list_all(token, W.RULES_COLLECTION)}
    # Documents on a VAT payment to a tax office, or on any INCOMING money (e.g.
    # a Finanzamt VAT refund), are not input VAT.
    tax_payment_keys = {r["ledger_key"] for r in W.pb_list_all(token, W.LEDGER_COLLECTION)
                        if r.get("direction") == "in" or W.classify(r, rules)[0] == "Taxes & VAT"}
    out = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    invoices = defaultdict(list)
    for d in W.pb_list_all(token, W.DOCUMENTS_COLLECTION, filter_str="kind = 'invoice'"):
        ex = d.get("extracted") or {}
        tax = ex.get("tax_amount") or 0
        if not tax or (d.get("group_key") or "").split("|")[0] in W.GROUP_INVOICE_CLASSES.values():
            continue
        if d.get("ledger_key") in tax_payment_keys:
            continue
        country = (ex.get("tax_country") or "").upper()
        country = {"UK": "GB"}.get(country, country)
        month = (ex.get("invoice_date") or "")[:7] or (d.get("month") or "")
        if not re.match(rf"{year}-\d{{2}}$", month):
            continue
        cur = (ex.get("currency") or "EUR").upper()
        out[country][month][cur] += tax
        invoices[country].append({"id": d["id"], "month": month, "vendor": ex.get("vendor"), "number": ex.get("invoice_number"),
                                  "date": ex.get("invoice_date"), "tax": round(tax, 2), "rate": ex.get("tax_rate"),
                                  "currency": cur})
    return ({c: {m: {cur: round(v, 2) for cur, v in curs.items()} for m, curs in months.items()} for c, months in out.items()},
            {c: sorted(v, key=lambda i: i["month"]) for c, v in invoices.items()})


# ---------------------------------------------------------------- submitted returns (user's own filings)

SUBMITTED_DIR = os.path.join(VAT_DOCS_DIR, "submitted")
_SUBMITTED_RE = re.compile(r"^(UK|GB|NL|DE|FR|IT|ES|PL|CZ|IE|EE)_((?:\d{2}_)+)(\d{2})\.csv$", re.I)


def submitted_returns():
    """Returns the user filed, from ~/accounting/vat/submitted/<CC>_<MM>_<MM>_<MM>_<YY>.csv
    (box CSV as exported by the UK VAT software: "BOX n,description,value"),
    e.g. UK_11_12_01_26.csv - months are listed in order; a month number
    smaller than the first one belongs to the next calendar year (Nov-Dec-Jan).
    Shown next to the computed quarter on /vat (per the user 2026-10-03)."""
    import csv as _csv
    out = defaultdict(list)
    if not os.path.isdir(SUBMITTED_DIR):
        return {}
    for name in sorted(os.listdir(SUBMITTED_DIR)):
        m = _SUBMITTED_RE.match(name)
        if not m:
            continue
        country = {"UK": "GB"}.get(m.group(1).upper(), m.group(1).upper())
        nums = [int(x) for x in m.group(2).strip("_").split("_")]
        year = 2000 + int(m.group(3))
        # the year in the name is the year of the first month
        months, y = [], year
        for i, mm in enumerate(nums):
            if i and mm < nums[i - 1]:
                y += 1
            months.append(f"{y:04d}-{mm:02d}")
        boxes = {}
        with open(os.path.join(SUBMITTED_DIR, name), newline="", encoding="utf-8-sig") as fh:
            for row in _csv.reader(fh):
                if len(row) >= 3 and row[0].strip().upper().startswith("BOX"):
                    try:
                        boxes[row[0].strip().upper().replace("BOX", "").strip()] = float(row[2].replace(",", "").strip() or 0)
                    except ValueError:
                        pass
        out[country].append({"file": name, "months": months, "boxes": boxes})
    return dict(out)


# ---------------------------------------------------------------- manual entries (e.g. DE import VAT)

MANUAL_COLLECTION = "vat_manual_entries"


def manual_entries(year):
    """{country: {entry_type: {YYYY-MM: {amount, currency, note}}}} for the year.
    Values the user types in on /vat, e.g. German import VAT (Einfuhrumsatz-
    steuer, entry_type "import_vat") - deductible input VAT that never shows up
    as an invoice on /wise (2026-10-03, per the user)."""
    import WiseLedger as W
    token = W.pb_authenticate()
    out = defaultdict(lambda: defaultdict(dict))
    for r in W.pb_list_all(token, MANUAL_COLLECTION, filter_str=f'month ~ "{int(year)}-"'):
        out[r["country"]][r["entry_type"]][r["month"]] = {"amount": r.get("amount") or 0,
                                                         "currency": r.get("currency") or "EUR", "note": r.get("note") or ""}
    return {c: dict(v) for c, v in out.items()}


def SetVatManualEntry(request):
    """{country, month (YYYY-MM), entryType, amount, currency, note} (+ key)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    body = request.get_json(silent=True) or {}
    args = getattr(request, "args", {}) or {}
    if WISE_REFRESH_KEY and (args.get("key") or body.get("key")) != WISE_REFRESH_KEY:
        return json_response({"error": "Unauthorized"}, 401)
    try:
        country = (body.get("country") or "").upper()
        month = body.get("month") or ""
        entry_type = body.get("entryType") or "import_vat"
        if not re.fullmatch(r"[A-Z]{2}", country) or not re.fullmatch(r"\d{4}-\d{2}", month):
            return json_response({"error": "country (2 letters) and month (YYYY-MM) are required"}, 400)
        import WiseLedger as W
        token = W.pb_authenticate()
        record = {"country": country, "month": month, "entry_type": entry_type,
                  "amount": float(body.get("amount") or 0), "currency": (body.get("currency") or "EUR").upper(),
                  "note": body.get("note") or ""}
        existing = W.pb_list_all(token, MANUAL_COLLECTION,
                                 filter_str=f'country = "{country}" && month = "{month}" && entry_type = "{entry_type}"')
        op = ({"method": "PATCH", "url": f"/api/collections/{MANUAL_COLLECTION}/records/{existing[0]['id']}", "body": record}
              if existing else {"method": "POST", "url": f"/api/collections/{MANUAL_COLLECTION}/records", "body": record})
        W.pb_batch(token, [op])
        return json_response({"ok": True, **record})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


# ---------------------------------------------------------------- Estonian KMD XML + OSS file

EE_REG_CODE = os.environ.get("EE_COMPANY_REG_CODE", "16137563")  # Mandala Life ART OÜ (from its invoices)

# KMD declarationBody element per VAT rate (current e-MTA vatdeclaration.xsd:
# 24% from 07.2025, 22% 01.2024-06.2025, 20% before, 13% from 01.2025, 9%, 5%).
KMD_RATE_ELEMENTS = {24.0: "transactions24", 22.0: "transactions22", 20.0: "transactions20",
                     13.0: "transactions13", 9.0: "transactions9", 5.0: "transactions5"}
KMD_BODY_ORDER = ["noSales", "noPurchases", "sumPerPartnerSales", "sumPerPartnerPurchases",
                  "transactions24", "transactions22", "transactions20", "transactions9", "transactions5",
                  "transactions13", "transactionsZeroVat", "euSupplyInclGoodsAndServicesZeroVat",
                  "euSupplyGoodsZeroVat", "exportZeroVat", "inputVatTotal"]


def ee_kmd_figures(year, month):
    rows, _w, _s = load_year(year, VAT_COUNTRIES)
    period = f"{year}-{int(month):02d}"
    rows = [r for r in rows if r["period"] == period]
    ee = [r for r in rows if r["bucket"] in ("DOMESTIC_RETURN", "OSS_RETURN") and r["dest"] == "EE"
          and not is_marketplace_collected(r["tax_resp"]) and (r["currency"] or "EUR") == "EUR"]
    figures = defaultdict(float)
    unmapped = defaultdict(float)
    for r in ee:
        if r["rate_percent"] in (0, None):
            continue
        tag = KMD_RATE_ELEMENTS.get(float(r["rate_percent"]))
        (figures if tag else unmapped)[tag or r["rate_percent"]] += r["net"]
    zero = reportable_zero_rows(rows, "EE")
    figures["transactionsZeroVat"] = sum(r["net"] for r in zero)
    figures["exportZeroVat"] = sum(r["net"] for r in zero if r["bucket"] == "OUTSIDE_EU")
    deductible, _inv = deductible_vat_by_country(year)
    figures["inputVatTotal"] = (deductible.get("EE", {}).get(period, {}) or {}).get("EUR", 0.0)
    return {k: round(v, 2) for k, v in figures.items()}, dict(unmapped)


def EeKmdXml(request):
    """GET ?year=YYYY&month=M -> downloadable KMD XML for e-MTA import."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    args = getattr(request, "args", {}) or {}
    try:
        year, month = int(args.get("year")), int(args.get("month"))
        figures, unmapped = ee_kmd_figures(year, month)
        # Per the user (2026-10-08): noSales/noPurchases must be false whenever
        # the month has sales / input VAT (a July 2026 file said noSales=true
        # next to a 24% sale).
        has_sales = any(figures.get(k) for k in KMD_BODY_ORDER if k.startswith(("transactions", "euSupply", "export")))
        has_purchases = bool(figures.get("inputVatTotal"))
        body = {"noSales": "false" if has_sales else "true", "noPurchases": "false" if has_purchases else "true",
                "sumPerPartnerSales": "false", "sumPerPartnerPurchases": "false"}
        for k, v in figures.items():
            if v:
                body[k] = f"{v:.2f}"
        lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<vatDeclaration>",
                 f"  <taxPayerRegCode>{EE_REG_CODE}</taxPayerRegCode>", f"  <year>{year}</year>",
                 f"  <month>{month}</month>", "  <declarationType>1</declarationType>",
                 "  <version>KMD6</version>", "  <declarationBody>"]
        for tag in KMD_BODY_ORDER:
            if tag in body:
                lines.append(f"    <{tag}>{body[tag]}</{tag}>")
        lines.append("  </declarationBody>")
        # KMD INF annexes with the same lines as the KMD invoices XLSX (per
        # the user 2026-10-08: "KMD XML should have the same fields as the KMD
        # xlsx"): part A = Amazon orders (sales), part B = /wise purchase
        # invoices with Estonian input VAT. Element names per vatdeclaration.xsd
        # SaleLine / PurchaseLine.
        from xml.sax.saxutils import escape as _x

        def _d(v):
            return v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else ""

        def _rate(r):
            pct = round((r or 0) * 100, 2)
            return str(int(pct)) if pct == int(pct) else str(pct)
        # KMD INF lists only invoices with an Estonian-registered partner (registry/VAT code) whose
        # monthly total ex-VAT is >= EUR 1,000; private customers never go there (e-MTA rejected the
        # July 2026 file: "check your counterparty's name, or enter its commercial register number",
        # 2026-10-14). The KMD invoices XLSX keeps the full list.
        sales = kmd_inf_lines(kmd_sales_rows(year, month))
        purchases = kmd_inf_lines(kmd_purchase_rows(year, month))
        if sales:
            lines += ["  <salesAnnex>", "    <noSales>false</noSales>", "    <sumPerPartnerSales>false</sumPerPartnerSales>"]
            for r in sales:
                lines.append("    <saleLine>")
                if r["code"]:
                    lines.append(f"      <buyerRegCode>{_x(str(r['code'])[:20])}</buyerRegCode>")
                lines += [f"      <buyerName>{_x(r['name'][:182])}</buyerName>",
                          f"      <invoiceNumber>{_x(str(r['number'])[:200])}</invoiceNumber>"]
                if _d(r["date"]):
                    lines.append(f"      <invoiceDate>{_d(r['date'])}</invoiceDate>")
                lines += [f"      <invoiceSum>{r['net']:.2f}</invoiceSum>", f"      <taxRate>{_rate(r['rate'])}</taxRate>",
                          f"      <sumForRateInPeriod>{r['net']:.2f}</sumForRateInPeriod>"]
                # <comments> is KMD INF column 10 "Erisuse kood" (classifier COMMENT_SALES/PURCHASES) - only
                # e-MTA codes are allowed there; our free-text notes made e-MTA reject the July 2026 file
                # ("please specify the special scheme code"), so they are never written (2026-10-13).
                if r.get("special_code"):
                    lines.append(f"      <comments>{_x(str(r['special_code'])[:50])}</comments>")
                lines.append("    </saleLine>")
            lines.append("  </salesAnnex>")
        if purchases:
            lines += ["  <purchasesAnnex>", "    <noPurchases>false</noPurchases>",
                      "    <sumPerPartnerPurchases>false</sumPerPartnerPurchases>"]
            for r in purchases:
                vat = round(r["net"] * (r["rate"] or 0), 2)
                lines.append("    <purchaseLine>")
                if r["code"]:
                    lines.append(f"      <sellerRegCode>{_x(str(r['code'])[:20])}</sellerRegCode>")
                lines += [f"      <sellerName>{_x(r['name'][:182])}</sellerName>",
                          f"      <invoiceNumber>{_x(str(r['number'])[:200])}</invoiceNumber>"]
                if _d(r["date"]):
                    lines.append(f"      <invoiceDate>{_d(r['date'])}</invoiceDate>")
                lines += [f"      <invoiceSumVat>{r['net'] + vat:.2f}</invoiceSumVat>", f"      <vatSum>{vat:.2f}</vatSum>",
                          f"      <vatInPeriod>{vat:.2f}</vatInPeriod>"]
                # <comments> is KMD INF column 10 "Erisuse kood" (classifier COMMENT_SALES/PURCHASES) - only
                # e-MTA codes are allowed there; our free-text notes made e-MTA reject the July 2026 file
                # ("please specify the special scheme code"), so they are never written (2026-10-13).
                if r.get("special_code"):
                    lines.append(f"      <comments>{_x(str(r['special_code'])[:50])}</comments>")
                lines.append("    </purchaseLine>")
            lines.append("  </purchasesAnnex>")
        lines.append("</vatDeclaration>")
        if unmapped:
            lines.insert(1, f"<!-- WARNING: sales at rates with no KMD line were left out: {unmapped} -->")
        xml = "\n".join(lines) + "\n"
        headers = dict(cors_headers())
        headers.update({"Content-Type": "application/xml; charset=utf-8",
                        "Content-Disposition": f'attachment; filename="KMD_EE_{year}-{month:02d}.xml"'})
        return xml, 200, headers
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def OssReturnCsv(request):
    """GET ?quarter=YYYY-Qn -> CSV of the OSS return lines for manual entry in
    e-MTA (OSS data can't be imported there as a file)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    args = getattr(request, "args", {}) or {}
    try:
        quarter = args.get("quarter") or ""
        year = quarter[:4]
        rows, _w, _s = load_year(year, VAT_COUNTRIES)
        lines = [l for l in oss_return(rows) if l["quarter"] == quarter]
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=";")
        w.writerow(["Quarter", "Scheme", "Country code", "Country", "VAT rate %", "Taxable amount (net)", "VAT amount",
                    "Currency", "Note"])
        for l in lines:
            w.writerow([l["quarter"], l["scheme"], l["countryCode"], l["country"], l["rate"], f"{l['net']:.2f}",
                        f"{l['vat']:.2f}", l["currency"], l["fxNote"]])
        headers = dict(cors_headers())
        headers.update({"Content-Type": "text/csv; charset=utf-8",
                        "Content-Disposition": f'attachment; filename="OSS_{quarter}.csv"'})
        return "﻿" + buf.getvalue(), 200, headers
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


# ---------------------------------------------------------------- KMD_Invoices_EN.xlsx (the user's own template)

KMD_TEMPLATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "KMD_Invoices_EN.xlsx")
KMD_FIRST_ROW, KMD_LAST_ROW = 6, 304  # template formulas (Annex A/B SUMIFS) cover rows 6..304


def _excel_serial(d):
    return (d - datetime(1899, 12, 30)).days


def _parse_any_date(text):
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime((text or "")[:10], fmt)
        except ValueError:
            continue
    return None


KMD_INF_THRESHOLD_EUR = 1000


def kmd_inf_lines(rows):
    """Rows that belong on KMD INF: partner has a registry/VAT code and the
    partner's total net for the month is >= KMD_INF_THRESHOLD_EUR."""
    totals = {}
    for r in rows:
        if r["code"]:
            totals[r["code"]] = totals.get(r["code"], 0) + r["net"]
    return [r for r in rows if r["code"] and totals[r["code"]] >= KMD_INF_THRESHOLD_EUR]


def kmd_sales_rows(year, month):
    """Amazon orders taxed in Estonia for the month (EE local return lines +
    reportable 0% lines), one row per order and VAT rate."""
    rows, _w, _s = load_year(year, VAT_COUNTRIES)
    period = f"{year}-{int(month):02d}"
    rows = [r for r in rows if r["period"] == period]
    ee = [r for r in rows if r["bucket"] in ("DOMESTIC_RETURN", "OSS_RETURN") and r["dest"] == "EE"
          and not is_marketplace_collected(r["tax_resp"]) and r["rate_percent"] not in (0, None)]
    ee += reportable_zero_rows(rows, "EE")
    grouped = {}
    for r in ee:
        key = (r["tx_id"], r["rate_percent"], r["transaction_type"])
        g = grouped.setdefault(key, {"date": _parse_any_date(r["tax_calc_date"]), "order": r["tx_id"] or "",
                                     "rate": (r["rate_percent"] or 0) / 100, "net": 0.0, "type": r["transaction_type"],
                                     "dest": r["dest"], "currency": r["currency"], "buyer_name": r.get("buyer_name"),
                                     "buyer_vat": r.get("buyer_vat"), "city": r.get("arrival_city")})
        g["net"] += r["net"]
    out = []
    for g in sorted(grouped.values(), key=lambda g: (g["date"] or datetime.min, g["order"])):
        comment = f"Amazon {g['type'].lower().replace('_', ' ')}" + (f" - export to {g['dest']}" if g["dest"] != "EE" else "")
        if g["currency"] and g["currency"] != "EUR":
            comment += f" (amount in {g['currency']})"
        # Amazon's VAT report has the buyer's name/VAT number only for business
        # buyers; consumer orders carry no name at all (privacy) - those show
        # as a private customer with the delivery town.
        name = g["buyer_name"] or (f"Private customer ({g['city']})" if g["city"] else "Private customer (B2C)")
        out.append({"date": g["date"], "code": g["buyer_vat"] or "", "name": name, "number": g["order"],
                    "net": round(g["net"], 2), "rate": g["rate"], "comment": comment})
    return out


def kmd_purchase_rows(year, month):
    """Expense invoices uploaded on /wise dated in the month that carry a
    positive ESTONIAN input VAT (per the user, 2026-10-02 - foreign-VAT and
    no-VAT invoices don't belong on the KMD purchase list), in EUR (ECB rate
    on the invoice date)."""
    import WiseLedger as W
    token = W.pb_authenticate()
    rules = {r["rule_key"]: r["classification"] for r in W.pb_list_all(token, W.RULES_COLLECTION)}
    excluded_keys = {r["ledger_key"] for r in W.pb_list_all(token, W.LEDGER_COLLECTION)
                     if r.get("direction") == "in" or W.classify(r, rules)[0] == "Taxes & VAT"}
    period = f"{year}-{int(month):02d}"
    out = []
    for d in W.pb_list_all(token, W.DOCUMENTS_COLLECTION, filter_str="kind = 'invoice'"):
        ex = d.get("extracted") or {}
        if (d.get("group_key") or "").split("|")[0] in W.GROUP_INVOICE_CLASSES.values():
            continue  # our own sales invoices to Amazon/Etsy
        if d.get("ledger_key") in excluded_keys or not ex.get("total"):
            continue
        inv_date = _parse_any_date(ex.get("invoice_date"))
        if (inv_date.strftime("%Y-%m") if inv_date else d.get("month")) != period:
            continue
        cur = (ex.get("currency") or "EUR").upper()
        day = inv_date.strftime("%Y-%m-%d") if inv_date else f"{period}-15"
        try:
            W.eur_rates_for({cur}, day, day)
        except Exception:
            pass
        total_eur = W.to_eur(ex.get("total") or 0, cur, day)
        tax_eur = W.to_eur(ex.get("tax_amount") or 0, cur, day) if ex.get("tax_amount") else 0.0
        if total_eur is None:
            continue
        estonian = (ex.get("tax_country") or "").upper() == "EE" and tax_eur
        if not (estonian and tax_eur > 0):
            continue  # per the user: only invoices with (positive) Estonian input VAT belong here
        comment = []
        if cur != "EUR":
            comment.append(f"{ex.get('total')} {cur} at ECB rate {day}")
        if ex.get("tax_amount") and not estonian:
            comment.append(f"{ex.get('tax_country') or 'foreign'} VAT {ex.get('tax_amount')} {cur} - not Estonian input VAT")
        out.append({
            "date": inv_date, "code": "", "name": ex.get("vendor") or "", "number": ex.get("invoice_number") or "",
            "net": round(total_eur - tax_eur, 2) if estonian else round(total_eur, 2),
            "rate": (ex.get("tax_rate") or 0) / 100 if estonian else 0.0,
            "comment": "; ".join(comment),
        })
    return sorted(out, key=lambda r: (r["date"] or datetime.min, r["name"]))


def _fill_sheet(xml, rows, subtitle):
    import html
    esc = lambda t: html.escape(str(t or ""), quote=False)

    def set_cell(xml, ref, inner_xml, t=None):
        pattern = re.compile(rf'<c r="{ref}"( s="\d+")?(?: t="\w+")?\s*(/>|>.*?</c>)', re.S)
        m = pattern.search(xml)
        if not m:
            return xml
        style = m.group(1) or ""
        tattr = f' t="{t}"' if t else ""
        return xml[:m.start()] + f'<c r="{ref}"{style}{tattr}>{inner_xml}</c>' + xml[m.end():]

    def clear_cell(xml, ref):
        pattern = re.compile(rf'<c r="{ref}"( s="\d+")?(?: t="\w+")?\s*(/>|>(?!<f).*?</c>)', re.S)
        m = pattern.search(xml)
        return xml if not m else xml[:m.start()] + f'<c r="{ref}"{m.group(1) or ""}/>' + xml[m.end():]

    # remove the template's example row (row 5) values; its formulas stay,
    # with their stale cached results blanked
    for col in "ABCDEFK":
        xml = clear_cell(xml, f"{col}5")
    for col in "GHIJ":
        xml = re.sub(rf'(<c r="{col}5"[^>]*?)(?: t="\w+")?(>\s*<f[^>]*>.*?</f>)<v>[^<]*</v>', r'\1 t="str"\2<v></v>', xml, flags=re.S)
    xml = set_cell(xml, "A2", f"<is><t>{esc(subtitle)}</t></is>", "inlineStr")
    for i, r in enumerate(rows[:KMD_LAST_ROW - KMD_FIRST_ROW + 1]):
        n = KMD_FIRST_ROW + i
        if r["date"]:
            xml = set_cell(xml, f"A{n}", f"<v>{_excel_serial(r['date'])}</v>")
        # column K = "Erisuse kood" (special scheme code) - e-MTA codes only, never our free-text notes
        for col, key in (("B", "code"), ("C", "name"), ("D", "number"), ("K", "special_code")):
            if r.get(key):
                xml = set_cell(xml, f"{col}{n}", f"<is><t>{esc(r[key])}</t></is>", "inlineStr")
        xml = set_cell(xml, f"E{n}", f"<v>{r['net']}</v>")
        xml = set_cell(xml, f"F{n}", f"<v>{r['rate']}</v>")
    return xml


def build_kmd_invoices_xlsx(year, month):
    sales, purchases = kmd_sales_rows(year, month), kmd_purchase_rows(year, month)
    subtitle = f"Mandala Life ART OÜ (reg. {EE_REG_CODE}) - {int(year)}-{int(month):02d}"
    buf = io.BytesIO()
    with zipfile.ZipFile(KMD_TEMPLATE) as src, zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as dst:
        for item in src.infolist():
            data = src.read(item.filename)
            if item.filename == "xl/worksheets/sheet1.xml":
                data = _fill_sheet(data.decode("utf-8"), sales, subtitle + " - sales (Amazon orders, VAT report)").encode("utf-8")
            elif item.filename == "xl/worksheets/sheet2.xml":
                data = _fill_sheet(data.decode("utf-8"), purchases, subtitle + " - purchases (invoices on /wise)").encode("utf-8")
            elif item.filename == "xl/workbook.xml":
                # cached formula results are stale - make Excel recalculate on open
                data = data.decode("utf-8").replace("<calcPr/>", '<calcPr fullCalcOnLoad="1"/>').encode("utf-8")
            dst.writestr(item, data)
    return buf.getvalue(), len(sales), len(purchases)


def EeKmdInvoicesXlsx(request):
    """GET ?year=YYYY&month=M -> the user's KMD_Invoices_EN.xlsx template
    filled for Estonia: Amazon orders as sales, /wise invoices as purchases."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    args = getattr(request, "args", {}) or {}
    try:
        year, month = int(args.get("year")), int(args.get("month"))
        data, n_sales, n_purchases = build_kmd_invoices_xlsx(year, month)
        headers = dict(cors_headers())
        headers.update({"Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "Content-Disposition": f'attachment; filename="KMD_Invoices_EE_{year}-{month:02d}.xlsx"',
                        "X-Rows": f"{n_sales} sales, {n_purchases} purchases"})
        return data, 200, headers
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
