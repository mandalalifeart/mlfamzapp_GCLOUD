"""Returns, FBA storage and AWD cost per ASIN for a date range (per the user
2026-11-01, formula OK'd). All in USD at today's rate. India -> US/EU freight
and duty are NOT here (they stay the $1 + 10% estimate in the landed cost).

Returns (by return date, amazon_returns joined to order_items by order id for
the real price paid and the marketplace):
  per returned unit = price paid without VAT          (sale reversed)
                    - 0.8 x referral fee              (Amazon refunds 80%)
                    + returns processing fee          (that region-month's real
                      "FBA Returns Processing Fee" total / returned units)
                    - landed cost if SELLABLE         (unit back in stock)
FBA storage: fba_storage_monthly (Amazon's monthly fee per ASIN x FC x country),
  prorated by the days of each month inside the range.
AWD (USA only - EU AWD fees aren't visible in settlements): each month's
  "AWD Fee" total, split over ASINs by AWD->FBA transferred units x item volume
  that month (fba_shipments is_awd_transfer); a month without transfers falls
  back to US units sold x volume. Prorated like storage.
Lagerpark (per the user 2026-11-03, "A"): the EU 3PL warehouse in Germany that
  feeds Pan-EU FBA. Its invoices of the 365 days up to the range end (without
  VAT, which is deductible) / the package volume of every unit sold in the EU
  stores (not UK) in the same 365 days = cost per unit of volume; each EU unit
  sold in the range carries that x its package volume (sku_statistics pkg_*_cm,
  else its section's median). Added to "storage" and shown as "lagerpark"."""
import calendar
from collections import defaultdict
from datetime import date

from AsinAnalytics import pb_list_all

LAGERPARK_MPS = {"de", "fr", "it", "es", "nl", "be", "ie", "se", "pl"}
_lg_cache = {}

RETURN_REGION = {"usa": "usa", "ca": "usa", "mex": "usa"}  # everything else -> "eu"
STORAGE_COUNTRY = {"usa": "US", "ca": "CA", "mex": "MX", "uk": "GB", "de": "DE", "fr": "FR", "it": "IT",
                   "es": "ES", "nl": "NL", "be": "BE", "ie": "IE", "se": "SE", "pl": "PL"}
FEE_REGION = {"usa": "usa", "ca": "usa"}  # amazon_expense_events marketplace -> returns region; rest -> eu
COUNTRY_MP = {v: k for k, v in STORAGE_COUNTRY.items()}
CURRENCY_OF = {"usa": "USD", "ca": "CAD", "eu": "EUR", "uk": "GBP", "se": "SEK", "pl": "PLN"}


def _months(start, end):
    y, m = int(start[:4]), int(start[5:7])
    while f"{y:04d}-{m:02d}" <= end[:7]:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def _overlap(y, m, start, end):
    """Share of month y-m that lies inside [start, end] (ISO dates, inclusive)."""
    days = calendar.monthrange(y, m)[1]
    a = max(date(y, m, 1), date.fromisoformat(start))
    b = min(date(y, m, days), date.fromisoformat(end))
    return max((b - a).days + 1, 0) / days


def package_volumes(token):
    """({asin: package volume in litres}, {section: median})."""
    from ProductCost import _section_of_group
    dims = {r["sku"]: r for r in pb_list_all(token, "sku_statistics", "",
                                              fields="sku,pkg_length_cm,pkg_width_cm,pkg_height_cm")
            if r.get("pkg_length_cm") and r.get("pkg_width_cm") and r.get("pkg_height_cm")}
    vol, by_sec, sec_of = {}, defaultdict(list), {}
    for r in pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,sku,group"):
        sec_of[r["asin"]] = _section_of_group(r.get("group")) or "other"
        d = dims.get(r["sku"])
        if d and r["asin"] not in vol:
            vol[r["asin"]] = d["pkg_length_cm"] * d["pkg_width_cm"] * d["pkg_height_cm"] / 1000
            by_sec[sec_of[r["asin"]]].append(vol[r["asin"]])
    med = {k: sorted(v)[len(v) // 2] for k, v in by_sec.items()}
    med["other"] = sorted(x for v in by_sec.values() for x in v)[sum(len(v) for v in by_sec.values()) // 2] if by_sec else 5.0
    return {a: vol.get(a) or med.get(sec_of.get(a, "other")) or med["other"] for a in sec_of}, med


def lagerpark_rate(token, end, usd):
    """USD per litre of package volume sold in the EU stores, 365 days up to `end`."""
    key = end[:7]
    if key in _lg_cache:
        return _lg_cache[key]
    from datetime import timedelta
    since = (date.fromisoformat(end) - timedelta(days=365)).isoformat()
    cost = 0.0
    for d in pb_list_all(token, "wise_documents", 'stored_path ~ "agerpark" && kind = "invoice"', fields="extracted"):
        ex = d.get("extracted") or {}
        if since < (ex.get("invoice_date") or "") <= end:
            cost += usd(float(ex.get("total") or 0) - float(ex.get("tax_amount") or 0), (ex.get("currency") or "EUR").upper())
    vols, _med = package_volumes(token)
    sold = 0.0
    flt = " || ".join(f'marketplace = "{m}"' for m in sorted(LAGERPARK_MPS))
    for l in pb_list_all(token, "order_items", f'purchase_date > "{since}" && purchase_date <= "{end}" && ({flt}) '
                                               f'&& order_status != "Cancelled"', fields="asin,qty"):
        if l.get("asin"):
            sold += (l.get("qty") or 0) * vols.get(l["asin"], _med.get("other", 5.0))
    rate = (cost / sold if sold else 0.0, vols, _med.get("other", 5.0), cost)
    _lg_cache[key] = rate
    return rate


def extra_costs(token, start, end, atomic, fee_lookup, landed_of, usd, vat_of, by_mp=False):
    """-> {asin: {returnedUnits, returnsCost, storage, awd}} in USD
    (by_mp=True: keyed (asin, marketplace) instead - storage by its country,
    AWD on "usa", returns on the order's marketplace).
    fee_lookup(mp, asin) -> {referralPct, fbaUsd}; landed_of(asin) -> USD or None;
    usd(amount, currency); vat_of(mp) -> rate."""
    out = defaultdict(lambda: defaultdict(float))
    regions = {RETURN_REGION.get(m, "eu") for m in atomic}

    # ---- returns ----
    rets = [r for r in pb_list_all(token, "amazon_returns",
                                   f'return_date >= "{start} 00:00:00" && return_date <= "{end} 23:59:59"',
                                   fields="order_id,asin,sku,quantity,detailed_disposition,return_date,marketplace")
            if r.get("marketplace") in regions]
    # processing fee per returned unit, per region-month (all products' returns that month)
    units_rm = defaultdict(float)
    for r in rets:
        units_rm[(r["marketplace"], r["return_date"][:7])] += float(r.get("quantity") or 1)
    fee_rm = defaultdict(float)
    ym = {f"{y:04d}-{m:02d}" for y, m in _months(start, end)}
    for e in pb_list_all(token, "amazon_expense_events", 'category = "FBA Returns Processing Fee"',
                         fields="amount,marketplace,year,month"):
        key = f"{int(e['year']):04d}-{int(e['month']):02d}"
        if key not in ym:
            continue
        fee_rm[(FEE_REGION.get(e["marketplace"], "eu"), key)] += -usd(e.get("amount") or 0,
                                                                      CURRENCY_OF.get(e["marketplace"], "EUR"))
    order_ids = sorted({r["order_id"] for r in rets if r.get("order_id")})
    lines = {}
    for i in range(0, len(order_ids), 80):
        chunk = order_ids[i:i + 80]
        flt = " || ".join(f'order_id = "{o}"' for o in chunk)
        for l in pb_list_all(token, "order_items", f"({flt})", fields="order_id,asin,qty,net_amount,currency,marketplace"):
            if l.get("qty"):
                lines[(l["order_id"], l["asin"])] = l
    for r in rets:
        asin = r.get("asin")
        qty = float(r.get("quantity") or 1)
        line = lines.get((r.get("order_id"), asin))
        if line:
            mp = line["marketplace"]
            if mp not in atomic:
                continue
            gross = usd(line["net_amount"] / line["qty"], line["currency"] or "USD")
        else:
            mp = "usa" if r["marketplace"] == "usa" else "de"
            if mp not in atomic:
                continue
            gross = None  # order before 2025 / not stored: price unknown
        f = fee_lookup(mp, asin)
        landed = landed_of(asin)
        per_fee = fee_rm[(r["marketplace"], r["return_date"][:7])] / units_rm[(r["marketplace"], r["return_date"][:7])]
        cost = per_fee
        if gross is not None:
            cost += gross / (1 + vat_of(mp)) - 0.8 * f["referralPct"] * gross
        if landed and (r.get("detailed_disposition") or "").upper() == "SELLABLE":
            cost -= landed
        o = out[(asin, mp) if by_mp else asin]
        o["returnedUnits"] += qty
        o["returnsCost"] += cost * qty

    # ---- FBA storage ----
    countries = {STORAGE_COUNTRY[m] for m in atomic if m in STORAGE_COUNTRY}
    months = list(_months(start, end))
    mfilter = " || ".join(f'month = "{y:04d}-{m:02d}"' for y, m in months)
    volume = {}
    for s in pb_list_all(token, "fba_storage_monthly", f"({mfilter})",
                         fields="month,country,asin,fee,currency,item_volume,volume_units,region"):
        if s["region"] == "usa" and s.get("item_volume"):
            volume[s["asin"]] = s["item_volume"]
        if s["country"] not in countries:
            continue
        share = _overlap(int(s["month"][:4]), int(s["month"][5:7]), start, end)
        k = (s["asin"], COUNTRY_MP.get(s["country"], "de")) if by_mp else s["asin"]
        out[k]["storage"] += usd(s.get("fee") or 0, s.get("currency") or "USD") * share

    # ---- AWD (USA only) ----
    if "usa" in atomic:
        awd_fee = defaultdict(float)
        for e in pb_list_all(token, "amazon_expense_events", 'category = "AWD Fee" && marketplace = "usa"',
                             fields="amount,year,month"):
            awd_fee[(int(e["year"]), int(e["month"]))] += -(e.get("amount") or 0)
        sku_asin = {r["sku"]: r["asin"] for r in pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="sku,asin")}
        moved = defaultdict(lambda: defaultdict(float))
        for sh in pb_list_all(token, "fba_shipments", 'region = "usa"', fields="created_date,is_awd_transfer,items"):
            if str(sh.get("is_awd_transfer")).lower() != "true" or not sh.get("created_date"):
                continue
            items = sh["items"]
            if isinstance(items, str):
                import ast
                items = ast.literal_eval(items)
            for it in items or []:
                asin = sku_asin.get(it.get("sku")) or it.get("fnsku")
                moved[(int(sh["created_date"][:4]), int(sh["created_date"][5:7]))][asin] += float(it.get("shipped") or 0)
        for y, m in months:
            total = awd_fee.get((y, m), 0)
            if not total:
                continue
            weights = {a: q * volume.get(a, 0.05) for a, q in moved.get((y, m), {}).items()}
            if not weights:
                last = calendar.monthrange(y, m)[1]
                for l in pb_list_all(token, "order_items",
                                     f'marketplace = "usa" && purchase_date >= "{y:04d}-{m:02d}-01" && '
                                     f'purchase_date <= "{y:04d}-{m:02d}-{last:02d}" && order_status != "Cancelled"',
                                     fields="asin,qty"):
                    weights[l["asin"]] = weights.get(l["asin"], 0) + (l.get("qty") or 0) * volume.get(l["asin"], 0.05)
            wsum = sum(weights.values())
            if not wsum:
                continue
            share = _overlap(y, m, start, end)
            for a, w in weights.items():
                out[(a, "usa") if by_mp else a]["awd"] += total * share * w / wsum
    # ---- Lagerpark (EU 3PL) ----
    lg_mps = sorted(LAGERPARK_MPS & set(atomic))
    if lg_mps:
        rate, vols, vdef, _cost = lagerpark_rate(token, end, usd)
        if rate:
            flt = " || ".join(f'marketplace = "{m}"' for m in lg_mps)
            for l in pb_list_all(token, "order_items", f'purchase_date >= "{start}" && purchase_date <= "{end}" && ({flt}) '
                                                       f'&& order_status != "Cancelled"', fields="asin,marketplace,qty"):
                if not l.get("asin") or not l.get("qty"):
                    continue
                c = rate * vols.get(l["asin"], vdef) * l["qty"]
                o = out[(l["asin"], l["marketplace"]) if by_mp else l["asin"]]
                o["lagerpark"] += c
                o["storage"] += c
    return {a: {k: round(v, 2) for k, v in d.items()} for a, d in out.items()}
