"""FBA fee monitor (per the user 2026-11-02, plan OK'd).

Source: Amazon's "FBA fee preview" report GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA -
US creds (stores US/CA/MX) and EU creds (GB/DE/FR/IT/ES/NL/BE/IE/PL/SE/AE/SA):
per SKU and store the package dimensions + weight Amazon measured, the size
tier and the FBA fee per unit (US: expected-fulfillment-fee-per-unit, EU:
expected-domestic-fulfilment-fee-per-unit). One snapshot per week in
`fba_fee_snapshots` (delete-then-insert per week).

Weekly alert (Monday digest) vs the previous snapshot: FBA fee moved >= 5% and
>= 0.10 (x SCALE for MX/SE/PL/...), size tier changed, a side moved > 0.5 cm,
or the weight moved > 5%.

"Looks too big" per store, against products of the same type (pareo / pouf
cover / pillow cover / stuffed pouf / outdoor cover / stool), types with >= 3
products:
  🔴 fee > type median x 1.15 and >= 0.30 more (x SCALE), or weight / any
     side (sorted) > 1.3x the type median -> probably measured wrong
  🟠 size tier differs from the type's most common tier
Extra per year = (fee - type median) x units sold in that marketplace in the
last 12 months (sku_sales by ASIN).

Endpoints (local server): UpdateFbaFees (cron Monday, dry=1 = no Telegram),
GetFbaFees?store=US (page /fba-fees), GetFbaFeeHistory?asin=&store=."""
import csv
import io
import json
import statistics
from collections import Counter, defaultdict
from datetime import date, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all
from UpdateSkuSalesMonth import pb_batch
from WeeklyChecks import run_report

COLLECTION = "fba_fee_snapshots"
SOURCES = ("US", "DE")  # US creds -> US/CA/MX, EU creds -> all EU stores
SCALE = {"MX": 20, "SE": 11, "PL": 4.3, "CA": 1.4, "AE": 3.7, "SA": 3.7}
SALES_MP = {"US": "usa", "CA": "ca", "MX": "mex", "GB": "uk", "DE": "de", "FR": "fr", "IT": "it", "ES": "es",
            "NL": "nl", "BE": "be", "IE": "ie", "PL": "pl", "SE": "se"}
STORE_ORDER = ["US", "CA", "MX", "GB", "DE", "FR", "IT", "ES", "NL", "BE", "IE", "PL", "SE", "AE", "SA"]
TO_CM = {"inches": 2.54, "centimeters": 1.0}
TO_G = {"pounds": 453.592, "grams": 1.0, "kilograms": 1000.0, "ounces": 28.3495}


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


BIG_ITEMS = {"coverpoufrectpatch111": "large patchwork pouf cover"}


def product_type(sku, group):
    s, g = (sku or "").lower(), (group or "").upper()
    if "_set" in s:
        return "pareo set" if "pareo" in s else "set"
    if "PAREO" in g or "pareo" in s or "sarong" in s:
        return "pareo"
    if "cube" in s:
        return "cube cover"
    if "stuffed" in s:
        return "stuffed pouf"
    if "outdoor" in s:
        return "outdoor cover"
    if "stool" in s:
        return "stool"
    if s.startswith("coverpillow") or "pillow" in s:
        return "pillow cover"
    if s in BIG_ITEMS:  # bigger/heavier by design (per the user 2026-11-12): own type, not vs normal covers
        return BIG_ITEMS[s]
    if s.startswith("coverpouf") or "pouf" in s:
        return "pouf cover"
    return g.lower() or "other"


def parse(text, week):
    rows = []
    for r in csv.DictReader(io.StringIO(text or ""), delimiter="\t"):
        fee = _f(r.get("expected-fulfillment-fee-per-unit"))
        if fee is None:
            fee = _f(r.get("expected-domestic-fulfilment-fee-per-unit"))
        unit_d, unit_w = r.get("unit-of-dimension") or "", r.get("unit-of-weight") or ""
        sides = [_f(r.get(k)) for k in ("longest-side", "median-side", "shortest-side")]
        w = _f(r.get("item-package-weight"))
        rows.append({
            "week": week, "store": r.get("amazon-store") or "", "sku": r.get("sku") or "", "asin": r.get("asin") or "",
            "price": _f(r.get("your-price")), "currency": r.get("currency") or "",
            "longest_cm": round(sides[0] * TO_CM.get(unit_d, 1), 2) if sides[0] is not None else None,
            "median_cm": round(sides[1] * TO_CM.get(unit_d, 1), 2) if sides[1] is not None else None,
            "shortest_cm": round(sides[2] * TO_CM.get(unit_d, 1), 2) if sides[2] is not None else None,
            "weight_g": round(w * TO_G.get(unit_w, 1), 1) if w is not None else None,
            "size_tier": r.get("product-size-tier") or r.get("product-size-weight-band") or "",
            "fba_fee": fee, "referral_fee": _f(r.get("estimated-referral-fee-per-unit")),
        })
    return [r for r in rows if r["store"] and r["asin"]]


def pull(week=None):
    week = week or (date.today() - timedelta(days=date.today().weekday())).isoformat()
    rows = []
    for src in SOURCES:
        rows += parse(run_report(src, "GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA", wait_minutes=25), week)
    if not rows:
        raise RuntimeError("fee preview reports came back empty")
    token = pb_authenticate()
    old = pb_list_all(token, COLLECTION, f'week = "{week}"', fields="id")
    ops = [{"method": "DELETE", "url": f"/api/collections/{COLLECTION}/records/{r['id']}"} for r in old]
    ops += [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": b} for b in rows]
    for i in range(0, len(ops), 50):
        pb_batch(token, ops[i:i + 50])
    return week, rows


# ---------------- analysis ----------------

def _mapping(token):
    return {r["asin"]: r for r in pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,sku,group")}


def _units_12m(token):
    t = date.today()
    out = defaultdict(float)
    for r in pb_list_all(token, "sku_sales", f"year >= {t.year - 1}", fields="ASIN,marketplace,quantity,year,month"):
        if (r["year"], r["month"]) >= (t.year - 1, t.month) and (r["year"], r["month"]) < (t.year, t.month):
            out[(r["ASIN"], r["marketplace"])] += r.get("quantity") or 0
    return out


def analyse(rows, mapping, units):
    """rows of ONE week -> per row: type, typeMedian*, flags, extraPerYear."""
    by_store_asin = {}
    for r in rows:
        if r["fba_fee"] is None:
            continue
        g = mapping.get(r["asin"]) or {}
        if (g.get("group") or "").upper().startswith("IGNORE"):
            continue
        k = (r["store"], r["asin"])
        if k not in by_store_asin:  # several SKUs on one ASIN: same package, keep one
            by_store_asin[k] = dict(r, mainSku=g.get("sku") or r["sku"], type=product_type(g.get("sku") or r["sku"], g.get("group")))
    groups = defaultdict(list)
    for r in by_store_asin.values():
        groups[(r["store"], r["type"])].append(r)
    out = []
    for (store, typ), items in groups.items():
        sc = SCALE.get(store, 1)
        med = lambda key: statistics.median([x[key] for x in items if x.get(key)]) if any(x.get(key) for x in items) else None  # noqa: E731
        fee_m, w_m = med("fba_fee"), med("weight_g")
        sides_m = [med(k) for k in ("longest_cm", "median_cm", "shortest_cm")]
        tier_m = Counter(x["size_tier"] for x in items).most_common(1)[0][0]
        for r in items:
            # 🔴 only when the FEE is higher (money); size/weight outliers are
            # the explanation shown with it. 🟠 = other size tier, fee still normal.
            flags, level = [], ""
            if len(items) >= 3:
                if fee_m and r["fba_fee"] > fee_m * 1.15 and r["fba_fee"] - fee_m >= 0.30 * sc:
                    level = "red"
                    flags.append(f"fee {r['fba_fee']:.2f} vs type median {fee_m:.2f}")
                if r["size_tier"] != tier_m:
                    level = level or "orange"
                    flags.append(f"size tier {r['size_tier']} (most {typ}: {tier_m})")
                if level:
                    if w_m and r.get("weight_g") and r["weight_g"] > 1.3 * w_m:
                        flags.append(f"weight {r['weight_g']:.0f} g vs typical {w_m:.0f} g")
                    for name, k, m in zip(("longest", "middle", "shortest"), ("longest_cm", "median_cm", "shortest_cm"), sides_m):
                        if m and r.get(k) and r[k] > 1.3 * m and r[k] - m >= 1:
                            flags.append(f"{name} side {r[k]:.1f} cm vs typical {m:.1f} cm")
            u = units.get((r["asin"], SALES_MP.get(store, "")), 0)
            extra = (r["fba_fee"] - fee_m) * u if fee_m and r["fba_fee"] > fee_m else 0.0
            out.append(dict(r, typeCount=len(items), typeFee=fee_m, typeWeight=w_m, typeSides=sides_m, typeTier=tier_m,
                            units12m=u, extraPerYear=round(extra, 2),
                            level=level, flags=flags))
    return out


def changes(new_rows, old_rows):
    old = {(r["store"], r["sku"]): r for r in old_rows}
    out = []
    for r in new_rows:
        o = old.get((r["store"], r["sku"]))
        if not o or r["fba_fee"] is None or o.get("fba_fee") is None:
            continue
        sc = SCALE.get(r["store"], 1)
        why = []
        d = r["fba_fee"] - o["fba_fee"]
        if abs(d) >= 0.10 * sc and o["fba_fee"] and abs(d) / o["fba_fee"] >= 0.05:
            why.append(f"fee {o['fba_fee']:.2f} → {r['fba_fee']:.2f} {r['currency']}")
        if (o.get("size_tier") or "") != (r.get("size_tier") or ""):
            why.append(f"tier {o.get('size_tier')} → {r.get('size_tier')}")
        for k in ("longest_cm", "median_cm", "shortest_cm"):
            if o.get(k) and r.get(k) and abs(r[k] - o[k]) > 0.5:
                why.append(f"{k[:-3]} {o[k]:.1f} → {r[k]:.1f} cm")
        if o.get("weight_g") and r.get("weight_g") and abs(r["weight_g"] - o["weight_g"]) / o["weight_g"] > 0.05:
            why.append(f"weight {o['weight_g']:.0f} → {r['weight_g']:.0f} g")
        if why:
            out.append({"store": r["store"], "sku": r["sku"], "asin": r["asin"], "why": why, "up": d > 0})
    return out


def _weeks(token):
    return sorted({r["week"] for r in pb_list_all(token, COLLECTION, "", fields="week")}, reverse=True)


def _week_rows(token, week):
    return pb_list_all(token, COLLECTION, f'week = "{week}"')


def report_text(week, analysed, chg):
    lines = [f"📦 FBA fees - week of {week}"]
    lines.append(f"\nChanges vs last week: {len(chg)}")
    for c in sorted(chg, key=lambda c: (c["store"], c["sku"]))[:40]:
        lines.append(f"{'🔴' if c['up'] else '🟢'} {c['store']} {c['sku']}: {'; '.join(c['why'])}")
    if not chg:
        lines.append("none")
    red = sorted([r for r in analysed if r["level"] == "red"], key=lambda r: -r["extraPerYear"])
    orange = [r for r in analysed if r["level"] == "orange"]
    lines.append(f"\nLooks too big (vs same product type in the same marketplace): {len(red)} 🔴, {len(orange)} 🟠")
    for r in red[:25]:
        lines.append(f"🔴 {r['store']} {r['mainSku']} ({r['type']}): {'; '.join(r['flags'])}"
                     + (f" - ≈{r['extraPerYear']:.0f} {r['currency']}/year extra" if r["extraPerYear"] else ""))
    lines.append("Full list: https://mlfamzappfire.web.app/fba-fees")
    return "\n".join(lines)


def UpdateFbaFees(request):
    from NotificationRouting import notify
    args = getattr(request, "args", {}) or {}
    try:
        week, rows = pull(args.get("week"))
        token = pb_authenticate()
        prev = [w for w in _weeks(token) if w < week]
        chg = changes(rows, _week_rows(token, prev[0])) if prev else []
        analysed = analyse(rows, _mapping(token), _units_12m(token))
        text = report_text(week, analysed, chg)
        if not prev:
            text += "\n(first snapshot - changes start next week)"
        if str(args.get("dry")) != "1":
            notify("fba-fees-weekly", "AmzBot", text)
        return json.dumps({"week": week, "rows": len(rows), "changes": len(chg),
                           "red": sum(1 for r in analysed if r["level"] == "red"), "text": text}), 200, \
            {"Content-Type": "application/json"}
    except Exception as exc:
        notify("fba-fees-weekly", "AmzBot", f"📦 FBA fee check failed: {exc}", is_error=True)
        return json.dumps({"error": str(exc)}), 500, {"Content-Type": "application/json"}


def _cors(body, status=200):
    return json.dumps(body), status, {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}


def GetFbaFees(request):
    args = getattr(request, "args", {}) or {}
    try:
        token = pb_authenticate()
        weeks = _weeks(token)
        if not weeks:
            return _cors({"weeks": [], "stores": [], "rows": []})
        week = args.get("week") or weeks[0]
        rows = _week_rows(token, week)
        analysed = analyse(rows, _mapping(token), _units_12m(token))
        chg = changes(rows, _week_rows(token, [w for w in weeks if w < week][0])) if any(w < week for w in weeks) else []
        stores = [s for s in STORE_ORDER if any(r["store"] == s for r in analysed)]
        store = args.get("store") or (stores[0] if stores else "US")
        sel = [r for r in analysed if r["store"] == store]
        rank = {"red": 0, "orange": 1, "": 2}
        sel.sort(key=lambda r: (rank[r["level"]], -r["extraPerYear"], r["type"], r["mainSku"]))
        keep = ("store", "sku", "mainSku", "asin", "type", "price", "currency", "fba_fee", "referral_fee", "size_tier",
                "longest_cm", "median_cm", "shortest_cm", "weight_g", "typeCount", "typeFee", "typeWeight", "typeSides",
                "typeTier", "units12m", "extraPerYear", "level", "flags")
        counts = {s: {"red": sum(1 for r in analysed if r["store"] == s and r["level"] == "red"),
                      "orange": sum(1 for r in analysed if r["store"] == s and r["level"] == "orange")} for s in stores}
        return _cors({"week": week, "weeks": weeks, "stores": stores, "store": store, "counts": counts,
                      "rows": [{k: r.get(k) for k in keep} for r in sel],
                      "changes": [c for c in chg if c["store"] == store]})
    except Exception as exc:
        return _cors({"error": str(exc)}, 500)


def GetFbaFeeHistory(request):
    args = getattr(request, "args", {}) or {}
    try:
        token = pb_authenticate()
        rows = pb_list_all(token, COLLECTION, f'asin = "{args.get("asin", "")}" && store = "{args.get("store", "")}"',
                           fields="week,sku,fba_fee,size_tier,longest_cm,median_cm,shortest_cm,weight_g,currency")
        return _cors({"history": sorted(rows, key=lambda r: r["week"])})
    except Exception as exc:
        return _cors({"error": str(exc)}, 500)
