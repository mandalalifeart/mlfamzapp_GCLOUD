"""Returns analysis (per the user 2026-11-02, plan OK'd): why do products come
back, and which ones come back too often.

Return rate per product per region (USA / EU / UK) = returned units ÷ units
sold over the same window (default the last 90 days - returns come weeks after
the sale, a single month alone is misleading). Region of a return = the
marketplace of its order (order_items); without a stored order, the returns
report's own region (usa / eu).

Flags (monthly check, 90 days ending last month's end):
  🔴 too many: >= 5 returns and (rate >= 2x the product type's average rate in
     that region, or rate >= 15% and >= 1.3x the type average)
  🟠 rising:   >= 5 returns and rate up >= 50% vs the 90 days before
"Why" theme per return: keywords in the customer comment (EN/DE/FR/IT/ES),
else the Amazon reason code. Keywords, not AI (free); the monthly check uses
Claude Haiku only to translate the few sample comments it shows.

Endpoints (local server, read only): GetReturnsAnalysis?start&end&region&type,
GetReturnsForProduct?asin&start&end&region. Monthly: SendReturnsMonthlyCheck
(3rd of the month)."""
import json
import os
import re
from collections import Counter, defaultdict
from datetime import date, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all
from FbaFees import product_type

GOAL_PCT = 10
REGION_OF_MP = {"usa": "USA", "ca": "USA", "mex": "USA", "uk": "UK"}  # rest of order_items -> EU

THEMES = [  # (theme, regex on the lower-case comment) - first match wins
    ("Thought it was stuffed / cover only",
     r"stuff|fill|insert|empty|only the (outer|cover)|cover only|just (a|the) cover|sleeve|füll|gefüllt|nur (der |die |ein )?(bezug|hülle)|"
     r"leer|rembour|garni|pas un coussin|vide|housse seule|imbott|riemp|vuot|solo (la )?fodera|relleno|vac[ií]o|solo (la )?funda"),
    ("Size", r"size|too (small|big|large|short|long)|tiny|smaller|bigger|größe|klein|groß|zu kurz|zu lang|passt nicht|taille|petit|"
             r"trop grand|dimension|misura|piccol|grande|tamaño|pequeñ|medida|fit\b"),
    ("Colour / looks different from photo",
     r"colou?r|farbe|couleur|colore|picture|photo|pictured|image|bild|foto|looks? different|pattern|muster|motif|print|design"),
    ("Quality / defect",
     r"defect|broken|hole|torn|rip|stain|dirty|zip|seam|smell|cheap|poor quality|bad quality|quality|qualit|calidad|kaputt|loch|"
     r"fleck|riss|naht|reißverschluss|geruch|déchir|trou|tache|fermeture|odeur|couture|cerniera|buco|macchia|strapp|roto|agujero|"
     r"mancha|cremallera|olor|used|gebraucht|usagé|usato|usado"),
    ("Fabric / feel", r"fabric|material|stoff|tissu|tessuto|tela|thin|dünn|fin\b|sottile|delgad|transparent|see.?through|rough|feel|velvet"),
    ("Delivery / arrived late or damaged", r"late|arriv|deliver|lieferung|zu spät|livraison|retard|consegna|ritardo|entrega|tarde|package|paket|colis"),
    ("Changed mind / not needed",
     r"changed mind|change of mind|no longer|not needed|needs changed|mistake|by accident|duplicate|better price|cheaper|"
     r"nicht mehr|versehentlich|doppelt|plus besoin|erreur|non serve|per errore|ya no|por error|gift|geschenk|cadeau"),
]
REASON_THEME = {
    "DEFECTIVE": "Quality / defect", "QUALITY_UNACCEPTABLE": "Quality / defect", "DAMAGED_BY_FC": "Quality / defect",
    "DAMAGED_BY_CARRIER": "Delivery / arrived late or damaged", "MISSED_ESTIMATED_DELIVERY": "Delivery / arrived late or damaged",
    "UNDELIVERABLE_REFUSED": "Delivery / arrived late or damaged", "UNDELIVERABLE_UNKNOWN": "Delivery / arrived late or damaged",
    "UNDELIVERABLE_INSUFFICIENT_ADDRESS": "Delivery / arrived late or damaged", "UNDELIVERABLE_FAILED_DELIVERY_ATTEMPTS": "Delivery / arrived late or damaged",
    "APPAREL_TOO_SMALL": "Size", "APPAREL_TOO_LARGE": "Size", "POOR_FIT": "Size", "NOT_COMPATIBLE": "Size",
    "DID_NOT_LIKE_FABRIC": "Fabric / feel", "APPAREL_STYLE": "Colour / looks different from photo",
    "UNWANTED_ITEM": "Changed mind / not needed", "MISORDERED": "Changed mind / not needed",
    "ORDERED_WRONG_ITEM": "Changed mind / not needed", "FOUND_BETTER_PRICE": "Changed mind / not needed",
    "EXTRA_ITEM": "Changed mind / not needed", "NOT_AS_DESCRIBED": "Not as described (no detail)",
    "NO_REASON_GIVEN": "No reason given",
}
THEME_ACTION = {
    "Thought it was stuffed / cover only": "make 'cover only, no filling' clearer: title start, main/2nd image text, first bullet",
    "Size": "check the size in the title/bullets + a size image with a person/sofa for scale",
    "Colour / looks different from photo": "new photos in daylight, a 'real colour' image",
    "Quality / defect": "check that batch with the supplier (Malani/Acacia)",
    "Fabric / feel": "describe the fabric honestly (thickness, feel) + close-up image",
    "Delivery / arrived late or damaged": "Amazon's side - nothing to change on the listing",
    "Changed mind / not needed": "normal level, no listing fix",
}


def theme_of(comment, reason):
    c = (comment or "").lower()
    if c:
        for name, rx in THEMES:
            if re.search(rx, c):
                return name
    return REASON_THEME.get((reason or "").upper(), "Other")


def _mapping(token):
    return {r["asin"]: r for r in pb_list_all(token, "asin_group_mapping", 'asin != ""', fields="asin,sku,group")}


def _returns(token, start, end):
    return pb_list_all(token, "amazon_returns", f'return_date >= "{start} 00:00:00" && return_date <= "{end} 23:59:59"',
                       fields="return_date,order_id,asin,sku,quantity,reason,customer_comments,detailed_disposition,marketplace,status")


def _order_regions(token, order_ids):
    out = {}
    ids = sorted({o for o in order_ids if o})
    for i in range(0, len(ids), 80):
        flt = " || ".join(f'order_id = "{o}"' for o in ids[i:i + 80])
        for l in pb_list_all(token, "order_items", f"({flt})", fields="order_id,marketplace"):
            out[l["order_id"]] = REGION_OF_MP.get(l["marketplace"], "EU")
    return out


def _sold(token, start, end):
    """(asin, region) -> units sold (order_items, non-cancelled, no MCF)."""
    out = defaultdict(float)
    for l in pb_list_all(token, "order_items", f'purchase_date >= "{start}" && purchase_date <= "{end}" '
                                               f'&& order_status != "Cancelled" && marketplace != "mcf"',
                         fields="asin,marketplace,qty"):
        if l.get("asin") and l.get("qty"):
            out[(l["asin"], REGION_OF_MP.get(l["marketplace"], "EU"))] += l["qty"]
    return out


def _enrich(token, rets):
    regions = _order_regions(token, [r.get("order_id") for r in rets])
    for r in rets:
        r["region"] = regions.get(r.get("order_id")) or ("USA" if r.get("marketplace") == "usa" else "EU")
        r["theme"] = theme_of(r.get("customer_comments"), r.get("reason"))
        r["qty"] = float(r.get("quantity") or 1)
    return rets


def analysis(start, end, region="", ptype="", token=None):
    token = token or pb_authenticate()
    mapping = _mapping(token)
    rets = _enrich(token, _returns(token, start, end))
    sold = _sold(token, start, end)

    def info(asin):
        g = mapping.get(asin) or {}
        return g.get("sku") or asin, product_type(g.get("sku") or "", g.get("group")), (g.get("group") or "").upper()

    prods = defaultdict(lambda: {"returned": 0.0, "reasons": Counter(), "themes": Counter(), "sellable": 0.0})
    for r in rets:
        sku, typ, grp = info(r["asin"])
        if grp.startswith("IGNORE") or (region and r["region"] != region) or (ptype and typ != ptype):
            continue
        p = prods[(r["asin"], r["region"])]
        p["returned"] += r["qty"]
        p["reasons"][r.get("reason") or "?"] += r["qty"]
        p["themes"][r["theme"]] += r["qty"]
        if (r.get("detailed_disposition") or "").upper() == "SELLABLE":
            p["sellable"] += r["qty"]
    for (asin, reg), u in sold.items():
        sku, typ, grp = info(asin)
        if grp.startswith("IGNORE") or (region and reg != region) or (ptype and typ != ptype):
            continue
        prods[(asin, reg)]["sold"] = u
    # type averages per region
    tsum = defaultdict(lambda: [0.0, 0.0])
    for (asin, reg), p in prods.items():
        t = tsum[(info(asin)[1], reg)]
        t[0] += p["returned"]
        t[1] += p.get("sold", 0)
    rows = []
    for (asin, reg), p in prods.items():
        sku, typ, _g = info(asin)
        sold_u = p.get("sold", 0)
        tr, ts = tsum[(typ, reg)]
        rate = p["returned"] / sold_u * 100 if sold_u else None
        trate = tr / ts * 100 if ts else None
        rows.append({"asin": asin, "sku": sku, "type": typ, "region": reg, "sold": sold_u, "returned": p["returned"],
                     "rate": round(rate, 1) if rate is not None else None,
                     "typeRate": round(trate, 1) if trate is not None else None,
                     "sellable": p["sellable"], "topReason": p["reasons"].most_common(1)[0][0] if p["reasons"] else "",
                     "topTheme": p["themes"].most_common(1)[0][0] if p["themes"] else "",
                     "themes": dict(p["themes"]), "reasons": dict(p["reasons"])})
    rows.sort(key=lambda r: (-r["returned"], r["sku"]))
    # themes per month + reason x type matrix + overall per region
    by_month = defaultdict(Counter)
    matrix = defaultdict(Counter)
    for r in rets:
        sku, typ, grp = info(r["asin"])
        if grp.startswith("IGNORE") or (region and r["region"] != region) or (ptype and typ != ptype):
            continue
        by_month[r["return_date"][:7]][r["theme"]] += r["qty"]
        matrix[r.get("reason") or "?"][typ] += r["qty"]
    overall = {}
    for reg in ("USA", "EU", "UK"):
        ret_u = sum(p["returned"] for p in rows if p["region"] == reg)
        sold_u = sum(p["sold"] for p in rows if p["region"] == reg)
        if ret_u or sold_u:
            overall[reg] = {"returned": ret_u, "sold": sold_u, "rate": round(ret_u / sold_u * 100, 1) if sold_u else None}
    return {"start": start, "end": end, "region": region, "type": ptype, "goalPct": GOAL_PCT, "overall": overall,
            "products": rows, "themesByMonth": {m: dict(c) for m, c in sorted(by_month.items())},
            "matrix": {k: dict(v) for k, v in matrix.items()},
            "types": sorted({r["type"] for r in rows}), "themeActions": THEME_ACTION}


def product_returns(asin, start, end, region="", token=None):
    token = token or pb_authenticate()
    rets = [r for r in _enrich(token, [r for r in _returns(token, start, end) if r.get("asin") == asin])
            if not region or r["region"] == region]
    rets.sort(key=lambda r: r["return_date"], reverse=True)
    keep = ("return_date", "order_id", "sku", "qty", "reason", "customer_comments", "detailed_disposition", "region", "theme")
    return {"asin": asin, "returns": [{k: r.get(k) for k in keep} for r in rets]}


# ---------------- monthly check ----------------

def _translate(comments):
    """English versions of a few comments (Claude Haiku; on any error the originals)."""
    if not comments or not os.environ.get("ANTHROPIC_API_KEY"):
        return comments
    try:
        import anthropic
        msg = anthropic.Anthropic().messages.create(
            model="claude-haiku-5-5", max_tokens=4096,
            messages=[{"role": "user", "content": "Translate each line to short plain English (keep English lines as they are). "
                                                  "Return ONLY a JSON array of strings, same order.\n" + json.dumps(comments, ensure_ascii=False)}])
        text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
        out = json.loads(text[text.index("["): text.rindex("]") + 1])
        return out if len(out) == len(comments) else comments
    except Exception:
        return comments


def monthly_text(today=None):
    today = today or date.today()
    end = today.replace(day=1) - timedelta(days=1)
    start = end - timedelta(days=89)
    p_end = start - timedelta(days=1)
    p_start = p_end - timedelta(days=89)
    token = pb_authenticate()
    cur = analysis(start.isoformat(), end.isoformat(), token=token)
    prev = {(r["asin"], r["region"]): r for r in analysis(p_start.isoformat(), p_end.isoformat(), token=token)["products"]}
    lines = [f"↩️ Returns check - 90 days {start} → {end}", ""]
    for reg, o in cur["overall"].items():
        mark = "🔴" if o["rate"] and o["rate"] > GOAL_PCT else "🟢"
        lines.append(f"{mark} {reg}: {o['returned']:.0f} returned / {o['sold']:.0f} sold = {o['rate']}% (goal {GOAL_PCT}%)")
    themes = Counter()
    for r in cur["products"]:
        themes.update(r["themes"])
    prev_themes = Counter()
    for r in prev.values():
        prev_themes.update(r["themes"])
    lines.append("\nWhy (themes, units; vs the 90 days before):")
    for t, n in themes.most_common(6):
        lines.append(f"• {t}: {n:.0f} (before {prev_themes.get(t, 0):.0f})")
    red, orange = [], []
    for r in cur["products"]:
        if r["returned"] < 5 or r["rate"] is None:
            continue
        # >= 15% only counts when also clearly above its type (pareos run ~18-22%
        # as a type - apparel - so 15% alone would flag every pareo).
        if (r["typeRate"] and r["rate"] >= 2 * r["typeRate"]) or \
                (r["rate"] >= 15 and (not r["typeRate"] or r["rate"] >= 1.3 * r["typeRate"])):
            red.append(r)
            continue
        p = prev.get((r["asin"], r["region"]))
        if p and p.get("rate") and r["rate"] >= 1.5 * p["rate"]:
            orange.append(dict(r, prevRate=p["rate"]))
    red.sort(key=lambda r: -r["returned"])
    lines.append(f"\n🔴 Too many returns: {len(red)}")
    month_rets = None
    for r in red[:15]:
        lines.append(f"🔴 {r['region']} {r['sku']} ({r['type']}): {r['returned']:.0f} of {r['sold']:.0f} = {r['rate']}% "
                     f"(type {r['typeRate']}%) - why: " + ", ".join(f"{t} {n:.0f}" for t, n in
                                                                    Counter(r["themes"]).most_common(3)))
        if month_rets is None:
            month_rets = _enrich(token, _returns(token, start.isoformat(), end.isoformat()))
        comments = [x["customer_comments"] for x in month_rets
                    if x["asin"] == r["asin"] and x["region"] == r["region"] and (x.get("customer_comments") or "").strip()]
        comments = list(dict.fromkeys(comments))[:3]
        for c in _translate(comments):
            lines.append(f"   “{c[:160]}”")
        # action from the biggest theme we can act on (not changed mind / no reason / delivery)
        fixable = [t for t, _n in Counter(r["themes"]).most_common() if t in THEME_ACTION
                   and t not in ("Changed mind / not needed", "Delivery / arrived late or damaged")]
        if fixable:
            lines.append(f"   ➜ {fixable[0]}: {THEME_ACTION[fixable[0]]}")
    lines.append(f"\n🟠 Rising (rate +50% vs the 90 days before): {len(orange)}")
    for r in orange[:10]:
        lines.append(f"🟠 {r['region']} {r['sku']}: {r['prevRate']}% → {r['rate']}% ({r['returned']:.0f} returns, top: {r['topTheme']})")
    lines.append("\nDetails: https://mlfamzappfire.web.app/returns")
    return "\n".join(lines)


def _cors(body, status=200):
    return json.dumps(body), status, {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}


def _range(args):
    end = args.get("end") or (date.today() - timedelta(days=1)).isoformat()
    start = args.get("start") or (date.fromisoformat(end) - timedelta(days=89)).isoformat()
    return start, end


def GetReturnsAnalysis(request):
    args = getattr(request, "args", {}) or {}
    try:
        start, end = _range(args)
        return _cors(analysis(start, end, args.get("region") or "", args.get("type") or ""))
    except Exception as exc:
        return _cors({"error": str(exc)}, 500)


def GetReturnsForProduct(request):
    args = getattr(request, "args", {}) or {}
    try:
        start, end = _range(args)
        return _cors(product_returns(args.get("asin") or "", start, end, args.get("region") or ""))
    except Exception as exc:
        return _cors({"error": str(exc)}, 500)


def SendReturnsMonthlyCheck(request):
    from NotificationRouting import notify
    args = getattr(request, "args", {}) or {}
    try:
        text = monthly_text(date.fromisoformat(args["today"]) if args.get("today") else None)
        if str(args.get("dry")) != "1":
            notify("returns-monthly-check", "amzbot", text)
        return _cors({"text": text})
    except Exception as exc:
        notify("returns-monthly-check", "amzbot", f"🚨 Returns check failed: {exc}", is_error=True)
        return _cors({"error": str(exc)}, 500)
