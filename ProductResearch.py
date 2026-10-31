"""New products research (2026-10-31, per the user): new fabric products the
Indian supplier can make, sold at $20 or more, also as bundles - mainly to
extend the mushroom collection (CoverPoufVMagic535) - that should sell at
least as well as our poufs.

RunProductResearch (Amazon US, read-only): for every idea in IDEAS it runs a
catalog search per keyword (top 20 results, relevance order) and reads each
result's Best Seller Rank in Home & Kitchen (or whatever top category it sits
in) plus the competitive price. Benchmark = the same Home & Kitchen rank of
our own pouf ASINs (COVER_* / VELVET groups). Per idea it stores: how many of
the top results rank better than our best pouf / our median pouf, the best
rank, median competitor price, the top competitors, and the searches our own
ads already get for the idea's words (ads_search_term_stats, last 90 days).
No combined score on purpose - the raw numbers are shown side by side.

Results live in job_state `product_research`; the user's status/notes per
idea in job_state `product_research_status` (SetProductResearchStatus).
GetProductResearch serves both to the /product-research page."""
import json
import os
import re
import statistics
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from sp_api.api import CatalogItems, Products
from sp_api.base import Marketplaces

from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state, set_job_state
from WeeklyChecks import _json

US = Marketplaces.US
CRED = dict(refresh_token=os.environ.get("REFRESH_TOKEN_USA", ""), lwa_app_id=os.environ.get("CLIENT_ID_USA", ""),
            lwa_client_secret=os.environ.get("CLIENT_SECRET_USA", ""))
OUR_BRANDS = ("mandala life art", "boho paradise")
POUF_GROUP_PREFIXES = ("COVER_", "VELVET")
# Searches like "mushroom pillow cover" also return products whose COLOUR is
# "mushroom" (a brown/taupe) - only titles with a theme word and no colour
# use count as on-theme competitors.
THEME_WORDS = ("mushroom", "toadstool", "fungi", "woodland", "cottagecore", "fairy", "psychedelic", "groovy",
               "70s", "retro", "hippie", "trippy")
COLOUR_RE = re.compile(r"mushroom[ -]?(brown|colou?r|beige|gr[ae]y|taupe|tan|cream)|"
                       r"(brown|beige|gr[ae]y|taupe|tan|cream|light|dark)[ -]?mushroom", re.I)
STATUSES = ("idea", "researching", "sample", "ordered", "rejected")

# id, name, keywords (Amazon US searches), match = words that must all be in
# one of our ad search terms, price = suggested retail range, bundle, notes
# (what makes it a quality product from an Indian fabric supplier).
IDEAS = [
    {"id": "mushroom-pillow-cover", "name": "Mushroom embroidered velvet throw pillow cover 18x18",
     "keywords": ["mushroom pillow cover", "mushroom throw pillow"], "match": ["mushroom", "pillow"],
     "price": "$22-26 single", "bundle": "Set of 2 $39-44; pouf + 2 matching pillow covers",
     "notes": "Same velvet + embroidery as the Magic535 pouf, so it matches the pouf. Hidden zipper, cover only."},
    {"id": "mushroom-lumbar", "name": "Mushroom lumbar pillow cover 12x20",
     "keywords": ["mushroom lumbar pillow cover"], "match": ["mushroom", "lumbar"],
     "price": "$22-25", "bundle": "With 2 square covers as a sofa set",
     "notes": "Long panel lets the embroidery tell a scene (mushrooms + flowers + butterflies)."},
    {"id": "mushroom-tapestry", "name": "Mushroom wall tapestry (block print / embroidered cotton)",
     "keywords": ["mushroom tapestry"], "match": ["mushroom", "tapestry"],
     "price": "$24-35", "bundle": "Tapestry + pillow cover in the same print",
     "notes": "Hand block print on heavy cotton or velvet with embroidery stands out vs cheap polyester prints."},
    {"id": "mushroom-throw-blanket", "name": "Mushroom throw blanket (woven cotton or kantha)",
     "keywords": ["mushroom throw blanket", "mushroom blanket"], "match": ["mushroom", "blanket"],
     "price": "$39-55", "bundle": "Blanket + pillow cover",
     "notes": "Kantha quilt or jacquard cotton - premium vs fleece prints."},
    {"id": "mushroom-rug", "name": "Mushroom rug (tufted cotton / wool)",
     "keywords": ["mushroom rug"], "match": ["mushroom", "rug"],
     "price": "$45-90", "bundle": "Rug + pouf",
     "notes": "Hand tufted, the shape itself can be a mushroom. India is strong in tufted rugs."},
    {"id": "mushroom-table-runner", "name": "Mushroom table runner (block print cotton)",
     "keywords": ["mushroom table runner"], "match": ["mushroom", "runner"],
     "price": "$20-26", "bundle": "Runner + 4 napkins / placemats",
     "notes": "Cheap to make, light to ship, Thanksgiving/autumn demand."},
    {"id": "mushroom-tablecloth", "name": "Mushroom tablecloth (block print cotton)",
     "keywords": ["mushroom tablecloth"], "match": ["mushroom", "tablecloth"],
     "price": "$28-40", "bundle": "Tablecloth + napkins",
     "notes": "Block print on cotton, sizes for 4 and 6 seats."},
    {"id": "mushroom-curtains", "name": "Mushroom curtains (embroidered / printed cotton, pair)",
     "keywords": ["mushroom curtains"], "match": ["mushroom", "curtain"],
     "price": "$35-49 pair", "bundle": "Curtains + pillow cover",
     "notes": "Sizes per window length makes many SKUs - start with 63\" and 84\"."},
    {"id": "mushroom-quilt", "name": "Mushroom quilt / bedspread (kantha or embroidered)",
     "keywords": ["mushroom quilt", "mushroom bedding"], "match": ["mushroom", "quilt"],
     "price": "$69-110", "bundle": "Quilt + 2 shams",
     "notes": "High price per order; Queen and King sizes."},
    {"id": "mushroom-duvet", "name": "Mushroom duvet cover (cotton)",
     "keywords": ["mushroom duvet cover"], "match": ["mushroom", "duvet"],
     "price": "$59-89", "bundle": "Duvet cover + 2 pillow shams",
     "notes": "Cotton percale with block print."},
    {"id": "mushroom-shower-curtain", "name": "Mushroom shower curtain (cotton)",
     "keywords": ["mushroom shower curtain"], "match": ["mushroom", "shower"],
     "price": "$29-39", "bundle": "-",
     "notes": "Fabric (not plastic) shower curtain, block print."},
    {"id": "mushroom-tote", "name": "Mushroom embroidered tote bag (canvas / velvet)",
     "keywords": ["mushroom tote bag"], "match": ["mushroom", "tote"],
     "price": "$20-24", "bundle": "Tote + pareo (beach set)",
     "notes": "Borderline at $20; works better as a bundle add-on."},
    {"id": "mushroom-bean-bag", "name": "Mushroom bean bag chair cover",
     "keywords": ["mushroom bean bag chair", "mushroom chair"], "match": ["mushroom", "bean"],
     "price": "$49-79", "bundle": "Bean bag cover + pouf",
     "notes": "Cover only (like our poufs), shipped flat - same model as the pouf business."},
    {"id": "mushroom-dog-bed", "name": "Mushroom pet bed cover",
     "keywords": ["mushroom dog bed", "mushroom cat bed"], "match": ["mushroom", "bed"],
     "price": "$29-45", "bundle": "-",
     "notes": "Cover only or stuffed, velvet mushroom shape."},
    {"id": "mushroom-floor-cushion", "name": "Mushroom floor cushion / meditation cushion",
     "keywords": ["mushroom floor cushion", "mushroom floor pillow"], "match": ["mushroom", "cushion"],
     "price": "$29-45", "bundle": "2 floor cushions",
     "notes": "We have CoverPillowVMagic535A (60 cm) - more colours/shapes."},
    {"id": "mushroom-pouf-more", "name": "More mushroom poufs (new colours / shapes)",
     "keywords": ["mushroom pouf", "mushroom ottoman"], "match": ["mushroom", "pouf"],
     "price": "$65-75", "bundle": "2 poufs; pouf + pillow covers",
     "notes": "Benchmark for our own core product: how strong is the mushroom pouf search itself."},
    {"id": "mushroom-laundry", "name": "Mushroom fabric laundry basket / storage basket",
     "keywords": ["mushroom laundry basket", "mushroom storage basket"], "match": ["mushroom", "basket"],
     "price": "$22-32", "bundle": "Set of 2 sizes",
     "notes": "Cotton rope or quilted fabric."},
    {"id": "groovy-pillow-covers", "name": "70s groovy / retro pillow covers (not only mushroom)",
     "keywords": ["groovy pillow covers", "70s throw pillow covers"], "match": ["groovy"],
     "price": "$22-26", "bundle": "Set of 2 / 4",
     "notes": "Wider retro theme the mushroom pouf already sells into (trippy / psychedelic searches)."},
    {"id": "cottagecore-pillow", "name": "Cottagecore embroidered pillow covers",
     "keywords": ["cottagecore pillow covers"], "match": ["cottagecore"],
     "price": "$22-26", "bundle": "Set of 2",
     "notes": "Flowers + mushrooms + butterflies - same artwork family."},
]


def _rank(item):
    """(Home & Kitchen-level rank, category, sub-category rank, sub-category)."""
    for sr in item.get("salesRanks") or []:
        dg = (sr.get("displayGroupRanks") or [{}])[0] if sr.get("displayGroupRanks") else {}
        cr = (sr.get("classificationRanks") or [{}])[0] if sr.get("classificationRanks") else {}
        return dg.get("rank"), dg.get("title"), cr.get("rank"), cr.get("title")
    return None, None, None, None


def _prices(pr, asins):
    out = {}
    for k in range(0, len(asins), 20):
        for attempt in range(3):
            try:
                for c in pr.get_competitive_pricing_for_asins(asins[k:k + 20]).payload:
                    lp = [x["Price"]["LandedPrice"]["Amount"] for x in
                          ((c.get("Product") or {}).get("CompetitivePricing") or {}).get("CompetitivePrices", [])
                          if (x.get("Price") or {}).get("LandedPrice")]
                    if lp:
                        out[c["ASIN"]] = float(min(lp))
                break
            except Exception:
                time.sleep(3 * (attempt + 1))
        time.sleep(2.1)
    return out


def our_pouf_benchmark(token, ci):
    rows = pb_list_all(token, "asin_group_mapping", "", fields="asin,group,end_of_life")
    asins = sorted({(r.get("asin") or "").strip() for r in rows
                    if (r.get("group") or "").startswith(POUF_GROUP_PREFIXES) and not r.get("end_of_life")
                    and (r.get("asin") or "").startswith("B0")})
    # One get_catalog_item per ASIN: the identifiers search returned ranks for
    # only 3 of 49 pouf ASINs.
    ranks = {}
    for a in asins:
        try:
            p = ci.get_catalog_item(a, marketplaceIds=US.marketplace_id, includedData="salesRanks").payload
        except Exception:
            continue
        r, cat, _, _ = _rank(p)
        if r and cat == "Home & Kitchen":
            ranks[a] = r
        time.sleep(0.6)
    # Variation children share their family's rank - one value per family.
    vals = sorted(set(ranks.values()))
    return {"asins": len(asins), "ranked": len(vals), "best": vals[0] if vals else None,
            "top5Median": int(statistics.median(vals[:5])) if vals else None,
            "median": int(statistics.median(vals)) if vals else None, "ranks": ranks}


def our_search_demand(token, since):
    rows = pb_list_all(token, "ads_search_term_stats", f'country_code = "US" && date >= "{since}"',
                       fields="search_term,impressions,clicks,orders,spend,sales")
    agg = defaultdict(lambda: {"impressions": 0, "clicks": 0, "orders": 0, "spend": 0.0, "sales": 0.0})
    for r in rows:
        t = (r.get("search_term") or "").lower().strip()
        if not t or t.startswith("b0") or t.startswith("("):
            continue
        a = agg[t]
        for k in ("impressions", "clicks", "orders"):
            a[k] += int(r.get(k) or 0)
        a["spend"] += float(r.get("spend") or 0)
        a["sales"] += float(r.get("sales") or 0)
    return agg


def research_idea(idea, ci, pr, bench, demand):
    found = {}
    for kw in idea["keywords"]:
        try:
            items = ci.search_catalog_items(keywords=[kw], marketplaceIds=[US.marketplace_id],
                                            includedData=["summaries", "salesRanks"], pageSize=20).payload.get("items", [])
        except Exception as exc:
            items = []
            idea.setdefault("errors", []).append(f"{kw}: {str(exc)[:80]}")
        for it in items:
            if it["asin"] in found:
                continue
            s = (it.get("summaries") or [{}])[0]
            r, cat, sub_r, sub = _rank(it)
            found[it["asin"]] = {"asin": it["asin"], "title": (s.get("itemName") or "")[:140], "brand": s.get("brand") or "",
                                 "rank": r, "rankCategory": cat, "subRank": sub_r, "subCategory": sub, "keyword": kw}
        time.sleep(0.6)
    prices = _prices(pr, list(found))
    comps = []
    for a, c in found.items():
        c["price"] = prices.get(a)
        c["ours"] = c["brand"].lower().startswith(OUR_BRANDS)
        t = c["title"].lower()
        c["onTheme"] = any(w in t for w in THEME_WORDS) and not (COLOUR_RE.search(t) and "mushroom" not in COLOUR_RE.sub("", t))
        comps.append(c)
    others = [c for c in comps if not c["ours"]]
    hk = sorted(c["rank"] for c in others if c["rank"] and c["rankCategory"] == "Home & Kitchen")
    other_cat = [c for c in others if c["rank"] and c["rankCategory"] != "Home & Kitchen"]
    price_vals = [c["price"] for c in others if c["price"]]
    terms = [(t, v) for t, v in demand.items() if all(w in t for w in idea["match"])]
    terms.sort(key=lambda x: -x[1]["impressions"])
    tot = {k: sum(v[k] for _, v in terms) for k in ("impressions", "clicks", "orders")}
    comps.sort(key=lambda c: (c["rank"] is None, c["rank"] or 0))
    theme_hk = sorted(c["rank"] for c in others if c["onTheme"] and c["rank"] and c["rankCategory"] == "Home & Kitchen")
    theme_prices = [c["price"] for c in others if c["onTheme"] and c["price"]]
    return {
        "id": idea["id"], "results": len(comps),
        "hkRanked": len(hk),
        "betterThanOurBest": sum(1 for r in hk if bench["best"] and r <= bench["best"]),
        "betterThanOurTop5": sum(1 for r in hk if bench["top5Median"] and r <= bench["top5Median"]),
        "betterThanOurMedian": sum(1 for r in hk if bench["median"] and r <= bench["median"]),
        "under100k": sum(1 for r in hk if r <= 100000),
        "onTheme": sum(1 for c in others if c["onTheme"]),
        "onThemeBetterThanOurBest": sum(1 for r in theme_hk if bench["best"] and r <= bench["best"]),
        "onThemeUnder100k": sum(1 for r in theme_hk if r <= 100000),
        "onThemeBestRank": theme_hk[0] if theme_hk else None,
        "onThemeMedianPrice": round(statistics.median(theme_prices), 2) if theme_prices else None,
        "bestRank": hk[0] if hk else None,
        "top5MedianRank": int(statistics.median(hk[:5])) if hk else None,
        "otherCategoryRanked": len(other_cat),
        "medianPrice": round(statistics.median(price_vals), 2) if price_vals else None,
        "minPrice": min(price_vals) if price_vals else None, "maxPrice": max(price_vals) if price_vals else None,
        "pricedAtLeast20": sum(1 for p in price_vals if p >= 20), "priced": len(price_vals),
        "ourSearches": {**tot, "terms": [{"term": t, **{k: round(v[k], 2) for k in v}} for t, v in terms[:15]]},
        "competitors": comps[:20],
        "errors": idea.get("errors", []),
    }


def run_research(only=None):
    token = pb_authenticate()
    ci = CatalogItems(credentials=CRED, marketplace=US, version="2022-04-01")
    pr = Products(credentials=CRED, marketplace=US)
    bench = our_pouf_benchmark(token, ci)
    demand = our_search_demand(token, (date.today() - timedelta(days=90)).isoformat())
    prev = get_job_state(token, "product_research") or {}
    if isinstance(prev, str):
        prev = json.loads(prev or "{}")
    results = dict(prev.get("results") or {})
    for idea in IDEAS:
        if only and idea["id"] not in only:
            continue
        results[idea["id"]] = research_idea(dict(idea), ci, pr, bench, demand)
    out = {"ranAt": datetime.now(timezone.utc).isoformat(timespec="seconds"), "marketplace": "US",
           "benchmark": {k: v for k, v in bench.items() if k != "ranks"}, "results": results}
    set_job_state(token, "product_research", json.dumps(out))
    return out


def _load(token, key):
    v = get_job_state(token, key)
    if isinstance(v, str):
        v = json.loads(v or "{}")
    return v or {}


def RunProductResearch(request):
    args = getattr(request, "args", {}) or {}
    only = [x for x in (args.get("only") or "").split(",") if x] or None
    try:
        out = run_research(only)
        return _json({"ok": True, "ranAt": out["ranAt"], "ideas": len(out["results"]), "benchmark": out["benchmark"]})
    except Exception as exc:
        return _json({"error": str(exc)}, 500)


def GetProductResearch(request):
    try:
        token = pb_authenticate()
        data = _load(token, "product_research")
        status = _load(token, "product_research_status")
        ideas = []
        for idea in IDEAS:
            ideas.append({**idea, "research": (data.get("results") or {}).get(idea["id"]),
                          "status": (status.get(idea["id"]) or {}).get("status", "idea"),
                          "userNotes": (status.get(idea["id"]) or {}).get("notes", "")})
        return _json({"ranAt": data.get("ranAt"), "benchmark": data.get("benchmark"), "ideas": ideas,
                      "statuses": STATUSES})
    except Exception as exc:
        return _json({"error": str(exc)}, 500)


def SetProductResearchStatus(request):
    body = request.get_json(silent=True) or {}
    idea_id = body.get("id")
    if idea_id not in {i["id"] for i in IDEAS}:
        return _json({"error": "unknown idea"}, 400)
    if body.get("status") is not None and body["status"] not in STATUSES:
        return _json({"error": "bad status"}, 400)
    try:
        token = pb_authenticate()
        status = _load(token, "product_research_status")
        cur = status.get(idea_id) or {}
        if body.get("status") is not None:
            cur["status"] = body["status"]
        if body.get("notes") is not None:
            cur["notes"] = str(body["notes"])[:2000]
        status[idea_id] = cur
        set_job_state(token, "product_research_status", json.dumps(status))
        return _json({"ok": True, "id": idea_id, **cur})
    except Exception as exc:
        return _json({"error": str(exc)}, 500)
