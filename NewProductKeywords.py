"""Weekly search-term tracking for new product ideas (per the user 2026-11-17:
"lets track search terms for new products: mushroom lamp, crystal lamp,
coconut lamp and add more potential keywords").

Source: Brand Analytics Search Terms report (GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT,
WEEK = Sunday-Saturday) per marketplace - every search term in the store with its
search frequency rank (1 = most searched) and the top 3 clicked products.
The file is huge (US: many GB), so it is streamed and only terms matching a
product's root word AND a context word (lamp / light / decor ... in every
language) are kept.

Config: job_state `new_product_keywords` (JSON: {"products": {name: [roots]},
"context": [words], "marketplaces": [...]}) - defaults below; add products/words there.
Rows: PocketBase `new_product_search_terms` (week, marketplace, product, term, rank,
top = [{asin, name, click, conv}]), replaced per (week, marketplace).
Catch-up safe: finished week|marketplace pairs in job_state `new_product_kw_done`.
Report (@baba_update_bot, notify "new-product-keywords"): per product the best-ranked
terms per marketplace with the change vs the previous stored week + new terms.

Args: dry=1 (no Telegram), weeks=N (look back N finished weeks, default 2),
mp=US,DE (subset), week=YYYY-MM-DD (a Sunday), report=1 (only the report)."""
import json
import re
import time
import zlib
from datetime import date, timedelta

import requests
from sp_api.api import ReportsV2
from sp_api.base import Marketplaces

from AsinAnalytics import pb_authenticate, pb_list_all, pb_replace
from JobState import get_job_state, set_job_state
from WeeklyChecks import creds

COLLECTION = "new_product_search_terms"
JOB = "new-product-keywords"
DEFAULT = {
    "products": {
        "mushroom lamp": ["mushroom", "pilz", "fliegenpilz", "champignon", "fungo", "funghi", "seta", "setas", "hongo", "toadstool"],
        "crystal lamp": ["crystal", "crystals", "quartz", "amethyst", "selenite", "geode", "gemstone", "citrine", "kristall",
                         "bergkristall", "edelstein", "amethyst", "selenit", "cristal", "cristaux", "cristallo", "cristalli",
                         "ametista", "amatista", "quarzo", "cuarzo", "quartz rose"],
        "coconut lamp": ["coconut", "kokos", "kokosnuss", "kokosnuß", "coco", "noix de coco", "cocco", "noce di cocco"],
    },
    "context": ["lamp", "lamps", "light", "lights", "night light", "nightlight", "lampshade", "shade", "lantern", "bedside",
                "table lamp", "decor", "lampe", "lampen", "leuchte", "licht", "nachtlicht", "tischlampe", "nachttischlampe",
                "lampenschirm", "deko", "lumière", "lumiere", "veilleuse", "lampada", "lampade", "luce", "lume", "abat-jour",
                "lámpara", "lampara", "luz", "decoración", "decorazione", "décoration", "decoration", "led"],
    "marketplaces": ["US", "UK", "DE", "FR", "IT", "ES"],
    "exclude": ["drink", "drink mix", "pimax", "headset", "powder", "on the go"],
}
TERM_RE = re.compile(r'"searchTerm"\s*:\s*"((?:[^"\\]|\\.)*)"')


def config(token):
    raw = get_job_state(token, "new_product_keywords")
    if not raw:
        set_job_state(token, "new_product_keywords", json.dumps(DEFAULT, ensure_ascii=False))
        return DEFAULT
    return json.loads(raw)


def matchers(cfg):
    w = lambda words: r"(?<![\w])(?:" + "|".join(re.escape(x) for x in sorted(set(words), key=len, reverse=True)) + r")(?![\w])"
    prods = [(p, re.compile(w(roots), re.I)) for p, roots in cfg["products"].items()]
    ctx = re.compile(w(cfg["context"]), re.I)
    exc = re.compile(w(cfg.get("exclude") or ["\x00"]), re.I)
    return prods, (ctx, exc)


def classify(term, prods, ctx):
    ctx, exc = ctx
    if not ctx.search(term) or exc.search(term):
        return None
    for p, rx in prods:
        if rx.search(term):
            return p
    return None


def last_sundays(n, today=None):
    today = today or date.today()
    # a WEEK report covers Sunday..Saturday; Amazon has it ready ~2-3 days later
    last_sat = today - timedelta(days=(today.weekday() + 2) % 7 or 7)
    if (today - last_sat).days < 3:
        last_sat -= timedelta(days=7)
    return [last_sat - timedelta(days=6 + 7 * i) for i in range(n)][::-1]


def request_report(mp, sunday):
    cred, _ = creds(mp)
    m = getattr(Marketplaces, mp)
    r = ReportsV2(credentials=cred, marketplace=m)
    for attempt in range(6):
        try:
            rid = r.create_report(reportType="GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT", marketplaceIds=[m.marketplace_id],
                                  dataStartTime=sunday.isoformat(), dataEndTime=(sunday + timedelta(days=6)).isoformat(),
                                  reportOptions={"reportPeriod": "WEEK"}).payload["reportId"]
            return r, rid
        except Exception as exc:
            if "QuotaExceeded" not in str(exc) or attempt == 5:
                raise
            time.sleep(60)


def wait_report(r, rid, minutes=60):
    for _ in range(minutes * 2):
        p = r.get_report(rid).payload
        if p["processingStatus"] in ("DONE", "FATAL", "CANCELLED"):
            return p
        time.sleep(30)
    raise TimeoutError(f"report {rid} not done after {minutes} min")


def stream_rows(r, doc_id, prods, ctx):
    """Stream the (gzipped) JSON document; return {term: {product, rank, top}}."""
    doc = r.get_report_document(doc_id).payload
    resp = requests.get(doc["url"], stream=True, timeout=900)
    resp.raise_for_status()
    dec = zlib.decompressobj(16 + zlib.MAX_WBITS) if doc.get("compressionAlgorithm") == "GZIP" else None
    found, buf = {}, ""
    for chunk in resp.iter_content(1 << 20):
        buf += (dec.decompress(chunk) if dec else chunk).decode("utf-8", "ignore")
        cut = buf.rfind("}")
        if cut < 0:
            continue
        part = buf[:cut + 1]
        for m in TERM_RE.finditer(part):
            term = json.loads('"' + m.group(1) + '"')
            prod = classify(term, prods, ctx)
            if not prod:
                continue
            s, e = part.rfind("{", 0, m.start()), part.find("}", m.end())
            if s < 0 or e < 0:
                continue
            try:
                o = json.loads(part[s:e + 1])
            except ValueError:
                continue
            d = found.setdefault(term, {"product": prod, "rank": o.get("searchFrequencyRank"), "top": []})
            d["top"].append({"asin": o.get("clickedAsin"), "name": (o.get("clickedItemName") or "")[:120],
                             "click": o.get("clickShare"), "conv": o.get("conversionShare"), "pos": o.get("clickShareRank")})
        buf = buf[cut + 1:][-20000:]
    for d in found.values():
        d["top"] = sorted(d["top"], key=lambda x: x.get("pos") or 9)[:3]
    return found


def pull(token, weeks, mps, cfg):
    prods, ctx = matchers(cfg)
    done = set(json.loads(get_job_state(token, "new_product_kw_done") or "[]"))
    log = []
    for sunday in weeks:
        for mp in mps:
            key = f"{sunday}|{mp}"
            if key in done:
                continue
            try:
                r, rid = request_report(mp, sunday)
                p = wait_report(r, rid)
                if p["processingStatus"] != "DONE":
                    log.append(f"{key}: {p['processingStatus']}")
                    continue
                found = stream_rows(r, p["reportDocumentId"], prods, ctx)
                pb_replace(token, COLLECTION, f'week = "{sunday}" && marketplace = "{mp}"',
                           [{"week": str(sunday), "marketplace": mp, "product": d["product"], "term": t, "rank": d["rank"],
                             "top": d["top"]} for t, d in found.items()])
                done.add(key)
                set_job_state(token, "new_product_kw_done", json.dumps(sorted(done)))
                log.append(f"{key}: {len(found)} terms")
            except Exception as exc:
                log.append(f"{key}: ERROR {str(exc)[:200]}")
            print(log[-1], flush=True)
    return log


def report(token, cfg, mps):
    rows = pb_list_all(token, COLLECTION, 'week != ""', fields="week,marketplace,product,term,rank,top")
    weeks = sorted({r["week"] for r in rows})
    if not weeks:
        return "No search-term data yet."
    cur = weeks[-1]
    prev = weeks[-2] if len(weeks) > 1 else None
    seen_before = {(r["marketplace"], r["term"]) for r in rows if r["week"] < cur}
    by = {(r["week"], r["marketplace"], r["term"]): r for r in rows}
    lines = [f"🔎 New product search terms - week {cur} (Brand Analytics rank, lower = more searched)"]
    for prod in cfg["products"]:
        lines.append(f"\n== {prod.upper()} ==")
        for mp in mps:
            cur_rows = sorted([r for r in rows if r["week"] == cur and r["marketplace"] == mp and r["product"] == prod],
                              key=lambda r: r["rank"] or 1e9)
            if not cur_rows:
                lines.append(f"{mp}: no term in the store's ranked list")
                continue
            parts = []
            for r in cur_rows[:8]:
                p = by.get((prev, mp, r["term"])) if prev else None
                ch = ""
                if p and p.get("rank") and r.get("rank"):
                    d = p["rank"] - r["rank"]
                    ch = f" ({'▲' if d > 0 else '▼'}{abs(d):,})" if d else " (=)"
                elif (mp, r["term"]) not in seen_before and len(weeks) > 1:
                    ch = " 🆕"
                top = (r.get("top") or [{}])[0]
                parts.append(f"  #{int(r['rank']):,} {r['term']}{ch} - top: {(top.get('name') or '')[:45]}")
            lines.append(f"{mp} ({len(cur_rows)} terms):")
            lines += parts
    return "\n".join(lines)


def SendNewProductKeywords(request):
    from AsinAnalytics import json_response
    from NotificationRouting import notify
    args = request.args
    token = pb_authenticate()
    cfg = config(token)
    mps = (args.get("mp") or "").upper().split(",") if args.get("mp") else cfg["marketplaces"]
    log = []
    if not args.get("report"):
        weeks = [date.fromisoformat(args["week"])] if args.get("week") else last_sundays(int(args.get("weeks") or 2))
        log = pull(token, weeks, mps, cfg)
    text = report(token, cfg, cfg["marketplaces"])
    errors = [l for l in log if "ERROR" in l]
    if errors:
        text += "\n\n⚠️ " + "\n".join(errors)
    if not args.get("dry"):
        notify(JOB, "AmzBot", text, is_error=bool(errors) and not any("terms" in l for l in log))
    return json_response({"log": log, "text": text})
