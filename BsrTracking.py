"""Daily Best Seller Rank tracking (per the user 2026-10-31 "lets track bsr
also for all my items").

Once a day, for every ASIN in asin_group_mapping (IGNORE groups skipped) and
every main marketplace, reads the Catalog Items API salesRanks: the main
(display group) rank, e.g. Home & Kitchen / Clothing, and the sub-category
ranks. One row per (date, marketplace, asin) in PocketBase `bsr_daily`
(delete-then-insert for the day, so a rerun is safe). An ASIN that is not in
a marketplace's catalog or has no rank there is simply not written.
Catalog getCatalogItem allows ~2 requests/s, so ~120 ASINs x 7 marketplaces
takes about 8 minutes.

Endpoints: UpdateBsrDaily (cron, args `mp=US,DE` / `date=`), GetBsrHistory
(`asin` or `sku`, optional `mp`, `days`)."""
import json
import os
import time
from datetime import date, timedelta

import requests
from sp_api.api import CatalogItems
from sp_api.base import Marketplaces

from AsinAnalytics import pb_authenticate, pb_list_all
from WeeklyChecks import _json

COLLECTION = "bsr_daily"
MARKETPLACES = ["US", "CA", "UK", "DE", "FR", "IT", "ES"]
POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")


def _creds(mp):
    if mp in ("US", "CA", "MX"):
        return dict(refresh_token=os.environ["REFRESH_TOKEN_USA"], lwa_app_id=os.environ["CLIENT_ID_USA"],
                    lwa_client_secret=os.environ["CLIENT_SECRET_USA"])
    return dict(refresh_token=os.environ["REFRESH_TOKEN_EU"], lwa_app_id=os.environ["CLIENT_ID_EU"],
                lwa_client_secret=os.environ["CLIENT_SECRET_EU"])


def tracked_asins(token):
    out = {}
    for r in pb_list_all(token, "asin_group_mapping", ""):
        g = r.get("group") or ""
        if not r.get("asin") or g.upper().startswith("IGNORE") or r.get("end_of_life"):
            continue
        out.setdefault(r["asin"], {"sku": r.get("sku") or "", "group": g})
    return out


def read_ranks(payload):
    """-> (category, rank, [sub-category ranks]) from a catalog item's salesRanks."""
    for block in payload.get("salesRanks") or []:
        main = (block.get("displayGroupRanks") or [{}])[0]
        subs = [{"title": c.get("title"), "rank": c.get("rank")} for c in block.get("classificationRanks") or []]
        if main or subs:
            return main.get("title"), main.get("rank"), subs
    return None, None, []


def fetch_ranks(mp, asins):
    m = getattr(Marketplaces, mp)
    ci = CatalogItems(credentials=_creds(mp), marketplace=m, version="2022-04-01")
    rows = []
    for asin in asins:
        for attempt in range(4):
            try:
                p = ci.get_catalog_item(asin, marketplaceIds=m.marketplace_id, includedData="salesRanks").payload
                break
            except Exception as exc:
                if "NOT_FOUND" in str(exc) or "not found" in str(exc).lower():
                    p = None
                    break
                if attempt == 3:
                    p = None
                    break
                time.sleep(2 * (attempt + 1))
        time.sleep(0.55)
        if not p:
            continue
        cat, rank, subs = read_ranks(p)
        if rank is None and not subs:
            continue
        best = min((s for s in subs if s.get("rank")), key=lambda s: s["rank"], default={})
        rows.append({"asin": asin, "category": cat or "", "rank": rank, "sub_category": best.get("title") or "",
                     "sub_rank": best.get("rank"), "sub_ranks": subs})
    return rows


def write_day(token, day, mp, rows, meta):
    old = pb_list_all(token, COLLECTION, f'date = "{day}" && marketplace = "{mp}"', fields="id")
    ops = [{"method": "DELETE", "url": f"/api/collections/{COLLECTION}/records/{r['id']}"} for r in old]
    ops += [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records",
             "body": {"date": day, "marketplace": mp, **meta.get(r["asin"], {}), **r}} for r in rows]
    for i in range(0, len(ops), 50):
        resp = requests.post(f"{POCKETBASE_URL}/api/batch", headers={"Authorization": token},
                             json={"requests": ops[i:i + 50]}, timeout=60)
        resp.raise_for_status()
        bad = [x for x in resp.json() if not 200 <= int(x.get("status", 0)) < 300]
        if bad:
            raise RuntimeError(f"bsr_daily batch failed: {str(bad[:2])[:300]}")
    return len(rows)


def run(mps=None, day=None):
    token = pb_authenticate()
    meta = tracked_asins(token)
    day = day or date.today().isoformat()
    out = {}
    for mp in mps or MARKETPLACES:
        try:
            out[mp] = write_day(token, day, mp, fetch_ranks(mp, list(meta)), meta)
        except Exception as exc:
            out[mp] = f"error: {str(exc)[:200]}"
    return {"date": day, "asins": len(meta), "written": out}


def UpdateBsrDaily(request):
    args = getattr(request, "args", {}) or {}
    mps = [x.strip().upper() for x in (args.get("mp") or "").split(",") if x.strip()] or None
    try:
        return _json(run(mps, args.get("date")))
    except Exception as exc:
        return _json({"error": str(exc)}, 500)


def GetBsrHistory(request):
    args = getattr(request, "args", {}) or {}
    try:
        token = pb_authenticate()
        asin = (args.get("asin") or "").strip()
        sku = (args.get("sku") or "").strip()
        if not asin and sku:
            hit = pb_list_all(token, "asin_group_mapping", f"sku = {json.dumps(sku)}")
            asin = hit[0]["asin"] if hit else ""
        if not asin:
            return _json({"error": "asin or sku required"}, 400)
        since = (date.today() - timedelta(days=int(args.get("days") or 365))).isoformat()
        flt = f'asin = "{asin}" && date >= "{since}"'
        if args.get("mp"):
            flt += f' && marketplace = "{args["mp"].upper()}"'
        rows = pb_list_all(token, COLLECTION, flt, fields="date,marketplace,category,rank,sub_category,sub_rank",
                           sort="date")
        series = {}
        for r in rows:
            series.setdefault(r["marketplace"], []).append(
                {k: r.get(k) for k in ("date", "category", "rank", "sub_category", "sub_rank")})
        return _json({"asin": asin, "series": series})
    except Exception as exc:
        return _json({"error": str(exc)}, 500)
