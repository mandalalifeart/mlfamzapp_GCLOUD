"""Weekly (Mondays since 2026-10-24, per the user; was daily) Telegram digest for specific watched campaigns (2026-10-02, per the
user: "monitor daily performance of this campaign on baba social bot").

Watch list = JSON in job_state key "campaign_watch":
  [{"label": "...", "campaign_ids": [...], "sku": "...", "since": "YYYY-MM-DD"}]
Reads only already-stored data (ads_campaign_stats, ads_search_term_stats,
ads_advertised_product_stats - pulled by the morning cron jobs) plus live
USA stock, and sends one message per watch to @baba_social_bot
(MCF_TELEGRAM_* credentials, same as DailyAdsPerformanceDigest).
"""
import json
import os
from collections import defaultdict
from datetime import date, timedelta

import requests

from AdsStockGuard import usa_stock
from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state
from NotificationRouting import notify, send_amz_telegram

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
TELEGRAM_BOT_TOKEN = (os.environ.get("UPDATE_TELEGRAM_BOT_TOKEN") or os.environ.get("MCF_TELEGRAM_BOT_TOKEN", ""))
TELEGRAM_CHAT_ID = (os.environ.get("UPDATE_TELEGRAM_CHAT_ID") or os.environ.get("MCF_TELEGRAM_CHAT_ID", ""))
JOB_NAME = "campaign-watch-digest"


def _json(body, status=200):
    return json.dumps(body), status, {"Access-Control-Allow-Origin": ALLOWED_ORIGIN, "Content-Type": "application/json"}


def _sum(rows):
    t = {"impressions": 0, "clicks": 0, "spend": 0.0, "sales": 0.0, "orders": 0}
    for r in rows:
        for k in t:
            t[k] += r.get(k) or 0
    return t


def _line(t, cur="$"):
    ctr = f"{100 * t['clicks'] / t['impressions']:.2f}%" if t["impressions"] else "–"
    cpc = f"{cur}{t['spend'] / t['clicks']:.2f}" if t["clicks"] else "–"
    acos = f"{100 * t['spend'] / t['sales']:.0f}%" if t["sales"] else ("∞" if t["spend"] else "–")
    return (f"{t['impressions']:,} impr · {t['clicks']} clicks (CTR {ctr}) · {cur}{t['spend']:.2f} spend (CPC {cpc})"
            f" · {t['orders']} orders · {cur}{t['sales']:.2f} sales · ACOS {acos}")


def _dedupe(rows, key_fields):
    best = {}
    for r in rows:
        k = tuple(r.get(f) for f in key_fields)
        if k not in best or r["profile_id"] < best[k]["profile_id"]:
            best[k] = r
    return list(best.values())


def eu_stock(sku):
    """(FBA fulfillable, FC transfer + inbound) in the Pan-EU pool (DE marketplace)."""
    import os
    from sp_api.api import Inventories
    from sp_api.base import Marketplaces
    cred = {"refresh_token": os.environ["REFRESH_TOKEN_EU"], "lwa_app_id": os.environ["CLIENT_ID_EU"],
            "lwa_client_secret": os.environ["CLIENT_SECRET_EU"]}
    inv = Inventories(credentials=cred, marketplace=Marketplaces.DE).get_inventory_summary_marketplace(
        details=True, marketplaceIds=[Marketplaces.DE.marketplace_id], sellerSkus=[sku]).payload
    fba = coming = 0
    for s in inv.get("inventorySummaries", []):
        if s.get("sellerSku") != sku:
            continue
        d = s.get("inventoryDetails") or {}
        res = d.get("reservedQuantity") or {}
        fba += d.get("fulfillableQuantity") or 0
        coming += (res.get("pendingTransshipmentQuantity") or 0) + (res.get("fcProcessingQuantity") or 0) + \
            sum(d.get(k) or 0 for k in ("inboundWorkingQuantity", "inboundShippedQuantity", "inboundReceivingQuantity"))
    return fba, coming


def build_watch_text(pb, watch, day, days=7):
    # Weekly since 2026-10-24 (per the user "i want those updates weekly on
    # Monday"): per campaign the last `days` days ending `day`, vs the
    # `days` before, plus the since-launch total.
    # Per the user (2026-10-06) Germany watches too: "region": "de" uses €
    # and EU FBA stock instead of USA stock.
    cur = watch.get("currency") or "$"
    since = watch.get("since") or day
    d_end = date.fromisoformat(day)
    w_start = (d_end - timedelta(days=days - 1)).isoformat()
    p_start = (d_end - timedelta(days=2 * days - 1)).isoformat()
    p_end = (d_end - timedelta(days=days)).isoformat()
    q_start = min(since, p_start)
    ids = " || ".join(f'campaign_id = "{c}"' for c in watch["campaign_ids"])
    all_rows = _dedupe(pb_list_all(pb, "ads_campaign_stats", f'({ids}) && date >= "{q_start}" && date <= "{day}"'),
                       ("campaign_id", "date", "ad_product"))
    camp_rows = [r for r in all_rows if r["date"] >= since]
    last_date = max((r["date"] for r in all_rows), default=None)
    period = f"{w_start} – {day}" if days > 1 else day
    lines = [f"📣 {watch['label']} – ads {'week ' if days == 7 else ''}{period}"]
    if last_date != day:
        lines.append(f"(no ads data for {day} yet - Amazon's numbers usually land the next morning;"
                     f" latest stored day: {last_date or 'none'})")
    names = dict(watch.get("names") or {})
    for r in camp_rows:
        names[r["campaign_id"]] = r.get("campaign_name") or r["campaign_id"]
    for cid in watch["campaign_ids"]:
        week_rows = [r for r in all_rows if r["campaign_id"] == cid and w_start <= r["date"] <= day]
        prev_rows = [r for r in all_rows if r["campaign_id"] == cid and p_start <= r["date"] <= p_end]
        lines.append(f"\n• {names.get(cid, cid)}\n  {_line(_sum(week_rows), cur)}")
        if days > 1 and prev_rows:
            lines.append(f"  week before: {_line(_sum(prev_rows), cur)}")
    if days > 1:
        lines.append(f"\nAll campaigns this week: {_line(_sum([r for r in all_rows if w_start <= r['date'] <= day]), cur)}")
    total = _sum(camp_rows)
    lines.append(f"\nSince {since}: {_line(total, cur)}")

    if watch.get("sku"):
        adv = _dedupe(pb_list_all(pb, "ads_advertised_product_stats",
                                  f'({ids}) && date >= "{since}" && date <= "{day}"'),
                      ("campaign_id", "ad_group_id", "sku", "date"))
        adv = [r for r in adv if r.get("sku") == watch["sku"]]
        same_day = sum(r.get("same_sku_units") or 0 for r in adv if w_start <= r["date"] <= day)
        same_all = sum(r.get("same_sku_units") or 0 for r in adv)
        lines.append(f"Units of {watch['sku']} sold via these ads: {same_day} {'this week' if days > 1 else 'on ' + day}, {same_all} since {since}")

    terms = _dedupe(pb_list_all(pb, "ads_search_term_stats", f'({ids}) && date >= "{since}" && date <= "{day}"'),
                    ("campaign_id", "ad_group_id", "target_id", "search_term", "date"))
    agg = defaultdict(lambda: {"clicks": 0, "orders": 0, "spend": 0.0, "impressions": 0})
    for r in terms:
        a = agg[r["search_term"]]
        for k in a:
            a[k] += r.get(k) or 0
    top = sorted(agg.items(), key=lambda kv: (-kv[1]["orders"], -kv[1]["clicks"], -kv[1]["impressions"]))[:8]
    if top:
        lines.append(f"\nTop customer searches since {since}:")
        for term, a in top:
            lines.append(f"  {term}: {a['clicks']} clicks, {a['orders']} orders, {cur}{a['spend']:.2f}")

    if watch.get("sku") and watch.get("region") == "de":
        try:
            fba, coming = eu_stock(watch["sku"])
            st = pb_list_all(pb, "sku_statistics", f'sku = "{watch["sku"]}"',
                             fields="de_balance_lg,de_on_the_way")
            lg = (st[0].get("de_balance_lg") or 0) if st else 0
            otw = (st[0].get("de_on_the_way") or 0) if st else 0
            lines.append(f"\n📦 EU stock: {fba} FBA available + {coming} FC transfer / inbound to FBA"
                         f" + {lg} in LG warehouse + {otw} on the way to Germany = {fba + coming + lg + otw}")
        except Exception as exc:
            lines.append(f"\n📦 stock check failed: {exc}")
    elif watch.get("sku"):
        try:
            fba, awd, otw = usa_stock(watch["sku"], with_inbound=True)
            lines.append(f"\n📦 USA stock: {fba} FBA + {awd} AWD on hand + {otw} on the way to AWD = "
                         f"{fba + awd + otw} (stock guard pauses when FBA + AWD on hand ≤ "
                         f"{watch.get('guard_threshold', 10)})")
        except Exception as exc:
            lines.append(f"\n📦 stock check failed: {exc}")
    return "\n".join(lines)


def SendCampaignWatchDigest(request):
    try:
        args = getattr(request, "args", {}) or {}
        day = args.get("date") or (date.today() - timedelta(days=1)).isoformat()
        days = int(args.get("days") or 7)
        pb = pb_authenticate()
        watches = json.loads(get_job_state(pb, "campaign_watch") or "[]")
        sent = []
        for watch in watches:
            text = build_watch_text(pb, watch, day, days)
            if args.get("dry") == "1":
                sent.append(text)
                continue
            # @baba_amz_bot since 2026-10-22 (per the user), social bot fallback.
            resp = send_amz_telegram("sendMessage", json_body={"text": text[:4000]}, timeout=30)
            resp.raise_for_status()
            sent.append(text)
        return _json({"day": day, "sent": sent})
    except Exception as exc:
        notify(JOB_NAME, "amzbot", f"🚨 Campaign watch digest failed: {exc}", is_error=True)
        return _json({"error": str(exc), "type": exc.__class__.__name__}, 500)
