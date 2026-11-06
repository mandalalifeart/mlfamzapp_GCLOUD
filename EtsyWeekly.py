"""Weekly Etsy ads + sales check (per the user 2026-11-06: "check every monday
in ads review section"). Runs Mondays inside the Monday digest, saved on
/ads-reviews (CheckReports.ADS_JOBS).

What the Etsy API gives: the payment ledger (Etsy Ads spend = "prolist",
Offsite Ads fees = one entry per order that came from an offsite ad), orders,
and lifetime listing views (daily snapshots in etsy_listing_views_daily since
2026-11-06). Etsy Ads orders/ROAS per listing are only in Shop Manager -> Etsy
Ads, there is no API for them, so the report asks for that table once a month.

Rules (from the chat 2026-11-06): margin before ads ~50% -> break-even ROAS ~2.0;
keep Etsy Ads while ROAS > 2.5; switch off a listing at $15+ spend with 0 orders.
Flags: 🟠 Etsy Ads spend > 15% of Etsy sales over the last 4 weeks;
⚠️ ad spend in the week but 0 orders; 🔴 a week with 0 orders at all."""
import calendar
import json
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests

TZ = ZoneInfo("Asia/Jerusalem")
BASE = "https://openapi.etsy.com/v3/application"


def _auth():
    from EtsyAuth import pb_authenticate, pb_get_connection, pb_save_connection, refresh_access_token, api_key_header
    pb = pb_authenticate()
    c = pb_get_connection(pb)
    at, rt = refresh_access_token(c["refresh_token"])
    if rt != c["refresh_token"]:
        pb_save_connection(pb, {"refresh_token": rt})
    return pb, c["shop_id"], {"x-api-key": api_key_header(), "Authorization": f"Bearer {at}"}


def _ts(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=TZ).timestamp())


def ledger(shop_id, headers, start, end):
    """Ledger totals by description for local dates [start, end)."""
    out = defaultdict(float)
    a, stop = _ts(start), _ts(end)
    while a < stop:
        b = min(a + 30 * 86400, stop)  # API max window 31 days
        off = 0
        while True:
            r = requests.get(f"{BASE}/shops/{shop_id}/payment-account/ledger-entries", headers=headers,
                             params={"min_created": a, "max_created": b - 1, "limit": 100, "offset": off}, timeout=30)
            r.raise_for_status()
            res = r.json()["results"]
            for e in res:
                out[e.get("description") or e.get("ledger_type")] += e["amount"] / 100
            if len(res) < 100:
                break
            off += 100
        a = b
    return out


def week_summary(L):
    sales = L["PAYMENT_GROSS"] + L["sales_tax"] + L["sales_tax_refund"] + L["REFUND_GROSS"]  # w/o sales tax, after refunds
    ads = -L["prolist"]
    offsite = round(-(L["offsite_ads_fee"] + L["offsite_ads_fee_refund"]), 2) + 0.0
    return sales, ads, offsite


def orders_in(pb, start, end):
    from AsinAnalytics import pb_list_all
    a, b = _ts(start), _ts(end)
    rows = [o for o in pb_list_all(pb, "etsy_orders", f"created >= {a} && created < {b}")
            if o.get("etsy_status") != "Canceled"]
    units = sum(o.get("item_count") or 0 for o in rows)
    by_listing = Counter()
    for o in rows:
        for li in o.get("line_items") or []:
            by_listing[str(li.get("listingId"))] += li.get("quantity", 1)
    return rows, units, by_listing


def views_gained(pb, start, end):
    """{listing_id: views gained} between the snapshots on start and end
    (None if the snapshots don't cover the week yet)."""
    from AsinAnalytics import pb_list_all
    a = {r["listing_id"]: r["views"] for r in pb_list_all(pb, "etsy_listing_views_daily", f'date = "{start.isoformat()}"')}
    b = {r["listing_id"]: r["views"] for r in pb_list_all(pb, "etsy_listing_views_daily", f'date = "{end.isoformat()}"')}
    if not a or not b:
        return None
    return {k: b[k] - a.get(k, b[k]) for k in b}


def weekly_text(today=None):
    today = today or datetime.now(TZ).date()
    end = today - timedelta(days=today.weekday())  # this Monday (exclusive)
    start = end - timedelta(days=7)
    pb, shop_id, H = _auth()
    from AsinAnalytics import pb_list_all
    titles = {str(r["listing_id"]): r["title"] for r in pb_list_all(pb, "etsy_listings", "")}

    L = ledger(shop_id, H, start - timedelta(days=21), end)  # last 4 weeks
    Lw = ledger(shop_id, H, start, end)
    Lp = ledger(shop_id, H, start - timedelta(days=7), start)
    s4, a4, o4 = week_summary(L)
    sw, aw, ow = week_summary(Lw)
    sp, ap, op = week_summary(Lp)
    rows, units, by_listing = orders_in(pb, start, end)
    prow, punits, _ = orders_in(pb, start - timedelta(days=7), start)
    ly_rows, ly_units, _ = orders_in(pb, start - timedelta(days=364), end - timedelta(days=364))

    lines = [f"🛍 Etsy weekly: {start:%d %b} - {end - timedelta(days=1):%d %b %Y}", ""]
    if not rows:
        lines.append("🔴 No Etsy orders this week.")
    lines += [
        f"Orders {len(rows)} ({units} units) - previous week {len(prow)}, same week last year {len(ly_rows)}",
        f"Sales (w/o tax, after refunds) ${sw:,.0f} - previous week ${sp:,.0f}",
        f"Etsy Ads spend ${aw:,.2f} (previous ${ap:,.2f}) · Offsite Ads fees ${ow:,.2f} (only charged on orders they brought)",
        "",
        f"Last 4 weeks: sales ${s4:,.0f}, Etsy Ads ${a4:,.2f} = {a4 / s4 * 100:.1f}% of sales" if s4 > 0
        else f"Last 4 weeks: no sales, Etsy Ads ${a4:,.2f}",
    ]
    if s4 > 0 and a4 / s4 > 0.15:
        lines.append("🟠 Etsy Ads above 15% of Etsy sales - check ROAS per listing in Shop Manager.")
    if aw > 0 and not rows:
        lines.append("⚠️ Ad spend this week but no orders.")

    vg = views_gained(pb, start, end)
    lines += ["", "Listings this week (views gained → units sold):"]
    if vg is None:
        lines.append("  views per listing: collecting daily snapshots since 2026-11-06, first full week from 16 Nov")
        for lid, n in by_listing.most_common(8):
            lines.append(f"  {n} sold · {titles.get(lid, lid)[:55]}")
    else:
        top = sorted(vg.items(), key=lambda kv: -kv[1])[:10]
        for lid, v in top:
            lines.append(f"  {v} views → {by_listing.get(lid, 0)} sold · {titles.get(lid, lid)[:55]}")
        sold_elsewhere = [(lid, n) for lid, n in by_listing.items() if lid not in dict(top)]
        for lid, n in sold_elsewhere[:5]:
            lines.append(f"  {vg.get(lid, 0)} views → {n} sold · {titles.get(lid, lid)[:55]}")
    if today.day <= 7:
        lines += ["", "📋 First Monday of the month: send me the Etsy Ads listing table (Shop Manager → Etsy Ads → "
                      "download) to check ROAS per listing - keep while ROAS > 2.5, switch off at $15+ spend with 0 orders."]
    return "\n".join(lines)


def SendEtsyWeekly(request):
    from NotificationRouting import notify
    args = getattr(request, "args", {}) or {}
    try:
        text = weekly_text(date.fromisoformat(args["today"]) if args.get("today") else None)
    except Exception as exc:
        text = f"🛍 Etsy weekly failed: {str(exc)[:300]}"
    if args.get("dry") != "1":
        notify("etsy-weekly", "AmzBot", text)
    return json.dumps({"text": text}), 200, {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}
