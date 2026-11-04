"""Daily cron job: requests the USA+DE sales report for the current month,
waits for it, and wet-updates PocketBase (sku_sales/country_sales) with it -
a fully automated version of the manual /update or /batch-update flow, so
this month's numbers stay fresh every day without anyone clicking through
the UI. On the 1st of the month, targets *last* month instead of this one,
since day 1 of a new month has no data of its own yet and this is the first
automated chance to close last month out with its truly final numbers.

Only 2 reports are ever needed, not one per marketplace - Amazon bundles all
of North America into the "usa" report and the whole EU region (including
the UK) into the "de" report, and every order is tagged by its own embedded
SalesChannel (see UpdateSkuSalesMonth.build_db_rows/DB_MARKETPLACE_MAP), so
ca/mex/uk/every individual EU country each already get their own
correctly-tagged rows for free. Confirmed live 2026-09-04 (228 real
amazon.co.uk orders already present in a single "de" report pull) - a UK
report was briefly, mistakenly requested as its own 3rd report alongside
usa/de; that would have double-counted every UK order (once from "de",
once from the redundant "uk" report) and was reverted before ever running.

Mirrors BatchUpdatePage.jsx's request -> poll -> update sequence exactly
(same endpoints, same 12-attempt/30s poll), just called from Python via the
already-running local API server instead of the browser - see CLAUDE.md's
"AmzBot: local job runner" section for why these 3 functions live there.

Only ever targets the current or previous calendar month, both always well
after the pipeline's July-2026 cutoff (see the "no automated pipeline may
touch pre-July-2026 history" standing rule) - this job never comes close to
that boundary, so no special-casing is needed here.
"""
import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import calendar

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from NotificationRouting import notify

LOCAL_API_BASE = os.environ.get("LOCAL_API_BASE", "https://amzapi.mandalalifeart.com")
# MlfReportReq/MlfReportGet are SP-API-only (no PocketBase dependency), so
# they run on GCP - see CLAUDE.md "AmzBot: local job runner".
GCLOUD_API_BASE = os.environ.get("GCLOUD_API_BASE", "https://us-central1-mlfamzapp.cloudfunctions.net")
LA_TZ = ZoneInfo("America/Los_Angeles")
JOB_NAME = "amzbot-daily-monthly-update"
APP_NAME = "amzbot"

POLL_MAX_ATTEMPTS = 12
POLL_RETRY_DELAY_SECONDS = 30


def get_target_month():
    """Today (day 1) -> last month. Any other day -> this month. All in the
    America/Los_Angeles timezone, matching every other month-boundary
    calculation in this app (App.jsx/BatchUpdatePage.jsx)."""
    now_la = datetime.now(LA_TZ)
    if now_la.day == 1:
        first_of_this_month = now_la.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        return last_month_end.year, last_month_end.month
    return now_la.year, now_la.month


def build_month_date_range(year, month):
    """Same rule as BatchUpdatePage.jsx's buildMonthDateRange: start of month
    at 00:00:00 LA time; end of month at 23:59:59.999 LA time, UNLESS this is
    the current in-progress month, in which case end_date is "now" instead."""
    now_la = datetime.now(LA_TZ)
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=LA_TZ)

    is_current_month = now_la.year == year and now_la.month == month
    if is_current_month:
        end = now_la
    else:
        next_month = datetime(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1, tzinfo=LA_TZ)
        end = next_month - timedelta(milliseconds=1)

    def to_iso_utc(dt):
        return dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    return to_iso_utc(start), to_iso_utc(end)


def report_api_base(marketplace):
    """AU reports run on the local server (its app isn't on the GCP copies)."""
    return LOCAL_API_BASE if marketplace == "au" else GCLOUD_API_BASE


def request_report(marketplace, start_date, end_date):
    resp = requests.post(
        f"{report_api_base(marketplace)}/MlfReportReq",
        json={"start_date": start_date, "end_date": end_date, "marketplace": marketplace},
        timeout=60,
    )
    data = resp.json()
    if not resp.ok or data.get("status") != "success":
        raise RuntimeError(f"{marketplace} report request failed: {data.get('message') or data}")
    report_req_id = (data.get("data") or {}).get("report_req_id")
    if not report_req_id:
        raise RuntimeError(f"{marketplace} report request missing report_req_id: {data}")
    return report_req_id


def poll_report_ready(marketplace, report_req_id):
    for attempt in range(1, POLL_MAX_ATTEMPTS + 1):
        try:
            resp = requests.post(
                f"{report_api_base(marketplace)}/MlfReportGet",
                json={"marketplace": marketplace, "report_req_id": report_req_id},
                timeout=60,
            )
            data = resp.json()
            status = data.get("status")
            payload_status = (data.get("data") or {}).get("payload")
            in_progress = status in ("IN_PROCESS", "IN_PROGRESS") or payload_status in ("IN_PROCESS", "IN_PROGRESS")

            if in_progress:
                if attempt == POLL_MAX_ATTEMPTS:
                    return False, f"{marketplace} report still processing after max attempts"
                time.sleep(POLL_RETRY_DELAY_SECONDS)
                continue

            if status == "success":
                return True, None
            return False, f"Unexpected {marketplace} report status: {status}"
        except Exception as exc:
            if attempt == POLL_MAX_ATTEMPTS:
                return False, f"{marketplace} report fetch failed: {exc}"
            time.sleep(POLL_RETRY_DELAY_SECONDS)
    return False, f"{marketplace} report timed out"


def run_update(report_ids, start_date, end_date, month, year):
    resp = requests.post(
        f"{LOCAL_API_BASE}/UpdateSkuSalesMonth",
        json={
            "reportIds": report_ids,
            "startDate": start_date,
            "endDate": end_date,
            "confirmMonth": month,
            "confirmYear": year,
            "dryRun": False,
        },
        timeout=300,
    )
    data = resp.json()
    if not resp.ok:
        raise RuntimeError(data.get("error") or f"HTTP {resp.status_code}")
    return data


# country_sales/UpdateSkuSalesMonth marketplace code -> label + the local
# currency its amounts are in (never converted per-country - see CLAUDE.md
# "Per-marketplace currency"). "eu" is the combined, EUR-converted EU row.
MARKETPLACE_LABELS = {
    "usa": ("USA", "USD"), "ca": ("Canada", "CAD"), "mex": ("Mexico", "MXN"),
    "uk": ("UK", "GBP"), "de": ("Germany", "EUR"), "fr": ("France", "EUR"),
    "it": ("Italy", "EUR"), "es": ("Spain", "EUR"), "nl": ("NL", "EUR"),
    "be": ("Belgium", "EUR"), "ie": ("Ireland", "EUR"), "se": ("SE", "SEK"),
    "pl": ("PL", "PLN"), "jp": ("Japan", "JPY"), "au": ("Australia", "AUD"), "eu": ("EU total", "EUR"),
}


def format_sales_section(title, rows_by_marketplace, with_ppc=False):
    """rows_by_marketplace: {code: {"QUANTITY", "AMOUNT", optional PPC_*}}.
    Sorted by units, "eu" combined total always last."""
    lines = [title]
    real = {k: v for k, v in rows_by_marketplace.items() if v.get("QUANTITY") or v.get("AMOUNT")}
    if not real:
        lines.append("  (no sales recorded)")
        return "\n".join(lines)
    order = sorted((c for c in real if c != "eu"), key=lambda c: -real[c]["QUANTITY"])
    if "eu" in real:
        order.append("eu")
    for code in order:
        vals = real[code]
        label, currency = MARKETPLACE_LABELS.get(code, (code.upper(), ""))
        line = f"  {label}: {vals['QUANTITY']:,} units / {currency} {vals['AMOUNT']:,.2f} sales"
        if with_ppc:
            spend = float(vals.get("PPC_SPEND") or 0)
            tacos = f"{spend / vals['AMOUNT'] * 100:.1f}%" if vals["AMOUNT"] else "n/a"
            line += f" / ad spend {spend:,.2f} (TACoS {tacos})"
        lines.append(line)
    return "\n".join(lines)


def fetch_promotion_names():
    try:
        from GetPromotionSales import fetch_promotion_names as _f, pb_authenticate as _auth
        return _f(_auth())
    except Exception:
        return {}


def _usd(amount, mp, rates):
    cur = MARKETPLACE_LABELS.get(mp, ("", "USD"))[1]
    r = 1.0 if cur == "USD" else (rates or {}).get(cur)
    return amount / r if r else None


def format_promo_section(title, rows, names, rates=None):
    """rows: [{marketplace, promotion_id, kind, units, sales, discount}] -
    one line per (promotion, marketplace), most units first."""
    lines = [title]
    if not rows:
        lines.append("  (none)")
        return "\n".join(lines)
    merged = {}
    for r in rows:
        k = (r["promotion_id"], r["marketplace"])
        m = merged.setdefault(k, {"kind": r.get("kind"), "units": 0, "discount": 0.0})
        m["units"] += int(r.get("units") or 0)
        m["discount"] += float(r.get("discount") or 0)
    for (pid, mp), m in sorted(merged.items(), key=lambda kv: -kv[1]["units"]):
        label, currency = MARKETPLACE_LABELS.get(mp, (mp.upper(), ""))
        name = (names.get(pid) or {}).get("name") or (f"Coupon {pid[:12]}" if m["kind"] == "coupon" else pid)
        icon = "🎟" if m["kind"] == "coupon" else "🏷"
        lines.append(f"  {icon} {name} ({label}): {m['units']} units, {currency} {m['discount']:,.2f} off")
    if rates is not None:
        usd = [_usd(m["discount"], mp, rates) for (_, mp), m in merged.items()]
        lines.append(f"  Σ Total: {sum(m['units'] for m in merged.values())} units, "
                     f"USD {sum(u for u in usd if u is not None):,.2f} off")
    return "\n".join(lines)


def format_business_section(title, by_mp, rates=None):
    """by_mp: {mp: {orders, units, sales}} - Amazon Business (B2B) orders."""
    lines = [title]
    real = {k: v for k, v in (by_mp or {}).items() if v.get("units")}
    if not real:
        lines.append("  (none)")
        return "\n".join(lines)
    for mp, v in sorted(real.items(), key=lambda kv: -kv[1]["units"]):
        label, currency = MARKETPLACE_LABELS.get(mp, (mp.upper(), ""))
        lines.append(f"  🏢 {label}: {v['orders']} orders, {v['units']} units, {currency} {v['sales']:,.2f}")
    if rates is not None:
        usd = [_usd(v["sales"], mp, rates) for mp, v in real.items()]
        lines.append(f"  Σ Total: {sum(v['orders'] for v in real.values())} orders, "
                     f"{sum(v['units'] for v in real.values())} units, USD {sum(u for u in usd if u is not None):,.2f}")
    return "\n".join(lines)


def build_sales_digest_text(result, year, month, marketplaces_used):
    """Same shape as the Ads Daily Digest (DailyAdsPerformanceDigest.py):
    latest day + month-to-date, per marketplace. This job runs just after
    LA midnight with the report covering the month through "now", so LA
    calendar-yesterday is always complete in the report."""
    yesterday = (datetime.now(LA_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    daily = (result.get("dailyCountryTotals") or {}).get(yesterday, {})
    month_rows = {r["MARKETPLACE"]: r for r in (result.get("updatedCountryRows") or [])}

    is_closed_month = datetime.now(LA_TZ).day == 1
    month_title = (
        f"Month closed ({year}-{month:02d}, final):" if is_closed_month
        else f"Month to date ({year}-{month:02d}-01 to {yesterday}):"
    )
    parts = [
        f"🛒 Sales Daily Digest - {datetime.now(LA_TZ).strftime('%Y-%m-%d')}",
        "",
        format_sales_section(f"Yesterday ({yesterday}):", daily),
        "",
        format_sales_section(month_title, month_rows, with_ppc=True),
        "",
        f"(sales = item price excl. tax/shipping, local currency; reports: {marketplaces_used})",
    ]
    # Coupons & promotions used (added 2026-10-21, per the user).
    names = fetch_promotion_names()
    parts += [
        "",
        "Coupons & promotions",
        format_promo_section(f"Yesterday ({yesterday}):", (result.get("dailyPromoTotals") or {}).get(yesterday, []), names),
        format_promo_section("Month to date:" if not is_closed_month else "Month:", result.get("promoRows") or [], names),
    ]
    return "\n".join(parts)


# Approved by the user 2026-10-24 (replaces the separate Sales + Ads digests).
COMBINED_DIGEST = True

# --- Combined Sales + Ads digest (2026-10-24, per the user: "merge those
# updates to a single update, with a clear view table, include both acos and
# tacos"). Replaces the separate 🛒 Sales and 📊 Ads daily digests.
ADS_COUNTRY_TO_CODE = {"US": "usa", "CA": "ca", "MX": "mex", "UK": "uk", "GB": "uk", "DE": "de", "FR": "fr",
                       "IT": "it", "ES": "es", "NL": "nl", "BE": "be", "IE": "ie", "SE": "se", "PL": "pl",
                       "AU": "au", "JP": "jp"}
SHORT_LABEL = {"usa": "US$", "ca": "CA$", "mex": "MX$", "uk": "UK£", "de": "DE€", "fr": "FR€", "it": "IT€",
               "es": "ES€", "nl": "NL€", "be": "BE€", "ie": "IE€", "se": "SEK", "pl": "PLN", "au": "AU$", "jp": "JP¥"}


def _ads_by_code(token, start, end):
    from DailyAdsPerformanceDigest import fetch_country_totals
    out = {}
    for cc, v in fetch_country_totals(token, start, end).items():
        code = ADS_COUNTRY_TO_CODE.get(cc, cc.lower())
        b = out.setdefault(code, {"spend": 0.0, "sales": 0.0})
        b["spend"] += v["spend"]
        b["sales"] += v["sales"]
    return out


def _pct(a, b):
    if not b:
        return "∞" if a else "-"
    return f"{100 * a / b:.0f}%"


def _num(v):
    return f"{v:,.0f}" if abs(v) < 10000 else f"{v / 1000:.1f}k" if abs(v) < 100000 else f"{v / 1000:.0f}k"


def format_table(sales, ads, rates):
    """sales: {code: {QUANTITY, AMOUNT}}, ads: {code: {spend, sales}} (local
    currency). One monospace row per marketplace + a USD total row."""
    codes = [c for c in set(sales) | set(ads) if c != "eu"
             and ((sales.get(c) or {}).get("QUANTITY") or (sales.get(c) or {}).get("AMOUNT") or (ads.get(c) or {}).get("spend"))]
    codes.sort(key=lambda c: -((sales.get(c) or {}).get("AMOUNT", 0) / (rates.get(MARKETPLACE_LABELS.get(c, ("", "USD"))[1]) or 1)))
    head = f"{'':4}{'Units':>5}{'Sales':>7}{'AdSpd':>6}{'AdSal':>6}{'ACOS':>5}{'TACOS':>6}"
    lines = [head]
    tot = {"u": 0, "s": 0.0, "sp": 0.0, "as": 0.0}
    for c in codes:
        u = int((sales.get(c) or {}).get("QUANTITY") or 0)
        s = float((sales.get(c) or {}).get("AMOUNT") or 0)
        sp = float((ads.get(c) or {}).get("spend") or 0)
        a = float((ads.get(c) or {}).get("sales") or 0)
        cur = MARKETPLACE_LABELS.get(c, ("", "USD"))[1]
        r = 1.0 if cur == "USD" else rates.get(cur)
        if r:
            tot["u"] += u; tot["s"] += s / r; tot["sp"] += sp / r; tot["as"] += a / r
        lines.append(f"{SHORT_LABEL.get(c, c.upper()[:3]):4}{u:>5}{_num(s):>7}{_num(sp):>6}{_num(a):>6}{_pct(sp, a):>5}{_pct(sp, s):>6}")
    lines.append("-" * len(head))
    lines.append(f"{'ALL$':4}{tot['u']:>5}{_num(tot['s']):>7}{_num(tot['sp']):>6}{_num(tot['as']):>6}"
                 f"{_pct(tot['sp'], tot['as']):>5}{_pct(tot['sp'], tot['s']):>6}")
    return "\n".join(lines)


def _esc(t):
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_combined_digest_text(result, year, month, marketplaces_used):
    from GetMarketplaceSalesSummary import fetch_usd_rates
    from AdsReporting import last_recorded_date, pb_authenticate
    now_la = datetime.now(LA_TZ)
    yesterday = (now_la - timedelta(days=1)).strftime("%Y-%m-%d")
    is_closed_month = now_la.day == 1
    month_start = f"{year}-{month:02d}-01"
    daily_sales = (result.get("dailyCountryTotals") or {}).get(yesterday, {})
    month_sales = {r["MARKETPLACE"]: r for r in (result.get("updatedCountryRows") or [])}
    token = pb_authenticate()
    last_ads = last_recorded_date(token) or yesterday
    ads_day = _ads_by_code(token, yesterday, yesterday)
    month_end = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}" if is_closed_month else yesterday
    ads_month = _ads_by_code(token, month_start, month_end)
    currencies = {MARKETPLACE_LABELS.get(c, ("", "USD"))[1] for c in set(daily_sales) | set(month_sales) | set(ads_day) | set(ads_month)}
    rates = fetch_usd_rates(currencies)

    parts = [f"🛒📊 <b>Sales &amp; Ads - {now_la.strftime('%Y-%m-%d')}</b>", "",
             f"<b>Yesterday ({yesterday})</b>",
             f"<pre>{_esc(format_table(daily_sales, ads_day, rates))}</pre>"]
    if last_ads < yesterday:
        parts.append(f"⚠️ ads data only up to {last_ads}")
    parts += [f"<b>{'Month closed' if is_closed_month else 'Month to date'} ({month_start} to {month_end})</b>",
              f"<pre>{_esc(format_table(month_sales, ads_month, rates))}</pre>",
              "<i>Local currency per row, ALL$ = everything in USD. AdSpd/AdSal = Ads spend / Ads sales (7-day). "
              "ACOS = AdSpd/AdSal, TACOS = AdSpd/Sales (all sales, excl. tax/shipping).</i>"]
    names = fetch_promotion_names()
    promo_day = (result.get("dailyPromoTotals") or {}).get(yesterday, [])
    promo_month = result.get("promoRows") or []
    biz = result.get("businessTotals") or {}
    extra = {MARKETPLACE_LABELS.get(r["marketplace"], ("", "USD"))[1] for r in promo_day + promo_month}
    extra |= {MARKETPLACE_LABELS.get(mp, ("", "USD"))[1] for d in [(biz.get("daily") or {}).get(yesterday) or {}, biz.get("month") or {}] for mp in d}
    if extra - set(rates) - {"USD"}:
        rates = {**rates, **fetch_usd_rates(extra - set(rates) - {"USD"})}
    promo = [format_promo_section(f"Yesterday:", promo_day, names, rates),
             format_promo_section("Month to date:" if not is_closed_month else "Month:", promo_month, names, rates)]
    try:
        from AsinAnalytics import pb_list_all
        pend = pb_list_all(token, "order_items", f'order_status = "Pending" && purchase_date >= "{month_start}" '
                                                 f'&& purchase_date <= "{month_end}" && marketplace != "mcf"',
                           fields="purchase_date")
        n_y = sum(1 for r in pend if r["purchase_date"][:10] == yesterday)
        if pend:
            promo.append(f"⏳ {len(pend)} order lines still pending ({n_y} from yesterday) - Amazon shows their "
                         f"coupons only once shipped, so these numbers grow over the next days.")
    except Exception:
        pass
    parts += ["", "<b>Coupons &amp; promotions</b>", _esc("\n".join(promo))]
    parts += ["", "<b>Business (Amazon Business) orders</b>",
              _esc("\n".join([format_business_section("Yesterday:", (biz.get("daily") or {}).get(yesterday), rates),
                               format_business_section("Month to date:" if not is_closed_month else "Month:", biz.get("month"), rates)]))]
    parts.append(f"<i>reports: {_esc(marketplaces_used)}</i>")
    return "\n".join(parts)


def in_target_window():
    """This system's cron only fires on its own local timezone (Asia/Jerusalem,
    confirmed via `man 5 crontab` - it has no per-job CRON_TZ support), but the
    user wants this job to run at 00:05 America/Los_Angeles specifically - and
    LA/Jerusalem's offset shifts by an hour whenever only one of the two is in
    DST, which would silently drift a fixed Jerusalem-time cron entry by an
    hour for a few weeks each spring/fall. Instead, cron fires this script
    every 5 minutes (always) and this checks the real LA wall-clock time,
    only proceeding on the one tick that lands at LA 00:00-00:04 - self-timed
    like the rest of this project's cron jobs, no twice-yearly DST edit ever
    needed. LA's UTC offset is always a whole number of hours, so its
    midnight always falls exactly on a 5-minute boundary somewhere, and
    exactly one 5-minute cron tick per day lands in this window."""
    now_la = datetime.now(LA_TZ)
    return now_la.hour == 0 and now_la.minute < 5


# Coupons/promotions only appear on an order line once it ships (Pending lines
# have no <Promotion> - confirmed 2026-11-02), and the 1st closes last month
# while its last days are still pending. So on days 2-8 last month is read
# again after the digest, silently, to pick up those discounts (per the user).
REFRESH_PREVIOUS_UNTIL_DAY = 8


def refresh_previous_month():
    now_la = datetime.now(LA_TZ)
    if not 2 <= now_la.day <= REFRESH_PREVIOUS_UNTIL_DAY:
        return
    last = now_la.replace(day=1) - timedelta(days=1)
    year, month = last.year, last.month
    start_date, end_date = build_month_date_range(year, month)
    report_ids, errors = {}, []
    for marketplace in ("usa", "de", "au"):
        try:
            report_ids[marketplace] = request_report(marketplace, start_date, end_date)
        except Exception as exc:
            errors.append(str(exc))
    ready = {}
    for marketplace, rid in report_ids.items():
        ok, error = poll_report_ready(marketplace, rid)
        if ok:
            ready[marketplace] = rid
        else:
            errors.append(error)
    # Only rewrite the month when USA and DE are both there (a partial run
    # would drop that region's rows for the month).
    if "usa" not in ready or "de" not in ready:
        notify(JOB_NAME, APP_NAME, f"Refresh of {year}-{month:02d} (pending-order coupons) skipped: {'; '.join(errors)}",
               is_error=True, telegram=False)
        return
    try:
        result = run_update(ready, start_date, end_date, month, year)
        print(f"refreshed {year}-{month:02d}: {result.get('orderItems')}")
    except Exception as exc:
        notify(JOB_NAME, APP_NAME, f"Refresh of {year}-{month:02d} (pending-order coupons) FAILED: {exc}",
               is_error=True, telegram=False)


def main():
    if not in_target_window():
        return
    try:
        send_daily_update()
    finally:
        refresh_previous_month()


def send_daily_update():
    year, month = get_target_month()
    start_date, end_date = build_month_date_range(year, month)

    report_ids = {}
    request_errors = []
    for marketplace in ("usa", "de", "au"):
        try:
            report_ids[marketplace] = request_report(marketplace, start_date, end_date)
        except Exception as exc:
            request_errors.append(str(exc))

    if not report_ids:
        text = f"Monthly update for {year}-{month:02d} FAILED - both report requests failed: {'; '.join(request_errors)}"
        notify(JOB_NAME, APP_NAME, text, is_error=True, bot="amazon")
        return

    ready_report_ids = {}
    poll_errors = list(request_errors)
    for marketplace, report_req_id in report_ids.items():
        ok, error = poll_report_ready(marketplace, report_req_id)
        if ok:
            ready_report_ids[marketplace] = report_req_id
        else:
            poll_errors.append(error)

    if not ready_report_ids:
        text = f"Monthly update for {year}-{month:02d} FAILED - no report became ready: {'; '.join(poll_errors)}"
        notify(JOB_NAME, APP_NAME, text, is_error=True, bot="amazon")
        return

    try:
        result = run_update(ready_report_ids, start_date, end_date, month, year)
    except Exception as exc:
        text = f"Monthly update for {year}-{month:02d} FAILED at the write step: {exc}"
        notify(JOB_NAME, APP_NAME, text, is_error=True, bot="amazon")
        return

    marketplaces_used = ", ".join(sorted(ready_report_ids.keys()))
    warning_note = f" - {result.get('asinWarning')}" if result.get("asinWarning") else ""
    skipped_note = f" (skipped: {'; '.join(poll_errors)})" if poll_errors else ""
    try:
        if not COMBINED_DIGEST:
            raise RuntimeError("combined digest not enabled yet")
        text = build_combined_digest_text(result, year, month, marketplaces_used)
        if warning_note or skipped_note:
            text += f"\n\n⚠️{_esc(warning_note + skipped_note)}"
        parse_mode = "HTML"
    except Exception as exc:  # fall back to the plain sales digest
        text = build_sales_digest_text(result, year, month, marketplaces_used)
        if COMBINED_DIGEST:
            text += f"\n\n(combined table failed: {exc})"
        if warning_note or skipped_note:
            text += f"\n\n⚠️{warning_note}{skipped_note}"
        parse_mode = None
    # @baba_social_bot since 2026-10-21 (per the user, with the daily Ads digest).
    notify(JOB_NAME, APP_NAME, text, is_error=bool(poll_errors and len(ready_report_ids) < len(report_ids) + len(request_errors)), bot="amazon",
           parse_mode=parse_mode)


if __name__ == "__main__":
    main()
