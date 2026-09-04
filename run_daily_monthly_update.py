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

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from NotificationRouting import notify

LOCAL_API_BASE = os.environ.get("LOCAL_API_BASE", "https://amzapi.mandalalifeart.com")
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


def request_report(marketplace, start_date, end_date):
    resp = requests.post(
        f"{LOCAL_API_BASE}/MlfReportReq",
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
                f"{LOCAL_API_BASE}/MlfReportGet",
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


def main():
    if not in_target_window():
        return

    year, month = get_target_month()
    start_date, end_date = build_month_date_range(year, month)

    report_ids = {}
    request_errors = []
    for marketplace in ("usa", "de"):
        try:
            report_ids[marketplace] = request_report(marketplace, start_date, end_date)
        except Exception as exc:
            request_errors.append(str(exc))

    if not report_ids:
        text = f"Monthly update for {year}-{month:02d} FAILED - both report requests failed: {'; '.join(request_errors)}"
        notify(JOB_NAME, APP_NAME, text, is_error=True)
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
        notify(JOB_NAME, APP_NAME, text, is_error=True)
        return

    try:
        result = run_update(ready_report_ids, start_date, end_date, month, year)
    except Exception as exc:
        text = f"Monthly update for {year}-{month:02d} FAILED at the write step: {exc}"
        notify(JOB_NAME, APP_NAME, text, is_error=True)
        return

    marketplaces_used = ", ".join(sorted(ready_report_ids.keys()))
    warning_note = f" - {result.get('asinWarning')}" if result.get("asinWarning") else ""
    skipped_note = f" (skipped: {'; '.join(poll_errors)})" if poll_errors else ""
    text = (
        f"Monthly update for {year}-{month:02d} done ({marketplaces_used}) - "
        f"{result.get('parsedOrderRows')} order rows, {result.get('dbRowsCount')} SKU rows, "
        f"{result.get('countryRowsCount')} country rows written{warning_note}{skipped_note}"
    )
    notify(JOB_NAME, APP_NAME, text, is_error=bool(poll_errors and len(ready_report_ids) < 2))


if __name__ == "__main__":
    main()
