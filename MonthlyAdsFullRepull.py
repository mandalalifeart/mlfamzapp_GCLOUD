"""Monthly cron job (1st of the month only): re-pulls the FULL previous
calendar month, from scratch, across all 4 Amazon Ads reporting pipelines
(campaign, keyword, search-term, advertised-product).

Why this exists on top of the daily catch-up jobs: Amazon's own attribution
window (an order can convert up to ~7-14 days after the click that drove it)
means a day's numbers, when first pulled the day after it happened, can
genuinely undercount sales/orders that only show up in Amazon's reporting
later. The daily catch-up logic (last_recorded_date+1 -> yesterday) only
ever fills forward and never revisits a date once it has any row, so the
last week or so of a month can stay permanently slightly low unless
something re-pulls it after attribution has settled. Doing that once, a
full month after the fact, is the standard fix.

Each Update* function already supports an explicit start_date/end_date pair
that bypasses its own catch-up logic and does a full delete-then-reinsert
for that exact range (see e.g. AdsSearchTermReporting.pull_and_store_search_
term_stats) - this script just supplies "the whole previous month" as that
range, for all 4 pipelines, then retries once if any pipeline reported
errors (a throttled Amazon report, for example) rather than silently
leaving a gap for a whole month.

Deliberately scheduled well before run_daily_monthly_update.py's own
1st-of-month "last month" run (which merges Ads spend/sales into
country_sales.ppc_spend/ppc_sales) - see the crontab comment - so that
merge reads the freshly-corrected ads_campaign_stats, not attribution-
lagged numbers from when the month was still in progress.
"""
import sys
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from NotificationRouting import notify

JERUSALEM_TZ = ZoneInfo("Asia/Jerusalem")
JOB_NAME = "amzbot-monthly-ads-full-repull"
APP_NAME = "amzbot"


class FakeArgs(dict):
    def get(self, key, default=None, type=None):
        value = super().get(key, default)
        if type is not None and value is not None:
            try:
                value = type(value)
            except (TypeError, ValueError):
                return default
        return value


class FakeRequest:
    def __init__(self, args):
        self.method = "GET"
        self.args = FakeArgs(args)

    def get_json(self, silent=True):
        return None


def previous_month_range():
    now = datetime.now(JERUSALEM_TZ)
    first_of_this_month = now.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start.strftime("%Y-%m-%d"), last_month_end.strftime("%Y-%m-%d"), last_month_start.year, last_month_start.month


PIPELINES = [
    ("UpdateAdsCampaignStats", "AdsReporting"),
    ("UpdateAdsKeywordStats", "AdsKeywordReporting"),
    ("UpdateAdsSearchTermStats", "AdsSearchTermReporting"),
    ("UpdateAdsAdvertisedProductStats", "AdsAdvertisedProductReporting"),
]


def run_one(function_name, module_name, start_date, end_date):
    module = __import__(module_name, fromlist=[function_name])
    handler = getattr(module, function_name)
    args = {"start_date": start_date, "end_date": end_date, "key": os.environ.get("ADMIN_KEY", "")}
    result = handler(FakeRequest(args))
    if isinstance(result, tuple):
        body = result[0]
    else:
        body = result
    return body if isinstance(body, dict) else {}


def run_with_retry(function_name, module_name, start_date, end_date):
    result = run_one(function_name, module_name, start_date, end_date)
    errors = result.get("errors") or []
    if errors:
        # One retry - a full re-pull of the same range (delete+reinsert
        # again), since these functions have no per-profile retry granularity.
        result = run_one(function_name, module_name, start_date, end_date)
    return result


def main():
    start_date, end_date, year, month = previous_month_range()
    now = datetime.now(JERUSALEM_TZ)
    if now.day != 1:
        return

    summaries = []
    any_errors = False
    for function_name, module_name in PIPELINES:
        try:
            result = run_with_retry(function_name, module_name, start_date, end_date)
        except Exception as exc:
            summaries.append(f"{function_name}: FAILED - {exc}")
            any_errors = True
            continue
        errors = result.get("errors") or []
        rows = result.get("rowsWritten")
        if errors:
            any_errors = True
            summaries.append(f"{function_name}: {rows} rows, {len(errors)} error(s) after retry - {'; '.join(errors[:3])}")
        else:
            summaries.append(f"{function_name}: {rows} rows, clean")

    text = f"Monthly full Ads re-pull for {year}-{month:02d} done:\n" + "\n".join(summaries)
    notify(JOB_NAME, APP_NAME, text, is_error=any_errors)


if __name__ == "__main__":
    main()
