"""Backfill order_items (2026-11-01) month by month from the orders report.
Writes ONLY order_items. Resumable via logs/order_items_backfill_progress.json.
Usage: backfill_order_items.py YYYY-MM YYYY-MM [max_months] [regions=usa,de]"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from run_daily_monthly_update import build_month_date_range
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backfill_promo_sales import fetch, months
from OrderItems import parse_order_items, write_order_items

PROGRESS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "order_items_backfill_progress.json")


def main():
    done = json.load(open(PROGRESS)) if os.path.exists(PROGRESS) else {}
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else 99
    regions = (sys.argv[4] if len(sys.argv) > 4 else "usa,de").split(",")
    for y, m in months(sys.argv[1], sys.argv[2]):
        start, end = build_month_date_range(y, m)
        for region in regions:
            key = f"{y:04d}-{m:02d}|{region}"
            if done.get(key) or limit <= 0:
                continue
            xml = fetch(region, start, end)
            rows = parse_order_items(xml, region) if xml else []
            res = write_order_items(rows, region, y, m)
            done[key] = res
            json.dump(done, open(PROGRESS, "w"), indent=1)
            print(key, res, flush=True)
            limit -= 1


if __name__ == "__main__":
    main()
