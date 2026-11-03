"""Backfill fba_storage_monthly (2026-11-01). Resumable via logs/storage_backfill_progress.json.
Usage: backfill_storage_fees.py YYYY-MM YYYY-MM [max_reports]"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backfill_promo_sales import months
from StorageFees import pull_month, REGIONS

PROGRESS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "storage_backfill_progress.json")
done = json.load(open(PROGRESS)) if os.path.exists(PROGRESS) else {}
limit = int(sys.argv[3]) if len(sys.argv) > 3 else 99
throttled = set()  # a region that hit QuotaExceeded is skipped for the rest of this run
for y, m in months(sys.argv[1], sys.argv[2]):
    for region in REGIONS:
        key = f"{y:04d}-{m:02d}|{region}"
        if done.get(key) or limit <= 0 or region in throttled:
            continue
        try:
            done[key] = pull_month(region, f"{y:04d}-{m:02d}")
        except Exception as exc:
            print(key, "failed:", str(exc)[:120], flush=True)
            throttled.add(region)
            continue
        json.dump(done, open(PROGRESS, "w"), indent=1)
        print(key, done[key], flush=True)
        limit -= 1
