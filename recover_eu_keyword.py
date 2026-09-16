"""One-off targeted recovery: keyword/targeting stats for just the real
"Boho Paradise UK" EU profiles - see recover_eu_search_term.py for the same
pattern and rationale (scoped down from the full 30-profile sweep, which
gets killed on this machine for heavier report types - see CLAUDE.md).
Usage: python3 recover_eu_keyword.py <start_date> <end_date>
"""
import sys

sys.path.insert(0, ".")

from AdsAuth import pb_authenticate
from AdsKeywordReporting import (
    pb_list_keyword_stats_ids,
    poll_and_store_keyword_jobs,
    submit_keyword_report_jobs,
)
from AdsReporting import pb_batch, pb_list_connected

POCKETBASE_BATCH_SIZE = 50
POCKETBASE_ADS_KEYWORD_COLLECTION = "ads_keyword_stats"

EU_PROFILE_IDS = {
    "1262925395560957", "1167111969385560", "2967253577783675", "548812940689391",
    "429682557125413", "964809683265675", "3309595935395677", "2956421459998329",
    "2564371016909939", "3760349253492161",
}


def main():
    start_date, end_date = sys.argv[1], sys.argv[2]
    pb_token = pb_authenticate()
    connections = pb_list_connected(pb_token)

    scoped_connections = []
    for connection in connections:
        profiles = [p for p in (connection.get("profiles") or []) if str(p.get("profileId")) in EU_PROFILE_IDS]
        if profiles:
            scoped = dict(connection)
            scoped["profiles"] = profiles
            scoped_connections.append(scoped)

    total_profiles = sum(len(c["profiles"]) for c in scoped_connections)
    print(f"Scoped to {total_profiles} EU profiles across {len(scoped_connections)} connection(s)")

    errors = []
    profile_ids = {str(p.get("profileId")) for c in scoped_connections for p in c["profiles"]}
    for profile_id in profile_ids:
        existing_ids = pb_list_keyword_stats_ids(pb_token, profile_id, start_date, end_date)
        ops = [{"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ADS_KEYWORD_COLLECTION}/records/{rid}"} for rid in existing_ids]
        for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
            pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])
        if existing_ids:
            print(f"cleared {len(existing_ids)} existing rows for profile {profile_id}")

    jobs = submit_keyword_report_jobs(scoped_connections, start_date, end_date, errors)
    print(f"submitted {len(jobs)} report jobs")
    rows_written = poll_and_store_keyword_jobs(pb_token, jobs, errors)

    print(f"DONE. rowsWritten={rows_written}, errors={errors}")


if __name__ == "__main__":
    main()
