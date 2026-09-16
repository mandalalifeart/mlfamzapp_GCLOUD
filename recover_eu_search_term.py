"""One-off targeted recovery: search-term stats for just the real "Boho
Paradise UK" EU profiles (9 countries), scoped down from the full 30-profile
sweep which has been repeatedly killed on this machine (see CLAUDE.md). Reuses
the tested submit/poll/write functions from AdsSearchTermReporting.py, just
with a filtered `connections` list.
Usage: python3 recover_eu_search_term.py <start_date> <end_date>
"""
import sys

sys.path.insert(0, ".")

from AdsAuth import pb_authenticate
from AdsSearchTermReporting import (
    pb_list_search_term_stats_ids,
    poll_and_store_search_term_jobs,
    submit_search_term_report_jobs,
)
from AdsReporting import pb_batch, pb_list_connected

POCKETBASE_BATCH_SIZE = 50
POCKETBASE_ADS_SEARCH_TERM_COLLECTION = "ads_search_term_stats"

# The real "Boho Paradise UK" profile IDs discovered 2026-09-04 (see
# CLAUDE.md's ads_connections/EU-profile-list fix note) - deliberately
# excludes the old dormant "Mandala Life ART Shop" duplicates and non-EU
# profiles (USA/CA/MX/JP/SG), to keep this recovery run small and fast.
EU_PROFILE_IDS = {
    "1262925395560957",  # DE
    "1167111969385560",  # FR
    "2967253577783675",  # IT
    "548812940689391",   # ES
    "429682557125413",   # UK
    "964809683265675",   # NL
    "3309595935395677",  # SE
    "2956421459998329",  # PL
    "2564371016909939",  # BE
    "3760349253492161",  # IE
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
        existing_ids = pb_list_search_term_stats_ids(pb_token, profile_id, start_date, end_date)
        ops = [{"method": "DELETE", "url": f"/api/collections/{POCKETBASE_ADS_SEARCH_TERM_COLLECTION}/records/{rid}"} for rid in existing_ids]
        for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
            pb_batch(pb_token, ops[i:i + POCKETBASE_BATCH_SIZE])
        if existing_ids:
            print(f"cleared {len(existing_ids)} existing rows for profile {profile_id}")

    jobs = submit_search_term_report_jobs(scoped_connections, start_date, end_date, errors)
    print(f"submitted {len(jobs)} report jobs")
    rows_written = poll_and_store_search_term_jobs(pb_token, jobs, errors)

    print(f"DONE. rowsWritten={rows_written}, errors={errors}")


if __name__ == "__main__":
    main()
