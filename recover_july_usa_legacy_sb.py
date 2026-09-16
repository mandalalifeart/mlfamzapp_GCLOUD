"""One-off targeted recovery: legacy (non-SBv4) Sponsored Brands campaigns
for the USA profile, for a given day range - the same v2 hsa/campaigns/report
supplement pull_and_store_legacy_sb_stats does, but scoped to just one
profile so it can run without the OOM-kill issue the full sweep has been
hitting on this machine (see CLAUDE.md's July-retention-regression note).
Usage: python3 recover_july_usa_legacy_sb.py <start_date> <end_date>
"""
import sys

sys.path.insert(0, ".")

from AdsAuth import AD_PROFILES, ADS_REGION_ENDPOINTS, POCKETBASE_URL, POCKETBASE_ADMIN_EMAIL, POCKETBASE_ADMIN_PASSWORD
from AdsReporting import (
    LEGACY_CREATIVE_TYPES,
    legacy_row_to_body,
    pb_batch,
    pb_known_campaign_ids,
    poll_legacy_report,
    refresh_access_token,
    request_legacy_report,
)
import requests
from datetime import datetime, timedelta

USA_PROFILE = {"profileId": "1649312585287580", "countryCode": "US", "currencyCode": "USD"}


def pb_auth():
    r = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    return r.json()["token"]


def main():
    start_date, end_date = sys.argv[1], sys.argv[2]
    pb_token = pb_auth()
    conn = requests.get(
        f"{POCKETBASE_URL}/api/collections/ads_connections/records?filter=region='USA'",
        headers={"Authorization": pb_token}, timeout=30,
    ).json()["items"][0]
    access_token = refresh_access_token("USA", conn["refresh_token"])
    client_id = AD_PROFILES["USA"]["client_id"]
    base_url = ADS_REGION_ENDPOINTS["NA"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": client_id,
        "Amazon-Advertising-API-Scope": str(USA_PROFILE["profileId"]),
    }

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_strs = []
    d = start
    while d <= end:
        date_strs.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    total_written = 0
    total_spend = 0.0
    total_sales = 0.0
    for date_str in date_strs:
        for creative_type in LEGACY_CREATIVE_TYPES:
            try:
                known_ids = pb_known_campaign_ids(pb_token, USA_PROFILE["profileId"], "SPONSORED_BRANDS", date_str)
                report_id = request_legacy_report(base_url, access_token, client_id, USA_PROFILE["profileId"], date_str, creative_type)
                rows = poll_legacy_report(base_url, headers, report_id)
                bodies = [
                    legacy_row_to_body(USA_PROFILE, row, date_str)
                    for row in rows
                    if str(row.get("campaignId")) not in known_ids and (row.get("cost") or row.get("attributedSales14d"))
                ]
                ops = [{"method": "POST", "url": "/api/collections/ads_campaign_stats/records", "body": b} for b in bodies]
                for i in range(0, len(ops), 50):
                    pb_batch(pb_token, ops[i:i + 50])
                total_written += len(bodies)
                total_spend += sum(b["spend"] for b in bodies)
                total_sales += sum(b["sales"] for b in bodies)
                print(f"{date_str} ({creative_type}): {len(bodies)} rows")
            except Exception as exc:
                print(f"{date_str} ({creative_type}): ERROR {exc}")

    print(f"DONE. total rows: {total_written}, total spend: {round(total_spend,2)}, total sales: {round(total_sales,2)}")


if __name__ == "__main__":
    main()
