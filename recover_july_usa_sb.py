"""One-off targeted recovery: pulls ONLY Sponsored Brands for the USA profile
for the recoverable portion of July (2026-07-07 through 2026-07-31 - Amazon's
retention floor, confirmed live, is exactly 2026-07-07; July 1-6 is
permanently gone). Deliberately narrow (1 profile, 1 product) rather than the
full pull_and_store_campaign_stats sweep (29 profiles x 3 products), which
has been getting OOM-killed on this machine twice in a row - see CLAUDE.md's
July-retention-regression note. Insert-only, no delete step, since July SB
rows for USA are confirmed to be genuinely zero right now (nothing to lose)."""
import sys
import time

sys.path.insert(0, ".")

from AdsAuth import AD_PROFILES, ADS_REGION_ENDPOINTS, POCKETBASE_URL, POCKETBASE_ADMIN_EMAIL, POCKETBASE_ADMIN_PASSWORD
from AdsReporting import (
    AD_PRODUCTS,
    REPORT_POLL_DELAY_SECONDS,
    REPORT_POLL_ROUNDS,
    campaign_row_to_body,
    check_report_status,
    download_report_rows,
    pb_batch,
    refresh_access_token,
    request_campaign_report,
)
import requests

START_DATE = "2026-07-07"
END_DATE = "2026-07-31"
USA_PROFILE = {"profileId": "1649312585287580", "countryCode": "US", "currencyCode": "USD"}


def pb_auth():
    r = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    return r.json()["token"]


def main():
    pb_token = pb_auth()
    conn = requests.get(
        f"{POCKETBASE_URL}/api/collections/ads_connections/records?filter=region='USA'",
        headers={"Authorization": pb_token}, timeout=30,
    ).json()["items"][0]
    access_token = refresh_access_token("USA", conn["refresh_token"])
    client_id = AD_PROFILES["USA"]["client_id"]
    base_url = ADS_REGION_ENDPOINTS["NA"]
    sb_product = next(p for p in AD_PRODUCTS if p["key"] == "SB")

    print("Requesting SB report...")
    report_id = request_campaign_report(base_url, access_token, client_id, USA_PROFILE["profileId"], START_DATE, END_DATE, sb_product)
    print("report_id:", report_id)

    job = {
        "report_id": report_id,
        "base_url": base_url,
        "headers": {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": str(USA_PROFILE["profileId"]),
        },
    }

    download_url = None
    for attempt in range(REPORT_POLL_ROUNDS):
        status, url = check_report_status(job)
        print(f"poll {attempt+1}: {status}")
        if status == "done":
            download_url = url
            break
        time.sleep(REPORT_POLL_DELAY_SECONDS)

    if not download_url:
        print("FAILED: report never completed")
        sys.exit(1)

    rows = download_report_rows(download_url)
    print("rows downloaded:", len(rows))

    bodies = [campaign_row_to_body(USA_PROFILE, row, sb_product) for row in rows]
    ops = [
        {"method": "POST", "url": "/api/collections/ads_campaign_stats/records", "body": b}
        for b in bodies
    ]
    for i in range(0, len(ops), 50):
        pb_batch(pb_token, ops[i:i + 50])
        print(f"wrote batch {i}-{i+len(ops[i:i+50])}")

    print("DONE. Total rows written:", len(bodies))


if __name__ == "__main__":
    main()
