"""Wise (wise.com) accounting integration - business account "Mandala Life ART OU".

Pulls GET /v1/transfers - the full history of money MOVEMENTS this account
has initiated (currency conversions, payouts to suppliers/recipients,
balance top-ups sent between currencies) - plus a balance snapshot via
GET /v4/profiles/{id}/balances. Both need only the personal API token,
confirmed live 2026-09-09.

This does NOT cover incoming deposits (e.g. an Amazon settlement landing
directly in the USD balance) - that needs the real per-balance ledger via
GET /v1/profiles/{id}/balance-statements/{balanceId}/statement.json, which
is gated by Wise's Strong Customer Authentication (SCA: sign a challenge
token with a registered RSA key). Investigated live 2026-09-09: correctly
signed the challenge two independent ways (Python `cryptography` and raw
`openssl dgst -sha256 -sign`, matching Wise's documented method exactly),
with the key confirmed registered and the token bumped to Full access, and
it was rejected every time regardless. Wise's own docs (pasted by the user)
confirm personal API tokens are only "a sub-set of Wise Platform API
capabilities" - this endpoint is very likely restricted to full OAuth 2.0
Wise Platform partner integrations (partnership agreement + mTLS), not
reachable via a personal token at all. Not worth pursuing further for a
personal-token integration; abandoned rather than left half-wired.

The transfers pull fully replaces the collection's rows for the profile each
run (delete-then-insert, same convention as AmazonFinances.py's expense
events) - transfer status changes over time (e.g. outgoing_payment_sent ->
completed), so a plain append would go stale. wise_balances is the one
exception - snapshots are appended, not replaced, so a balance-over-time
view is possible later; volume is trivial (one row per currency per pull).
"""
import os
import re
from datetime import datetime, timezone

import requests

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_BATCH_SIZE = int(os.environ.get("POCKETBASE_BATCH_SIZE", "50"))

TRANSFERS_COLLECTION = "wise_transfers"
BALANCES_COLLECTION = "wise_balances"

WISE_API_TOKEN = os.environ.get("WISE_API_TOKEN", "")
# Narrowly scoped, not the shared ADMIN_KEY - same reasoning as
# FINANCES_REFRESH_KEY/OPS_DASHBOARD_KEY/BID_APPLY_KEY.
WISE_REFRESH_KEY = os.environ.get("WISE_REFRESH_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

WISE_API_BASE = "https://api.wise.com"

# Only the business profile ("Mandala Life ART OU") for now - the personal
# profile on this same token has ~$50 total across 2 currencies and isn't
# what "accounting" means here (per the user, 2026-09-09).
PROFILES = {
    "business": int(os.environ.get("WISE_BUSINESS_PROFILE_ID", "0") or 0),
}


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    import json
    return json.dumps(body), status, cors_headers()


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"PocketBase auth failed: HTTP {response.status_code} - {response.text}")
    return response.json()["token"]


def pb_batch(token, batch_requests):
    """See AmazonFinances.pb_batch's docstring - a 200 on the outer /api/batch
    call is not proof every individual op inside it succeeded; this raises if
    any op didn't return 2xx."""
    for i in range(0, len(batch_requests), POCKETBASE_BATCH_SIZE):
        chunk = batch_requests[i:i + POCKETBASE_BATCH_SIZE]
        response = requests.post(
            f"{POCKETBASE_URL}/api/batch",
            headers={"Authorization": token},
            json={"requests": chunk},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PocketBase batch failed: HTTP {response.status_code} - {response.text}")
        results = response.json()
        failed = [r for r in results if not (200 <= r.get("status", 0) < 300)]
        if failed:
            raise RuntimeError(f"PocketBase batch had {len(failed)}/{len(results)} failed op(s): {failed[:3]}")


def pb_list_ids(token, collection, filter_str):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params={"filter": filter_str, "fields": "id", "perPage": 200, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        ids.extend(item["id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


def wise_headers():
    return {"Authorization": f"Bearer {WISE_API_TOKEN}"}


def wise_iso(naive_str):
    """Wise's /v1/transfers 'created' field is a naive 'YYYY-MM-DD HH:MM:SS'
    string with no timezone - confirmed live 2026-09-09 (matches the account's
    own activity feed timestamps, which ARE UTC-suffixed for the same events),
    so treated as UTC."""
    if not naive_str:
        return None
    dt = datetime.strptime(naive_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_all_transfers(profile_id):
    transfers = []
    offset = 0
    limit = 200
    while True:
        response = requests.get(
            f"{WISE_API_BASE}/v1/transfers",
            headers=wise_headers(),
            params={"profile": profile_id, "offset": offset, "limit": limit},
            timeout=30,
        )
        response.raise_for_status()
        page = response.json()
        if not page:
            break
        transfers.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return transfers


_HTML_TAG_RE = re.compile(r"<[^>]+>")


def fetch_recent_counterparty_names(profile_id):
    """/v1/transfers has no recipient name field (just a numeric targetAccount
    id) - the activity feed (/v1/profiles/{id}/activities) has a human title
    per TRANSFER activity (e.g. "<strong>BBk Rostock</strong>") that IS the
    counterparty name. Returns {transfer_id_str: name}.

    Only the most recent ~100 activities (size=100 is the real max - 150+
    returns 400, confirmed live 2026-09-09) - this endpoint's `cursor` didn't
    actually advance in testing (repeated identical first page regardless of
    the cursor value passed back), so there's no reliable way to page further
    back. Good enough for what the UI actually needs (recent transfers), not
    a full historical join back to 2021."""
    response = requests.get(
        f"{WISE_API_BASE}/v1/profiles/{profile_id}/activities",
        headers=wise_headers(),
        params={"size": 100},
        timeout=30,
    )
    response.raise_for_status()
    names = {}
    for activity in response.json().get("activities", []):
        resource = activity.get("resource") or {}
        if resource.get("type") != "TRANSFER" or not resource.get("id"):
            continue
        title = _HTML_TAG_RE.sub("", activity.get("title") or "").strip()
        if title:
            names[resource["id"]] = title
    return names


def write_transfers(pb_token, profile_tag, profile_id, transfers, counterparty_names):
    existing_ids = pb_list_ids(pb_token, TRANSFERS_COLLECTION, f'profile_tag = "{profile_tag}"')
    ops = [
        {"method": "DELETE", "url": f"/api/collections/{TRANSFERS_COLLECTION}/records/{rid}"}
        for rid in existing_ids
    ]
    for t in transfers:
        created = wise_iso(t.get("created"))
        if not created:
            continue
        ops.append({
            "method": "POST",
            "url": f"/api/collections/{TRANSFERS_COLLECTION}/records",
            "body": {
                "transfer_id": t["id"],
                "profile_id": profile_id,
                "profile_tag": profile_tag,
                "status": t.get("status", ""),
                "reference": t.get("reference", "") or "",
                "counterparty": counterparty_names.get(str(t["id"]), ""),
                "created_at": created,
                "source_currency": t.get("sourceCurrency", ""),
                "source_value": t.get("sourceValue") or 0,
                "target_currency": t.get("targetCurrency", ""),
                "target_value": t.get("targetValue") or 0,
                "rate": t.get("rate") or 0,
                "customer_transaction_id": t.get("customerTransactionId", "") or "",
            },
        })
    pb_batch(pb_token, ops)
    return len(ops)


def fetch_balances(profile_id):
    response = requests.get(
        f"{WISE_API_BASE}/v4/profiles/{profile_id}/balances",
        headers=wise_headers(),
        params={"types": "STANDARD"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def write_balances(pb_token, profile_tag, profile_id, balances):
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ops = []
    for b in balances:
        ops.append({
            "method": "POST",
            "url": f"/api/collections/{BALANCES_COLLECTION}/records",
            "body": {
                "profile_id": profile_id,
                "profile_tag": profile_tag,
                "currency": b.get("currency", ""),
                "amount": (b.get("amount") or {}).get("value") or 0,
                "reserved_amount": (b.get("reservedAmount") or {}).get("value") or 0,
                "snapshot_at": now_iso,
            },
        })
    if ops:
        pb_batch(pb_token, ops)
    return len(ops)


def UpdateWiseFinances(request):
    """Pulls the business profile's full transfer history + a fresh balance
    snapshot and writes wise_transfers/wise_balances. No date-range params -
    pulled in full every time (~4,000 transfers, this account) rather than
    tracking incremental state, since transfer status can change after the
    fact (e.g. outgoing_payment_sent -> completed)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if WISE_REFRESH_KEY and (not hasattr(request, "args") or request.args.get("key") != WISE_REFRESH_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    if not WISE_API_TOKEN:
        return json_response({"error": "WISE_API_TOKEN not configured"}, 500)

    pb_token = pb_authenticate()
    results = {}
    try:
        for profile_tag, profile_id in PROFILES.items():
            if not profile_id:
                continue
            transfers = fetch_all_transfers(profile_id)
            counterparty_names = fetch_recent_counterparty_names(profile_id)
            transfers_written = write_transfers(pb_token, profile_tag, profile_id, transfers, counterparty_names)
            balances = fetch_balances(profile_id)
            balances_written = write_balances(pb_token, profile_tag, profile_id, balances)
            results[profile_tag] = {"transfersWritten": transfers_written, "balancesWritten": balances_written}
        return json_response({"regions": results})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__, "partialResults": results}, 500)


def GetWiseFinancesSummary(request):
    """Read-only: aggregates already-stored wise_transfers/wise_balances into
    a frontend-friendly shape - no Wise API calls, no key needed."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    try:
        pb_token = pb_authenticate()

        transfers = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{TRANSFERS_COLLECTION}/records",
                headers={"Authorization": pb_token},
                params={"sort": "-created_at", "perPage": 500, "page": page},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            transfers.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1

        latest_balances = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{BALANCES_COLLECTION}/records",
                headers={"Authorization": pb_token},
                params={"sort": "-snapshot_at", "perPage": 500, "page": page},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            latest_balances.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1

        seen_currency = set()
        current_balances = []
        for b in latest_balances:
            key = (b["profile_tag"], b["currency"])
            if key in seen_currency:
                continue
            seen_currency.add(key)
            current_balances.append(b)

        months = {}
        for t in transfers:
            created_at = t.get("created_at") or ""
            if len(created_at) < 7:
                continue
            year, month = int(created_at[:4]), int(created_at[5:7])
            key = (t["profile_tag"], t.get("source_currency", ""), year, month)
            bucket = months.setdefault(key, {
                "profileTag": t["profile_tag"], "currency": t.get("source_currency", ""),
                "year": year, "month": month, "totalOut": 0, "count": 0,
            })
            if t.get("status") in ("completed", "outgoing_payment_sent"):
                bucket["totalOut"] += t.get("source_value") or 0
                bucket["count"] += 1
        month_rows = sorted(months.values(), key=lambda m: (m["profileTag"], m["year"], m["month"]), reverse=True)

        return json_response({
            "transfers": transfers[:500],
            "currentBalances": current_balances,
            "monthlyOutflow": month_rows,
            "note": (
                "transfers/monthlyOutflow cover money this account sent/converted, not incoming "
                "deposits (e.g. an Amazon settlement landing directly in the balance) - see "
                "WiseFinances.py module docstring. currentBalances reflects the latest snapshot."
            ),
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
