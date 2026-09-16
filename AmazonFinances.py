"""Amazon SP-API Payments/Finances integration - USA + all EU marketplaces (see CLAUDE.md).

Data model: Amazon groups every fee/charge/refund into "financial event groups",
each one a real settlement period (usually biweekly) that ends in either an
ongoing "Open" period or a "Closed" period that gets disbursed (paid out) to the
seller's bank account. Rather than pulling raw financial events over an arbitrary
date range (list_financial_events's PostedAfter/PostedBefore caps at 180 days,
and several event types - confirmed live 2026-09-07, e.g. ServiceFeeEventList -
carry no PostedDate at all, so there is no reliable way to bucket them by month
from a flat pull), this pulls events PER SETTLEMENT GROUP via
list_financial_events_by_group_id, using that group's own FinancialEventGroupStart
as the date anchor. This also ties every dollar of "expenses" directly back to the
real payout it was deducted from, which is the more natural fit for a payments
page anyway - each settlement already has its own start/end and everything that
happened in it is unambiguous once scoped that way.

Categorization: the 5 currently-real, high-volume event types (ShipmentEventList,
RefundEventList, ServiceFeeEventList, ProductAdsPaymentEventList,
RemovalShipmentEventList) get precise category labels. Every other event list
type falls back to a generic recursive money-object walker (Amazon consistently
shapes every dollar amount in this API as {"CurrencyCode":..., "CurrencyAmount":...})
bucketed under "Other: <ListName>" - so nothing is ever silently dropped even for
an event type this file doesn't know about yet, it just shows up under a visible
generic label instead of a precise one.
"""
import os
import time
from datetime import datetime, timedelta, timezone

import requests

# Reused from SocialMarketting's @baba_social_bot - same credential pair
# EtsyMcfFulfillment.py already uses, per the user's standing preference
# (2026-09-09) to route payment notifications there rather than a new bot.
TELEGRAM_BOT_TOKEN = os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("MCF_TELEGRAM_CHAT_ID", "")

POCKETBASE_URL = os.environ["POCKETBASE_URL"].rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ["POCKETBASE_ADMIN_EMAIL"]
POCKETBASE_ADMIN_PASSWORD = os.environ["POCKETBASE_ADMIN_PASSWORD"]
POCKETBASE_SETTLEMENTS_COLLECTION = os.environ.get("POCKETBASE_SETTLEMENTS_COLLECTION", "amazon_settlements")
# One row per (group_id, marketplace, category) - NOT pre-aggregated across
# groups. Amazon's real rate limit for these Finances endpoints is 0.5 req/sec
# (confirmed live 2026-09-07 via the x-amzn-RateLimit-Limit response header),
# so a multi-year backfill is a long, many-hundred-call operation; writing each
# settlement group's own contribution immediately after processing it (rather
# than accumulating everything in memory and writing once at the very end)
# means a crash/rate-limit failure partway through only loses that one group's
# work, not the whole run. GetPaymentsExpensesSummary aggregates these into
# monthly totals at read time instead.
POCKETBASE_EXPENSE_COLLECTION = os.environ.get("POCKETBASE_EXPENSE_COLLECTION", "amazon_expense_events")
POCKETBASE_BATCH_SIZE = int(os.environ.get("POCKETBASE_BATCH_SIZE", "50"))
# Narrowly scoped (not the shared ADMIN_KEY) since the Payments page's
# "Refresh from Amazon" button holds this in the public frontend bundle -
# same reasoning as OPS_DASHBOARD_KEY/BID_APPLY_KEY.
FINANCES_REFRESH_KEY = os.environ.get("FINANCES_REFRESH_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")

# list_financial_event_groups caps at 365 days per call, list_financial_events_by_group_id
# has no such cap (it's scoped to one settlement, never more than ~30 days of real data).
GROUPS_MAX_WINDOW_DAYS = 360

CHARGE_CATEGORY_MAP = {
    "Principal": "Sales Revenue",
    "Tax": "Tax Collected",
    "ShippingCharge": "Shipping Charged to Buyer",
    "ShippingTax": "Tax Collected",
    "GiftWrap": "Shipping Charged to Buyer",
    "GiftWrapTax": "Tax Collected",
}
FEE_CATEGORY_MAP = {
    "Commission": "Referral Fee",
    "RefundCommission": "Referral Fee",
    "FBAPerOrderFulfillmentFee": "FBA Fulfillment Fee",
    "FBAPerUnitFulfillmentFee": "FBA Fulfillment Fee",
    "FBAWeightBasedFee": "FBA Fulfillment Fee",
    "FixedClosingFee": "Other Selling Fees",
    "VariableClosingFee": "Other Selling Fees",
    "DigitalServicesFee": "Other Selling Fees",
    "GiftwrapChargeback": "Other Selling Fees",
    "ShippingChargeback": "Other Selling Fees",
}
SERVICE_FEE_CATEGORY_MAP = {
    "FBAStorageFee": "FBA Storage Fee",
    "FBALongTermStorageFee": "FBA Storage Fee",
    "Subscription": "Subscription Fee",
    "FBACustomerReturnPerUnitFee": "FBA Returns Processing Fee",
    "AmazonUpstreamProcessingFee": "AWD Fee",
    "AmazonUpstreamStorageTransportationFee": "AWD Fee",
}


# Amazon's real MarketplaceName strings on ShipmentEventList/RefundEventList
# (confirmed live for USA: "Amazon.com" - EU names inferred from Amazon's
# standard domain-per-marketplace convention, matched case-insensitively).
# ServiceFeeEventList/ProductAdsPaymentEventList/RemovalShipmentEventList and
# any generically-handled event type carry no per-event marketplace, so those
# fall back to CURRENCY_FALLBACK_TAG (the settlement group's own currency) -
# still real and complete, just at settlement-currency granularity (e.g. one
# combined "eu" bucket for the eurozone) rather than per-country.
MARKETPLACE_NAME_TO_TAG = {
    "amazon.com": "usa",
    "amazon.ca": "ca",
    "amazon.com.mx": "mex",
    "amazon.de": "de",
    "amazon.fr": "fr",
    "amazon.it": "it",
    "amazon.es": "es",
    "amazon.co.uk": "uk",
    "amazon.nl": "nl",
    "amazon.se": "se",
    "amazon.pl": "pl",
    "amazon.com.be": "be",
    "amazon.ie": "ie",
}
CURRENCY_FALLBACK_TAG = {
    "USD": "usa",
    "MXN": "mex",
    "CAD": "ca",
    "EUR": "eu",
    "GBP": "uk",
    "PLN": "pl",
    "SEK": "se",
}


def marketplace_tag_from_name(marketplace_name, default_tag):
    if not marketplace_name:
        return default_tag
    return MARKETPLACE_NAME_TO_TAG.get(marketplace_name.strip().lower(), default_tag)


def service_fee_fallback_category(fee_type):
    lowered = fee_type.lower()
    if "storage" in lowered:
        return "FBA Storage Fee"
    if "removal" in lowered or "disposal" in lowered:
        return "FBA Removal Fee"
    if "return" in lowered:
        return "FBA Returns Processing Fee"
    if "subscription" in lowered:
        return "Subscription Fee"
    return f"Other Service Fee ({fee_type})"


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
    """A 200 on the outer /api/batch call is NOT proof every operation inside
    it succeeded - confirmed live 2026-09-07: PocketBase returns per-request
    results as an array, each with its own status, and this code used to only
    check the outer HTTP status. A real backfill run reported 233 successful
    settlement writes that were never actually queryable afterward - same
    "clean 200 isn't proof of success" pitfall already documented elsewhere in
    this project for Amazon's Listings API, just for PocketBase's batch
    endpoint instead. Now raises if ANY individual op in the batch didn't
    return a 2xx status."""
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


def pb_list_ids(token, collection, filter_str, fields="id"):
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token},
            params={"filter": filter_str, "fields": fields, "perPage": 200, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        ids.extend(item["id"] for item in data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return ids


# "eu" pulls with ANY EU marketplace enum return the whole unified EU seller
# account's settlements/events (all EU marketplaces bundled under one
# AMAZON_SELLER_ID_EU grant, same account structure AmazonListingOps.py
# already relies on) - confirmed live 2026-09-07 (one EU credential pull
# returned PLN and GBP settlement groups together, no per-country call
# needed). DE is just the representative marketplace passed to the client.
CREDENTIAL_SCOPES = {
    "usa": {"refresh_env": "REFRESH_TOKEN_USA", "id_env": "CLIENT_ID_USA", "secret_env": "CLIENT_SECRET_USA", "marketplace": "US"},
    "eu": {"refresh_env": "REFRESH_TOKEN_EU", "id_env": "CLIENT_ID_EU", "secret_env": "CLIENT_SECRET_EU", "marketplace": "DE"},
}


def finances_client(credential_scope="usa"):
    from sp_api.api import Finances
    from sp_api.base import Marketplaces
    scope = CREDENTIAL_SCOPES[credential_scope]
    credentials = {
        "refresh_token": os.environ[scope["refresh_env"]],
        "lwa_app_id": os.environ[scope["id_env"]],
        "lwa_client_secret": os.environ[scope["secret_env"]],
    }
    return Finances(credentials=credentials, marketplace=getattr(Marketplaces, scope["marketplace"]))


# Amazon's real documented rate limit for these Finances endpoints, confirmed
# live via the x-amzn-RateLimit-Limit response header (0.5 req/sec) - NOT
# enforced client-side by the sp_api library (checked: no retry/backoff or
# rate-limiting decorator on these calls), so calling faster than this for
# long enough burns through the burst allowance and then gets a real 429
# (RequestThrottled) that the library just raises straight through. An
# earlier, faster pacing (0.3-0.4s) hit exactly this: burned the burst, then
# raised uncaught inside a long backfill loop with nothing written yet,
# silently losing the whole run's progress.
FINANCES_MIN_INTERVAL_SECONDS = 2.1
_last_finances_call_at = [0.0]


def paced_call(fn, *args, **kwargs):
    """Calls fn(*args, **kwargs), enforcing >=FINANCES_MIN_INTERVAL_SECONDS
    since the last Finances API call, with a few retries (longer backoff each
    time) for both throttling AND transient network errors (ReadTimeout,
    ConnectionError, etc. - confirmed live 2026-09-07: a plain requests
    ReadTimeout calling Amazon mid-backfill, unrelated to rate limiting,
    propagated uncaught out of the ONE fetch_all_groups call per date window
    that isn't inside the per-group try/except in pull_and_store_financial_data,
    silently aborting that whole scope's backfill with zero indication why).
    Only a non-transient error (bad input, auth failure, etc.) raises straight
    through without retrying."""
    TRANSIENT_HINTS = ("QuotaExceeded", "429", "RequestThrottled", "ReadTimeout", "ConnectionError", "Timeout", "Connection aborted")
    for attempt in range(5):
        elapsed = time.time() - _last_finances_call_at[0]
        if elapsed < FINANCES_MIN_INTERVAL_SECONDS:
            time.sleep(FINANCES_MIN_INTERVAL_SECONDS - elapsed)
        try:
            result = fn(*args, **kwargs)
            _last_finances_call_at[0] = time.time()
            return result
        except Exception as exc:
            _last_finances_call_at[0] = time.time()
            msg = str(exc) or exc.__class__.__name__
            is_transient = any(hint in msg or hint in exc.__class__.__name__ for hint in TRANSIENT_HINTS)
            if attempt < 4 and is_transient:
                time.sleep(8 * (attempt + 1))
                continue
            raise


def money(obj):
    return (obj or {}).get("CurrencyAmount") or 0


def sum_money_amounts(node):
    """Recursively sums every {"CurrencyCode":..., "CurrencyAmount":...} object
    found anywhere inside node - Amazon shapes every dollar amount in this API
    this same way, so this is a safe generic fallback for any event type this
    file doesn't have a precise category mapping for yet."""
    total = 0
    if isinstance(node, dict):
        if "CurrencyAmount" in node and "CurrencyCode" in node:
            return node.get("CurrencyAmount") or 0
        for v in node.values():
            total += sum_money_amounts(v)
    elif isinstance(node, list):
        for v in node:
            total += sum_money_amounts(v)
    return total


HANDLED_EVENT_LISTS = {
    "ShipmentEventList", "RefundEventList", "ServiceFeeEventList",
    "ProductAdsPaymentEventList", "RemovalShipmentEventList",
}


def categorize_group_events(events, bucket, unmapped_list_names, default_tag):
    """bucket: {(marketplace_tag, category): amount}. default_tag: the settlement
    group's own currency-derived tag, used for every event type that carries no
    per-event MarketplaceName of its own."""
    def add(tag, category, amount):
        if amount:
            key = (tag, category)
            bucket[key] = bucket.get(key, 0) + amount

    for ev in events.get("ShipmentEventList", []) or []:
        tag = marketplace_tag_from_name(ev.get("MarketplaceName"), default_tag)
        for item in ev.get("ShipmentItemList", []) or []:
            for c in item.get("ItemChargeList", []) or []:
                add(tag, CHARGE_CATEGORY_MAP.get(c["ChargeType"], f"Other Charge ({c['ChargeType']})"), money(c.get("ChargeAmount")))
            for f in item.get("ItemFeeList", []) or []:
                add(tag, FEE_CATEGORY_MAP.get(f["FeeType"], f"Other Fee ({f['FeeType']})"), money(f.get("FeeAmount")))
        for f in ev.get("ShipmentFeeList", []) or []:
            add(tag, FEE_CATEGORY_MAP.get(f["FeeType"], f"Other Fee ({f['FeeType']})"), money(f.get("FeeAmount")))

    for ev in events.get("RefundEventList", []) or []:
        tag = marketplace_tag_from_name(ev.get("MarketplaceName"), default_tag)
        total = 0
        for item in ev.get("ShipmentItemAdjustmentList", []) or []:
            for c in item.get("ItemChargeAdjustmentList", []) or []:
                total += money(c.get("ChargeAmount"))
            for f in item.get("ItemFeeAdjustmentList", []) or []:
                total += money(f.get("FeeAmount"))
        add(tag, "Refunds", total)

    for ev in events.get("ServiceFeeEventList", []) or []:
        for f in ev.get("FeeList", []) or []:
            fee_type = f["FeeType"]
            category = SERVICE_FEE_CATEGORY_MAP.get(fee_type) or service_fee_fallback_category(fee_type)
            add(default_tag, category, money(f.get("FeeAmount")))

    for ev in events.get("ProductAdsPaymentEventList", []) or []:
        add(default_tag, "Advertising", money(ev.get("transactionValue")))

    for ev in events.get("RemovalShipmentEventList", []) or []:
        for item in ev.get("RemovalShipmentItemList", []) or []:
            add(default_tag, "FBA Removal Fee", money(item.get("FeeAmount")))
            add(default_tag, "FBA Liquidation Revenue", money(item.get("Revenue")))
            add(default_tag, "Tax Collected", money(item.get("TaxAmount")))

    for list_name, items in events.items():
        if list_name in HANDLED_EVENT_LISTS or not isinstance(items, list) or not items:
            continue
        label = list_name.replace("EventList", "").strip() or list_name
        add(default_tag, f"Other: {label}", sum_money_amounts(items))
        unmapped_list_names.add(list_name)


def fetch_all_group_events(client, group_id):
    events = {}
    next_token = None
    while True:
        kwargs = {"NextToken": next_token} if next_token else {}
        resp = paced_call(client.list_financial_events_by_group_id, group_id, **kwargs)
        payload = resp.payload
        for list_name, items in (payload.get("FinancialEvents") or {}).items():
            if isinstance(items, list):
                events.setdefault(list_name, []).extend(items)
        next_token = payload.get("NextToken")
        if not next_token:
            break
    return events


def fetch_all_groups(client, start_iso, end_iso):
    groups = []
    next_token = None
    while True:
        kwargs = {"NextToken": next_token} if next_token else {
            "FinancialEventGroupStartedAfter": start_iso,
            "FinancialEventGroupStartedBefore": end_iso,
        }
        resp = paced_call(client.list_financial_event_groups, **kwargs)
        payload = resp.payload
        groups.extend(payload.get("FinancialEventGroupList", []))
        next_token = payload.get("NextToken")
        if not next_token:
            break
    return groups


def group_period_start(group):
    return group.get("FinancialEventGroupStart") or group.get("FundTransferDate")


def fetch_existing_settlement(pb_token, group_id):
    response = requests.get(
        f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SETTLEMENTS_COLLECTION}/records",
        headers={"Authorization": pb_token},
        params={"filter": f'group_id = "{group_id}"', "perPage": 1},
        timeout=30,
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    return items[0] if items else None


def send_payout_telegram(group, settlement_tag, currency, amount, fund_transfer_status, group_bucket=None):
    """Fires once per settlement group the first time its status is seen as
    "Closed" - see write_settlement's notified_paid handling. fund_transfer_status
    distinguishes what actually happened:
    - "Failed": a real payout attempt that did NOT go through - flagged
      distinctly since this needs the user's attention, not just an FYI.
    - "Unknown": confirmed live 2026-09-10 this is what Amazon uses for an
      internal cross-account balance adjustment (e.g. a negative NL balance
      covered by funds from the DE account) - no real bank transfer happens,
      so it's worded as a balance adjustment rather than a payout/charge.
    - "Succeeded"/"NoFundsDisbursed"/anything else: the normal payout
      (positive amount) or card-charge (negative amount) cases.

    Per the user (2026-09-09): no technical fields (trace ID / account tail)
    - instead a financial breakdown (income/expense categories) of what's
    actually in the settlement, using the same group_bucket categorize_group_events
    already computed for write_expenses. group_bucket is None when the
    events call failed (e.g. a group old enough to be past Amazon's shorter
    events-retention window) - the message still sends, just without a
    breakdown, rather than being silently skipped."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if fund_transfer_status == "Failed":
        header = f"\U0001f6a8 Amazon payout FAILED — {settlement_tag.upper()}"
    elif fund_transfer_status == "Unknown":
        header = f"⚖️ Amazon balance adjustment — {settlement_tag.upper()}"
    elif amount >= 0:
        header = f"\U0001f4b0 Amazon payout received — {settlement_tag.upper()}"
    else:
        header = f"\U0001f4b3 Amazon charged your card — {settlement_tag.upper()}"
    lines = [
        header,
        f"Period: {(group.get('FinancialEventGroupStart') or '')[:10]} → {(group.get('FinancialEventGroupEnd') or '')[:10] or 'ongoing'}",
        f"Paid out: {(group.get('FundTransferDate') or '')[:10]}",
        "",
    ]
    if group_bucket:
        # group_bucket keys are (marketplace_tag, category) - a single
        # settlement is usually one marketplace, so just sum by category.
        by_category = {}
        for (_, category), cat_amount in group_bucket.items():
            by_category[category] = by_category.get(category, 0) + cat_amount
        for category, cat_amount in sorted(by_category.items(), key=lambda kv: -kv[1]):
            sign = "+" if cat_amount >= 0 else "-"
            lines.append(f"{category}: {sign}{abs(cat_amount):.2f} {currency}")
        lines.append("")
    else:
        lines.append("(category breakdown unavailable for this period)")
        lines.append("")
    lines.append(f"Net: {amount:.2f} {currency}")
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": "\n".join(lines)},
        timeout=15,
    )


def write_settlement(pb_token, group, settlement_tag, currency):
    """Writes just the settlement row (upserted by group_id) - deliberately
    separate from write_expenses/the events fetch, and called unconditionally
    for every group regardless of whether its events can be fetched. Confirmed
    live 2026-09-07: list_financial_events_by_group_id has a MUCH shorter real
    retention window than list_financial_event_groups - old groups (multiple
    years back) list fine (their summary totals/dates/status are still
    available) but fail their events call with "data requested exceeds data
    retention period". Before this split, that failure (raised from inside the
    same try block that built the settlement write) meant the settlement row -
    which we DO still have real data for - was silently never written either,
    for every group old enough to hit this. Payments/disbursement history is
    if anything the more important half of this page for old periods, so it
    must not depend on expense-event availability.

    Returns just_paid (bool) - True the first time a group is seen fully
    disbursed - notified_paid is carried forward from the previous row (this
    function always deletes-then-reinserts) so a settlement already
    notified about never re-triggers on a later re-pull of the same window.
    The caller sends the actual Telegram notification (after the category
    breakdown is available from write_expenses - see send_payout_telegram),
    not this function."""
    group_id = group["FinancialEventGroupId"]
    original = group.get("OriginalTotal") or {}
    converted = group.get("ConvertedTotal") or {}
    status = group.get("ProcessingStatus", "")
    fund_transfer_status = group.get("FundTransferStatus", "")

    existing = fetch_existing_settlement(pb_token, group_id)
    already_notified = bool(existing and existing.get("notified_paid"))
    # "Closed" alone is the real finality signal, not fund_transfer_status ==
    # "Succeeded" specifically - confirmed live 2026-09-10: a real NL-negative-
    # balance-covered-by-DE-funds adjustment (an internal cross-account netting,
    # not a bank payout) landed as Closed/Unknown and was silently never
    # notified about under the old Succeeded-only rule, even though the user
    # explicitly wanted to know about it. "Failed" (a real payout attempt that
    # didn't go through) is equally real news, arguably more urgent than a
    # normal payout - see send_payout_telegram's handling. Only "Processing"
    # is excluded (still not actually final; 1 historical occurrence) so it
    # waits for a later pull to resolve to a real final status first.
    just_paid = status == "Closed" and fund_transfer_status != "Processing" and not already_notified
    notified_paid = already_notified or just_paid

    ops = []
    if existing:
        ops.append({"method": "DELETE", "url": f"/api/collections/{POCKETBASE_SETTLEMENTS_COLLECTION}/records/{existing['id']}"})
    ops.append({
        "method": "POST",
        "url": f"/api/collections/{POCKETBASE_SETTLEMENTS_COLLECTION}/records",
        "body": {
            "group_id": group_id,
            "marketplace": settlement_tag,
            "currency": currency,
            "original_amount": original.get("CurrencyAmount", 0),
            "converted_amount_usd": converted.get("CurrencyAmount", original.get("CurrencyAmount", 0) if currency == "USD" else None),
            "status": status,
            "fund_transfer_status": fund_transfer_status,
            "fund_transfer_date": group.get("FundTransferDate", ""),
            "period_start": group.get("FinancialEventGroupStart", ""),
            "period_end": group.get("FinancialEventGroupEnd", ""),
            "trace_id": group.get("TraceId", ""),
            "account_tail": group.get("AccountTail", ""),
            "notified_paid": notified_paid,
        },
    })
    pb_batch(pb_token, ops)
    return just_paid


def write_expenses(pb_token, group_id, group_bucket, month, year):
    """Writes this group's own (marketplace, category) -> amount contributions
    (upserted by group_id) - only called once its events were fetched
    successfully; see write_settlement's docstring for why this is kept
    separate rather than one combined write."""
    existing_ids = pb_list_ids(pb_token, POCKETBASE_EXPENSE_COLLECTION, f'group_id = "{group_id}"')
    ops = [
        {"method": "DELETE", "url": f"/api/collections/{POCKETBASE_EXPENSE_COLLECTION}/records/{rid}"}
        for rid in existing_ids
    ]
    for (tag, category), amount in group_bucket.items():
        ops.append({
            "method": "POST",
            "url": f"/api/collections/{POCKETBASE_EXPENSE_COLLECTION}/records",
            "body": {
                "group_id": group_id, "marketplace": tag, "month": month, "year": year,
                "category": category, "amount": round(amount, 2),
            },
        })
    if ops:
        pb_batch(pb_token, ops)


def pull_and_store_financial_data(start_iso, end_iso, credential_scope="usa"):
    client = finances_client(credential_scope)
    pb_token = pb_authenticate()

    unmapped_list_names = set()
    settlements_written = 0
    events_processed = 0
    events_unavailable = 0  # old groups whose settlement we captured but whose
    # per-group event detail has aged out of Amazon's shorter retention window
    # for that endpoint (see write_settlement's docstring) - expected/normal
    # for old history, not a real error, tracked separately from group_errors.
    group_errors = []
    window_errors = []

    # Each <=360-day window is its own independent unit, same reasoning as the
    # per-group isolation below - a window that fails even after paced_call's
    # retries (a longer real outage) is recorded and skipped rather than
    # losing every other window's already-fetched/already-written progress in
    # a multi-year backfill. Windows are walked NEWEST-first (recent history,
    # which is actually usable/complete, lands first) rather than oldest-first
    # - older groups increasingly hit the events-retention wall above, so
    # oldest-first spent most of a long backfill grinding through guaranteed
    # per-group event failures before ever reaching the useful recent years.
    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    window_end = end_dt
    while window_end > start_dt:
        window_start = max(window_end - timedelta(days=GROUPS_MAX_WINDOW_DAYS), start_dt)
        window_start_str = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        window_end_str = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            window_groups = fetch_all_groups(client, window_start_str, window_end_str)
        except Exception as exc:
            window_errors.append(f"{window_start_str}..{window_end_str}: {exc}")
            window_end = window_start
            continue

        for group in window_groups:
            group_id = group["FinancialEventGroupId"]
            period_start = group_period_start(group)
            if not period_start:
                continue
            dt = datetime.fromisoformat(period_start.replace("Z", "+00:00"))
            month, year = dt.month, dt.year

            original = group.get("OriginalTotal") or {}
            currency = original.get("CurrencyCode", "")
            settlement_tag = CURRENCY_FALLBACK_TAG.get(currency, currency.lower() or "unknown")

            # The settlement itself is written unconditionally - it doesn't
            # depend on the (shorter-retention) events endpoint below, and a
            # write failure here (throttling/network error that exhausted
            # paced_call's retries) is recorded and skipped rather than
            # aborting the rest of a long backfill. A skipped group is simply
            # retried by running the same date range again later (idempotent:
            # both writes are upserted by group_id).
            try:
                just_paid = write_settlement(pb_token, group, settlement_tag, currency)
                settlements_written += 1
            except Exception as exc:
                group_errors.append(f"{group_id} (settlement): {exc}")
                continue

            group_bucket = None
            try:
                group_bucket = {}
                events = fetch_all_group_events(client, group_id)
                categorize_group_events(events, group_bucket, unmapped_list_names, settlement_tag)
                write_expenses(pb_token, group_id, group_bucket, month, year)
                events_processed += 1
            except Exception as exc:
                group_bucket = None
                if "retention period" in str(exc):
                    events_unavailable += 1
                else:
                    group_errors.append(f"{group_id} (events): {exc}")

            if just_paid:
                send_payout_telegram(
                    group, settlement_tag, currency, original.get("CurrencyAmount", 0),
                    group.get("FundTransferStatus", ""), group_bucket,
                )

        window_end = window_start

    return {
        "settlementsWritten": settlements_written,
        "eventsProcessed": events_processed,
        "eventsUnavailable": events_unavailable,
        "groupErrors": group_errors,
        "windowErrors": window_errors,
        "unmappedEventListTypes": sorted(unmapped_list_names),
    }


def UpdateAmazonFinances(request):
    """Pulls settlement groups + their events and writes amazon_settlements /
    amazon_expense_events, for USA and/or EU (all EU marketplaces come from one
    unified EU credential pull - see CREDENTIAL_SCOPES). Default range is a
    rolling 40-day lookback (covers ~2-3 settlement periods, cheap to run daily);
    pass start_date/end_date (YYYY-MM-DD) explicitly for a wider backfill (each
    call still internally chunks the groups pull into <=360-day windows).
    region: "usa" | "eu" | omitted (both, one after another)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if FINANCES_REFRESH_KEY and (not hasattr(request, "args") or request.args.get("key") != FINANCES_REFRESH_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    start_date = request.args.get("start_date") if hasattr(request, "args") else None
    end_date = request.args.get("end_date") if hasattr(request, "args") else None
    region = (request.args.get("region") if hasattr(request, "args") else None) or None
    now = datetime.now(timezone.utc)
    end_iso = (
        datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if end_date
        else now - timedelta(minutes=5)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    start_iso = (
        datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if start_date
        else now - timedelta(days=40)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    scopes = [region] if region in CREDENTIAL_SCOPES else list(CREDENTIAL_SCOPES.keys())
    results = {}
    try:
        for scope in scopes:
            results[scope] = pull_and_store_financial_data(start_iso, end_iso, scope)
        return json_response({"startDate": start_iso, "endDate": end_iso, "regions": results})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__, "partialResults": results}, 500)


def GetPaymentsExpensesSummary(request):
    """Read-only: aggregates already-stored amazon_settlements/amazon_expense_summary
    into a frontend-friendly shape - no Amazon API calls, no ADMIN_KEY needed."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    try:
        pb_token = pb_authenticate()

        settlements = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_SETTLEMENTS_COLLECTION}/records",
                headers={"Authorization": pb_token},
                params={"sort": "-period_start", "perPage": 200, "page": page},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            settlements.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1

        expenses = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{POCKETBASE_EXPENSE_COLLECTION}/records",
                headers={"Authorization": pb_token},
                params={"perPage": 500, "page": page},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            expenses.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1

        months = {}
        for row in expenses:
            key = (row["marketplace"], row["year"], row["month"])
            months.setdefault(key, {
                "marketplace": row["marketplace"], "month": row["month"], "year": row["year"],
                "categories": {}, "net": 0,
            })
            # expenses rows are per (group_id, marketplace, category) - many groups can
            # contribute to the same month/category, so accumulate rather than overwrite.
            bucket = months[key]
            bucket["categories"][row["category"]] = round(bucket["categories"].get(row["category"], 0) + row.get("amount", 0), 2)
            bucket["net"] += row.get("amount", 0)
        for m in months.values():
            m["net"] = round(m["net"], 2)
        month_rows = sorted(months.values(), key=lambda m: (m["marketplace"], m["year"], m["month"]), reverse=True)

        disbursed = [s for s in settlements if s.get("status") == "Closed"]
        open_periods = [s for s in settlements if s.get("status") == "Open"]
        total_disbursed_usd = round(
            sum(s.get("converted_amount_usd") or 0 for s in disbursed if s.get("converted_amount_usd") is not None), 2
        )

        # Native-currency disbursed totals per marketplace tag - honest for non-USD
        # marketplaces (EUR/GBP/PLN/SEK/CAD/MXN) since there's no reliable USD
        # conversion for those the way ConvertedTotal covers MXN/CAD today.
        by_marketplace = {}
        for s in disbursed:
            tag = s.get("marketplace", "unknown")
            entry = by_marketplace.setdefault(tag, {"currency": s.get("currency", ""), "amount": 0})
            entry["amount"] += s.get("original_amount") or 0
        for entry in by_marketplace.values():
            entry["amount"] = round(entry["amount"], 2)

        marketplaces_present = sorted({s.get("marketplace") for s in settlements if s.get("marketplace")})

        return json_response({
            "settlements": settlements[:200],
            "openPeriods": open_periods,
            "totalDisbursedUsd": total_disbursed_usd,
            "totalDisbursedByMarketplace": by_marketplace,
            "marketplaces": marketplaces_present,
            "months": month_rows,
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
