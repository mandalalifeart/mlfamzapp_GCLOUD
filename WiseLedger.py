"""Wise (wise.com) accounting ledger - every money movement on every currency
balance of the business profile, one row per (transaction, currency,
direction), with an accounting classification. Backs the /wise page.

Built 2026-10-01 after comparing a real Sep 2026 statement export against
what the API had stored. Findings that shaped this module, all confirmed live:

- /v1/transfers covers BOTH directions. An incoming deposit (Amazon/HSBC/Etsy
  payout landing in a balance) is a transfer whose targetAccount is one of
  this profile's own `type: "balance"` accounts (GET /v1/accounts/{id}).
  Every other target type (iban/aba/swift_code/indian/...) is an outgoing
  payment to a third party, and that account's accountHolderName is the payee.
- A transfer's sourceValue EXCLUDES Wise's fee; the balance is debited
  sourceValue + fee. The exact fee is on the transfer's quote
  (GET /v3/profiles/{p}/quotes/{quoteUuid}): the paymentOption whose payIn
  matches the quote's payInMethod (and payOut matches) has price.total -
  matched the statement's "Total Fees" to the cent on all 9 Sep 2026 fees.
- Card payments, balance-to-balance conversions and cashback are NOT
  transfers. The activity feed (GET /v1/profiles/{p}/activities) has them
  (CARD_PAYMENT / INTERBALANCE / BALANCE_CASHBACK), but only the latest 100
  entries - so a daily pull keeps them going forward, and the monthly
  statement upload (ImportWiseStatement) backfills/corrects everything.
- The statement export is the authoritative record: a statement row always
  overwrites an API-sourced row with the same ledger_key, and later API pulls
  never overwrite a statement-sourced row. Statement "Date Time" is an Excel
  serial in local time (UTC+3 in Sep 2026 - Asia/Jerusalem assumed).

ledger_key = "<statement-style ID>|<currency>|<in|out>" (e.g.
"TRANSFER-2343421870|EUR|out", "CARD-4290092096|EUR|in"), matching the
statement's own ID column so both sources land on the same row. Cashback is
the one exception - the statement ID is a UUID while the activity feed uses a
numeric id - so it's keyed "CASHBACK|<currency>|<YYYY-MM>|<amount>".

Classification is computed at read time (user rules by counterparty, then
DEFAULT_RULES), never stored on the ledger row, so changing a rule re-
classifies history instantly with no rewrite.
"""
import base64
import hashlib
import io
import json
import mimetypes
import os
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from WiseFinances import (
    POCKETBASE_URL,
    PROFILES,
    WISE_API_BASE,
    WISE_API_TOKEN,
    WISE_REFRESH_KEY,
    cors_headers,
    json_response,
    pb_authenticate,
    pb_batch,
    wise_headers,
    wise_iso,
)

LEDGER_COLLECTION = "wise_ledger"
SUPPLIER_SETTINGS_COLLECTION = "wise_supplier_settings"
ACCOUNTS_COLLECTION = "wise_accounts"
RULES_COLLECTION = "wise_classification_rules"
IMPORTS_COLLECTION = "wise_statement_imports"
DOCUMENTS_COLLECTION = "wise_documents"

# Per the user (2026-10-01): keep every invoice together with that month's
# uploaded statement, one folder per month: <WISE_DOCS_DIR>/YYYY-MM/.
# Local disk on this machine (the local server is the only writer/reader).
WISE_DOCS_DIR = os.environ.get("WISE_DOCS_DIR", "/home/bababot/accounting/wise")
MAX_DOCUMENT_BYTES = 25 * 1024 * 1024

LOCAL_TZ = ZoneInfo("Asia/Jerusalem")

# Transfer statuses where money actually moved and stayed moved.
# cancelled/funds_refunded/bounced_back/charged_back never net-affect a balance.
EXCLUDED_TRANSFER_STATUSES = {"cancelled", "funds_refunded", "bounced_back", "charged_back"}

INCOME_CLASSES = [
    "Amazon payout",
    "Etsy payout",
    "Tax refund",
    "Owner capital / loan",
    "Currency conversion",
    "Refund / reversal",
    "Cashback",
    "Other income",
    "Unclassified",
]
# An outgoing row classified "Amazon payout" is netted against the Amazon
# payout balance (shown on the Incoming side as a negative amount) rather than
# booked as an expense - per the user (2026-10-01), Amazon card charges and
# their refunds both belong to the Amazon payment balance.
# Etsy too (per the user 2026-10-07: Etsy card charges belong to the Etsy
# payout balance, like Amazon's).
NET_AGAINST_INCOME_CLASSES = ["Amazon payout", "Etsy payout"]
EXPENSE_CLASSES = [
    "Amazon payout",
    "Inventory / suppliers",
    "Logistics & warehousing",
    "Taxes & VAT",
    "Accounting & tax services",
    "Software & subscriptions",
    "Marketing",
    "Business services",
    "Equipment / hardware",
    "Travel",
    "Amazon charges",
    "Owner withdrawal",
    "Transfer to own account",
    "Currency conversion",
    "Other expense",
    # One group for both directions (per the user 2026-10-03: "same group, merge them").
    "Refund / reversal",
    "Unclassified",
]
FEES_CLASS = "Wise fees"

# (direction, details_types or None, regex over "counterparty | reference | description", class)
# First match wins. Only clearly-identifiable payees are here - anyone ambiguous
# stays "Unclassified" for the user to set from the page (stored as a rule).
# Wise statements show ATM withdrawals as ordinary CARD rows (no ATM flag), so
# they are recognised by the terminal name: "...Oatm/Ratm/Cdm...", a bank
# branch ("Br", Vietnamese "Pgd" = transaction office) or a bank whose card
# terminals are ATMs, plus the specific 2026 ATMs (India/Vietnam) found in the
# ledger whose names don't say so (all round 10,000 INR / 5,220,000 VND).
ATM_RE = (r"\b(atm|oatm|ratm|cdm|pgd|br)\b|vpbank|bidv|vietcombank|techcombank|agribank|sacombank|"
          r"krungthai|kasikorn|church complex goa|ari hills|vietlongthinh|mapusa goa|sansar chandra road|206 duong 1/4")


def is_atm(row):
    return (row.get("details_type") or "").upper() == "CARD" and row.get("direction") == "out" and \
        bool(re.search(ATM_RE, row.get("counterparty") or "", re.I))


DEFAULT_RULES = [
    ("in", {"CONVERSION"}, r".", "Currency conversion"),
    ("out", {"CONVERSION"}, r".", "Currency conversion"),
    ("in", {"CASHBACK"}, r".", "Cashback"),
    # Per the user (2026-10-01): card refunds/reversals from Amazon belong to the
    # Amazon payment balance, not a generic card-refund bucket.
    ("in", {"CARD"}, r"amazon|amzn", "Amazon payout"),
    ("in", {"CARD"}, r"\betsy\b", "Etsy payout"),
    ("in", {"CARD"}, r".", "Refund / reversal"),
    ("in", None, r"etsy|^payout\b|\| *payout", "Etsy payout"),
    ("in", None, r"vat refund|erstatt|remb\. dgfip|tax refund|finanzamt", "Tax refund"),
    ("in", None, r"loan into an entity", "Owner capital / loan"),
    # Wise payment links ("Received payment from <customer>") = direct sales with
    # their own invoice (2025: MA2500007 / MA2500011), not Amazon payouts.
    ("in", None, r"received payment from", "Other income"),
    ("in", None, r"amazon|hsbc bank|intl pymnt|iat paymen|\bslr[0-9a-z]{6,}|\bsw\d{8,}|\bdas[0-9a-z]{8,}|marketplac|\bpayments?\b",
     "Amazon payout"),
    ("out", None, r"rafi tamir|refael tamir", "Owner withdrawal"),
    # Per the user (2026-10-03): ATM cash withdrawals -> "Other expense"
    # (no invoice, counted in the Other expenses balance), labelled ATM.
    ("out", {"CARD"}, ATM_RE, "Other expense"),
    # Card payments for hotels / flights / stays (per the user, 2026-10-01:
    # "Jai Niwas Jaipur" is a hotel in India -> travel, not the supplier).
    ("out", {"CARD"}, r"hotel|hostel|resort|airindia|air india|airline|airways|indigo|booking\.com|agoda|airbnb|niwas|make ?my ?trip|goibibo|cleartrip|yatra|12go|irctc|redbus", "Travel"),
    ("out", None, r"mandala life art", "Transfer to own account"),
    ("out", None, r"malani|acacia fashion|zinnia|om and sons|new rain|shrinath|sara ?a?gems|jai niwas", "Inventory / suppliers"),
    ("out", None, r"unicargo|lagerpark|ifulfillyou", "Logistics & warehousing"),
    ("out", None, r"bbk rostock|\bsiee\b|irish tax|belastingdienst|financni urad|ministero dell|hmrc|maksu- ja tolliamet|nbp o/okr|dgfip",
     "Taxes & VAT"),
    ("out", None, r"taxually|e-advisors|alcatera|smart accounts|avask|safari star", "Accounting & tax services"),
    ("out", None, r"google|anthropic|openai|microsoft|adobe|canva|shopify", "Software & subscriptions"),
    ("out", None, r"archer affiliates|facebook|meta ads|pinterest|tiktok", "Marketing"),
    ("out", None, r"gs1|telia|eurest", "Business services"),
    ("out", {"CARD"}, r"amazon|amzn", "Amazon payout"),
    ("out", {"CARD"}, r"\betsy\b", "Etsy payout"),
]
_COMPILED_RULES = [(d, t, re.compile(rx, re.I), c) for d, t, rx, c in DEFAULT_RULES]

_HTML_TAG_RE = re.compile(r"<[^>]+>")


# ---------------------------------------------------------------- PocketBase

def pb_list_all(token, collection, filter_str="", fields=None, sort=""):
    items, page = [], 1
    while True:
        params = {"perPage": 500, "page": page}
        if filter_str:
            params["filter"] = filter_str
        if fields:
            params["fields"] = fields
        if sort:
            params["sort"] = sort
        response = requests.get(
            f"{POCKETBASE_URL}/api/collections/{collection}/records",
            headers={"Authorization": token}, params=params, timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        items.extend(data.get("items", []))
        if page >= data.get("totalPages", 1):
            break
        page += 1
    return items


LEDGER_FIELDS = [
    "wise_id", "source", "occurred_at", "currency", "direction", "amount", "fee",
    "fee_checked", "counterparty", "counterparty_inferred", "description", "reference",
    "details_type", "exchange_currency", "exchange_amount", "quote_uuid",
]


def _same(a, b):
    if isinstance(a, float) or isinstance(b, float):
        try:
            return abs(float(a or 0) - float(b or 0)) < 0.005
        except (TypeError, ValueError):
            return False
    if isinstance(a, str) or isinstance(b, str):
        # PocketBase returns dates as "YYYY-MM-DD HH:MM:SS.000Z", we send "...THH:MM:SSZ".
        return str(a or "").replace("T", " ")[:19] == str(b or "").replace("T", " ")[:19]
    return (a or 0) == (b or 0)


def upsert_ledger_rows(token, rows, existing_by_key):
    """rows: dicts with ledger_key + LEDGER_FIELDS. Precedence:
    statement rows always win; API rows never overwrite a statement row;
    an API row never blanks out a real (non-inferred) counterparty."""
    ops, created, updated = [], 0, 0
    for row in rows:
        key = row["ledger_key"]
        existing = existing_by_key.get(key)
        if not existing:
            ops.append({"method": "POST", "url": f"/api/collections/{LEDGER_COLLECTION}/records", "body": row})
            existing_by_key[key] = dict(row)
            created += 1
            continue
        if existing.get("source") == "statement" and row["source"] != "statement":
            continue
        patch = {}
        for field in LEDGER_FIELDS:
            if field not in row:
                continue
            new = row[field]
            if field in ("counterparty", "counterparty_inferred") and row["source"] != "statement":
                if existing.get("counterparty") and not existing.get("counterparty_inferred") and row.get("counterparty_inferred"):
                    continue
                if field == "counterparty" and not new:
                    continue
            if field == "occurred_at" and row["source"] == "statement" and existing.get("occurred_at"):
                # API timestamps are true UTC; keep them over the statement's local-time serial.
                continue
            if field in ("fee", "fee_checked") and row["source"] != "statement" and existing.get("fee_checked") and not row.get("fee_checked"):
                continue
            if field == "amount" and row["source"] != "statement" and existing.get("fee_checked") and not row.get("fee_checked"):
                continue
            # A statement line that doesn't carry the converted amount (PDF and
            # older xlsx statements) must not wipe the one the API recorded
            # (e.g. EUR sent -> USD the supplier received).
            if field in ("exchange_currency", "exchange_amount") and not row.get("exchange_amount") \
                    and (existing.get("exchange_amount") or 0) > 0:
                continue
            if not _same(existing.get(field), new):
                patch[field] = new
        if patch:
            ops.append({"method": "PATCH", "url": f"/api/collections/{LEDGER_COLLECTION}/records/{existing['id']}", "body": patch})
            existing.update(patch)
            updated += 1
    if ops:
        pb_batch(token, ops)
    return created, updated


def load_ledger_by_key(token):
    return {r["ledger_key"]: r for r in pb_list_all(token, LEDGER_COLLECTION)}


# ---------------------------------------------------------------- Wise API

def money_str(s):
    """'<positive>+ 1,810.28 EUR</positive>' -> (1810.28, 'EUR', True)"""
    text = _HTML_TAG_RE.sub("", s or "").strip()
    positive = text.startswith("+") or "<positive>" in (s or "")
    m = re.search(r"(-?\d[\d,]*(?:\.\d+)?)\s*([A-Z]{3})", text)
    if not m:
        return None, None, positive
    return float(m.group(1).replace(",", "")), m.group(2), positive


def fetch_activities(profile_id):
    response = requests.get(
        f"{WISE_API_BASE}/v1/profiles/{profile_id}/activities",
        headers=wise_headers(), params={"size": 100}, timeout=30,
    )
    response.raise_for_status()
    return response.json().get("activities", [])


def sync_accounts(token, transfers):
    """Caches GET /v1/accounts/{id} per distinct targetAccount (77 accounts on
    this profile as of 2026-10) - only unseen ids hit the API."""
    known = {int(a["account_id"]): a for a in pb_list_all(token, ACCOUNTS_COLLECTION)}
    wanted = {t["targetAccount"] for t in transfers if t.get("targetAccount")}
    ops = []
    for account_id in sorted(wanted - set(known)):
        response = requests.get(f"{WISE_API_BASE}/v1/accounts/{account_id}", headers=wise_headers(), timeout=30)
        if response.status_code != 200:
            continue
        a = response.json()
        body = {
            "account_id": account_id,
            "account_type": a.get("type", ""),
            "holder_name": (a.get("accountHolderName") or "").strip(),
            "currency": a.get("currency", ""),
            "profile_id": a.get("profile") or 0,
        }
        known[account_id] = body
        ops.append({"method": "POST", "url": f"/api/collections/{ACCOUNTS_COLLECTION}/records", "body": body})
    if ops:
        pb_batch(token, ops)
    return known


def fetch_transfer_fee(profile_id, quote_uuid):
    """Returns the fee in source currency, or None if it can't be determined."""
    if not quote_uuid:
        return None
    response = requests.get(f"{WISE_API_BASE}/v3/profiles/{profile_id}/quotes/{quote_uuid}", headers=wise_headers(), timeout=30)
    if response.status_code != 200:
        return None
    q = response.json()
    pay_in = q.get("payInMethod") or q.get("preferredPayIn")
    pay_out = q.get("payOut")
    options = [
        o for o in q.get("paymentOptions") or []
        if o.get("payIn") == pay_in and (not pay_out or o.get("payOut") == pay_out)
    ]
    if not options:
        return None
    amount = (((options[0].get("price") or {}).get("total") or {}).get("value") or {}).get("amount")
    return float(amount) if amount is not None else None


def infer_incoming_payer(reference):
    """Older incoming transfers have no payer name anywhere in the API (the
    activity feed only covers the latest 100). Labels below come from the
    reference patterns of real payouts, cross-checked against the Sep 2026
    statement's actual Payer Name column."""
    ref = reference or ""
    if re.search(r"amazon svcs|/\w*GD$", ref, re.I):
        return "HSBC BANK PLC (Amazon)"
    if re.search(r"amazon payments europe|^SLR|^SW\d", ref, re.I):
        return "AMAZON PAYMENTS EUROPE S.C.A."
    if ref.startswith("DAS"):
        return "AMAZON PAYMENTS UK"
    if ref == "IAT Paymen":
        return "Amazon Payments Inc."
    if ref == "INTL PYMNT":
        return "Amazon (international payment)"
    if ref in ("PAYMENTS", "PAYMENT", "Marketplac"):
        return "Amazon"
    if ref == "PAYOUT":
        return "ETSY INC."
    if re.search(r"VAT REFUND", ref):
        return "Czech tax office (VAT refund)"
    if ref.startswith("ERSTATT"):
        return "German tax office (refund)"
    if ref.startswith("REMB. DGFiP"):
        return "French tax office (refund)"
    # Still give every unnamed deposit a label, so the page's classification
    # dropdown (which stores a rule per counterparty) can classify it.
    if re.fullmatch(r"[A-Za-z ]{3,}", ref.strip()):
        return f"Unknown payer (ref {ref.strip()})"
    return "Unknown payer"


def transfer_rows(profile_id, transfers, accounts, activity_names, existing_by_key):
    rows, delete_keys = [], []
    for t in transfers:
        target = accounts.get(t.get("targetAccount")) or {}
        # Incoming = a Wise balance of THIS profile. A "balance"-type account of
        # another profile (real case: Refael Tamir's personal Wise balance) is
        # an outgoing payment to that person, not a deposit.
        incoming = target.get("account_type") == "balance" and int(target.get("profile_id") or profile_id) == int(profile_id)
        direction = "in" if incoming else "out"
        currency = t.get("targetCurrency") if incoming else t.get("sourceCurrency")
        key = f"TRANSFER-{t['id']}|{currency}|{direction}"
        if t.get("status") in EXCLUDED_TRANSFER_STATUSES:
            existing = existing_by_key.get(key)
            if existing and existing.get("source") != "statement":
                delete_keys.append(key)
            continue
        occurred = wise_iso(t.get("created"))
        if not occurred:
            continue
        reference = t.get("reference") or ""
        if incoming:
            name = activity_names.get(str(t["id"]), "")
            inferred = False
            if not name:
                name, inferred = infer_incoming_payer(reference), True
            rows.append({
                "ledger_key": key, "wise_id": f"TRANSFER-{t['id']}", "source": "api_transfer",
                "occurred_at": occurred, "currency": currency, "direction": "in",
                "amount": t.get("targetValue") or 0, "fee": 0, "fee_checked": True,
                "counterparty": name, "counterparty_inferred": bool(name) and inferred,
                "description": f"Received money{' from ' + name if name and not inferred else ''}",
                "reference": reference, "details_type": "DEPOSIT",
                "exchange_currency": "", "exchange_amount": 0, "quote_uuid": t.get("quoteUuid") or "",
            })
        else:
            existing = existing_by_key.get(key) or {}
            fee_checked = bool(existing.get("fee_checked"))
            fee = existing.get("fee") or 0
            if not fee_checked:
                looked_up = fetch_transfer_fee(profile_id, t.get("quoteUuid"))
                time.sleep(0.15)
                if looked_up is not None:
                    fee, fee_checked = looked_up, True
            name = target.get("holder_name") or activity_names.get(str(t["id"]), "")
            source_value = t.get("sourceValue") or 0
            cross = t.get("sourceCurrency") != t.get("targetCurrency")
            rows.append({
                "ledger_key": key, "wise_id": f"TRANSFER-{t['id']}", "source": "api_transfer",
                "occurred_at": occurred, "currency": currency, "direction": "out",
                "amount": round(source_value + (fee if fee_checked else 0), 2),
                "fee": round(fee, 2) if fee_checked else 0, "fee_checked": fee_checked,
                "counterparty": name, "counterparty_inferred": False,
                "description": f"Sent money to {name}" if name else "Sent money",
                "reference": reference, "details_type": "TRANSFER",
                "exchange_currency": t.get("targetCurrency") if cross else "",
                "exchange_amount": (t.get("targetValue") or 0) if cross else 0,
                "quote_uuid": t.get("quoteUuid") or "",
            })
    return rows, delete_keys


def activity_rows(activities):
    """Card payments, conversions, cashback (+ any unrecognised money-moving
    activity type, generically) from the latest-100 activity feed. TRANSFER
    activities are skipped here (covered by /v1/transfers); CARD_CHECK is a
    zero-amount card verification; CANCELLED card payments never settle."""
    rows = []
    for a in activities:
        a_type = a.get("type")
        if a_type in ("TRANSFER", "CARD_CHECK") or a.get("status") == "CANCELLED":
            continue
        resource = a.get("resource") or {}
        occurred = (a.get("createdOn") or "")[:19] + "Z" if a.get("createdOn") else None
        if not occurred:
            continue
        title = _HTML_TAG_RE.sub("", a.get("title") or "").strip()
        p_amt, p_cur, p_pos = money_str(a.get("primaryAmount"))
        s_amt, s_cur, _ = money_str(a.get("secondaryAmount"))
        if p_amt is None:
            continue
        base = {"source": "api_activity", "occurred_at": occurred, "fee": 0, "fee_checked": False,
                "counterparty_inferred": False, "reference": "", "quote_uuid": ""}
        if a_type == "CARD_PAYMENT":
            currency, amount = (s_cur, s_amt) if s_amt is not None else (p_cur, p_amt)
            direction = "in" if p_pos else "out"
            wise_id = f"CARD-{resource.get('id')}"
            rows.append({**base, "ledger_key": f"{wise_id}|{currency}|{direction}", "wise_id": wise_id,
                         "currency": currency, "direction": direction, "amount": amount,
                         "counterparty": title, "details_type": "CARD",
                         "description": f"Card transaction of {p_amt:.2f} {p_cur} issued by {title}",
                         "exchange_currency": p_cur if p_cur != currency else "",
                         "exchange_amount": p_amt if p_cur != currency else 0})
        elif a_type == "INTERBALANCE" and s_amt is not None:
            wise_id = f"BALANCE-{resource.get('id')}"
            desc = f"Converted {s_amt:,.2f} {s_cur} to {p_amt:,.2f} {p_cur}"
            label = f"Conversion {s_cur} → {p_cur}"
            rows.append({**base, "ledger_key": f"{wise_id}|{s_cur}|out", "wise_id": wise_id, "currency": s_cur,
                         "direction": "out", "amount": s_amt, "counterparty": label, "details_type": "CONVERSION",
                         "description": desc, "exchange_currency": p_cur, "exchange_amount": p_amt})
            rows.append({**base, "ledger_key": f"{wise_id}|{p_cur}|in", "wise_id": wise_id, "currency": p_cur,
                         "direction": "in", "amount": p_amt, "counterparty": label, "details_type": "CONVERSION",
                         "description": desc, "exchange_currency": s_cur, "exchange_amount": s_amt, "fee_checked": True})
        elif a_type == "BALANCE_CASHBACK":
            rows.append({**base, "ledger_key": f"CASHBACK|{p_cur}|{occurred[:7]}|{p_amt:.2f}",
                         "wise_id": f"BALANCE_CASHBACK-{resource.get('id')}", "currency": p_cur, "direction": "in",
                         "amount": p_amt, "counterparty": "Wise cashback", "details_type": "CASHBACK",
                         "description": "Cashback", "exchange_currency": "", "exchange_amount": 0, "fee_checked": True})
        else:
            direction = "in" if p_pos else "out"
            wise_id = f"{resource.get('type') or a_type}-{resource.get('id')}"
            rows.append({**base, "ledger_key": f"{wise_id}|{p_cur}|{direction}", "wise_id": wise_id,
                         "currency": p_cur, "direction": direction, "amount": p_amt, "counterparty": title,
                         "details_type": a_type or "", "description": _HTML_TAG_RE.sub("", a.get("description") or "") or title,
                         "exchange_currency": "", "exchange_amount": 0})
    return rows


def sync_ledger_from_api(token, profile_id, transfers):
    """Called from UpdateWiseFinances with the transfer list it already pulled."""
    existing_by_key = load_ledger_by_key(token)
    accounts = sync_accounts(token, transfers)
    activities = fetch_activities(profile_id)
    activity_names = {}
    for a in activities:
        resource = a.get("resource") or {}
        if resource.get("type") == "TRANSFER" and resource.get("id"):
            name = _HTML_TAG_RE.sub("", a.get("title") or "").strip()
            if name:
                activity_names[str(resource["id"])] = name
    t_rows, delete_keys = transfer_rows(profile_id, transfers, accounts, activity_names, existing_by_key)
    a_rows = activity_rows(activities)
    created, updated = upsert_ledger_rows(token, t_rows + a_rows, existing_by_key)
    deletes = [
        {"method": "DELETE", "url": f"/api/collections/{LEDGER_COLLECTION}/records/{existing_by_key[k]['id']}"}
        for k in delete_keys if existing_by_key.get(k, {}).get("id")
    ]
    if deletes:
        pb_batch(token, deletes)
    return {"ledgerCreated": created, "ledgerUpdated": updated, "ledgerDeleted": len(deletes)}


# ---------------------------------------------------------------- statement import

_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _col_index(ref):
    letters = re.match(r"[A-Z]+", ref).group(0)
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def parse_xlsx_rows(data):
    """Minimal stdlib xlsx reader (no openpyxl on this box): first sheet,
    shared + inline strings, returns list of dicts keyed by the header row."""
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = zf.namelist()
        shared = []
        if "xl/sharedStrings.xml" in names:
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("m:si", _NS):
                shared.append("".join(t.text or "" for t in si.iter(f"{{{_NS['m']}}}t")))
        sheet_name = sorted(n for n in names if n.startswith("xl/worksheets/sheet"))[0]
        root = ET.fromstring(zf.read(sheet_name))
    table = []
    for row in root.iter(f"{{{_NS['m']}}}row"):
        cells = {}
        for c in row.findall("m:c", _NS):
            idx = _col_index(c.get("r"))
            t = c.get("t")
            v = c.find("m:v", _NS)
            if t == "s" and v is not None:
                cells[idx] = shared[int(v.text)]
            elif t == "inlineStr":
                cells[idx] = "".join(x.text or "" for x in c.iter(f"{{{_NS['m']}}}t"))
            elif v is not None:
                text = v.text or ""
                if t in ("str", "b"):
                    cells[idx] = text
                else:
                    try:
                        cells[idx] = float(text)
                    except ValueError:
                        cells[idx] = text
        if cells:
            table.append([cells.get(i, "") for i in range(max(cells) + 1)])
    if not table:
        return []
    header = [str(h).strip() for h in table[0]]
    return [{header[i]: (r[i] if i < len(r) else "") for i in range(len(header))} for r in table[1:]]


def excel_serial_to_utc_iso(serial):
    if serial in ("", None):
        return None
    if isinstance(serial, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}( \d{2}:\d{2}(:\d{2})?)?", serial.strip()):
        # PDF statements give a date (sometimes a time) instead of an Excel serial.
        text = serial.strip()
        fmt = "%Y-%m-%d %H:%M:%S" if text.count(":") == 2 else "%Y-%m-%d %H:%M" if ":" in text else "%Y-%m-%d"
        local = datetime.strptime(text, fmt)
        if fmt == "%Y-%m-%d":
            local = local.replace(hour=12)
        return local.replace(tzinfo=LOCAL_TZ).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    local = datetime(1899, 12, 30) + timedelta(days=float(serial))
    local = local.replace(tzinfo=LOCAL_TZ)
    return local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _num(v):
    if v in ("", None):
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


_CARD_DESC_RE = re.compile(r"Card transaction of ([\d,]+(?:\.\d+)?) ([A-Z]{3})")
_CONV_DESC_RE = re.compile(r"Converted ([\d,]+(?:\.\d+)?) ([A-Z]{3}) to ([\d,]+(?:\.\d+)?) ([A-Z]{3})")


def foreign_amount_from_description(description, currency, fallback_currency=""):
    """PDF statements (and some exports) leave the foreign amount empty but
    print it in the description - "Card transaction of 10,500.00 INR ...",
    "Converted 1,556.99 GBP to 1,810.28 EUR"."""
    m = _CARD_DESC_RE.search(description or "")
    if m and m.group(2) != currency:
        return m.group(2), float(m.group(1).replace(",", ""))
    m = _CONV_DESC_RE.search(description or "")
    if m:
        a, ca, b, cb = float(m.group(1).replace(",", "")), m.group(2), float(m.group(3).replace(",", "")), m.group(4)
        if currency == ca:
            return cb, b
        if currency == cb:
            return ca, a
    return fallback_currency, 0.0


def statement_row_to_ledger(r):
    wise_id = str(r.get("ID") or "").strip()
    if not wise_id and not r.get("_pdf_no_id"):
        return None  # PDF lines without an id are keyed later (resolve_pdf_statement_keys)
    amount_signed = _num(r.get("Amount"))
    currency = str(r.get("Currency") or "").strip()
    direction = "out" if str(r.get("Transaction Type")).upper() == "DEBIT" or amount_signed < 0 else "in"
    details = str(r.get("Transaction Details Type") or "").strip().upper()
    occurred = excel_serial_to_utc_iso(r.get("Date Time") or r.get("Date"))
    description = str(r.get("Description") or "").strip()
    ex_from, ex_to = str(r.get("Exchange From") or "").strip(), str(r.get("Exchange To") or "").strip()
    if wise_id.startswith("BALANCE_CASHBACK"):
        details = "CASHBACK"
    elif not details:
        # Older statement downloads (e.g. 2025 "Balance Statement" sheets) have
        # no Transaction Details Type column - the id prefix says the same.
        details = {"BALANCE": "CONVERSION", "CARD": "CARD", "TRANSFER": "TRANSFER"}.get(wise_id.split("-")[0], "")
    if details == "CONVERSION":
        counterparty = f"Conversion {ex_from} → {ex_to}"
    elif details == "CASHBACK":
        counterparty = "Wise cashback"
    elif direction == "in":
        counterparty = str(r.get("Payer Name") or r.get("Merchant") or "").strip()
    else:
        counterparty = str(r.get("Payee Name") or r.get("Merchant") or "").strip()
    if not counterparty:
        counterparty = str(r.get("Merchant") or r.get("Payer Name") or r.get("Payee Name") or "").strip()
    exchange_currency, exchange_amount = "", 0.0
    if ex_from and ex_to:
        if currency == ex_from:
            exchange_currency, exchange_amount = ex_to, _num(r.get("Exchange To Amount"))
        else:
            exchange_currency = ex_from
    if exchange_currency and not exchange_amount:
        exchange_currency, exchange_amount = foreign_amount_from_description(description, currency, exchange_currency)
    amount = round(abs(amount_signed), 2)
    if details == "CASHBACK":
        key = f"CASHBACK|{currency}|{(occurred or '')[:7]}|{amount:.2f}"
    else:
        key = f"{wise_id}|{currency}|{direction}"
    return {
        "ledger_key": key, "wise_id": wise_id, "source": "statement", "occurred_at": occurred,
        "currency": currency, "direction": direction, "amount": amount,
        "fee": round(_num(r.get("Total Fees")), 2), "fee_checked": True,
        "counterparty": counterparty, "counterparty_inferred": False,
        "description": description, "reference": str(r.get("Payment Reference") or "").strip(),
        "details_type": details, "exchange_currency": exchange_currency,
        "exchange_amount": round(exchange_amount, 2),
    }


def merge_fee_rows(rows):
    """Some statement downloads (seen: June 2025 "All transactions" sheet) list
    Wise's fee as its own "FEE-<id>" line and leave it out of the main line's
    Amount. Fold it back so a row's amount is always the total that left the
    balance (as in every other statement), and drop the FEE line - the fee is
    already in the main row's fee field."""
    fees = {(r["wise_id"][4:], r["currency"]): r["amount"] for r in rows if r["wise_id"].startswith("FEE-")}
    out = []
    for r in rows:
        if r["wise_id"].startswith("FEE-"):
            continue
        extra = fees.get((r["wise_id"], r["currency"]))
        if extra and r["direction"] == "out":
            r["amount"] = round(r["amount"] + extra, 2)
        out.append(r)
    return out


STATEMENT_NAME_RE = re.compile(r"_([A-Z]{3})_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})")


def statement_files(filename, data):
    """A Wise statement download is either one .xlsx or a .zip of one .xlsx
    per currency balance. Yields (name, xlsx_bytes)."""
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = zf.namelist()
        if any(n.startswith("xl/") for n in names):
            yield filename, data
            return
        for n in names:
            if n.lower().endswith(".xlsx"):
                yield n, zf.read(n)


def ImportWiseStatement(request):
    """POST JSON {filename, contentBase64} (+ ?key=WISE_REFRESH_KEY). Upserts
    every statement row into wise_ledger, statement taking precedence."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    args = getattr(request, "args", {}) or {}
    body = request.get_json(silent=True) or {}
    if WISE_REFRESH_KEY and (args.get("key") or body.get("key")) != WISE_REFRESH_KEY:
        return json_response({"error": "Unauthorized"}, 401)
    try:
        data = base64.b64decode(body.get("contentBase64") or "")
        if not data:
            return json_response({"error": "No file content"}, 400)
        filename = body.get("filename") or "statement.xlsx"
        rows, per_file, currencies, ranges = [], [], set(), set()
        if data[:4] == b"%PDF":
            pdf = parse_pdf_statement(filename, data)
            token = pb_authenticate()
            parsed = resolve_pdf_statement_keys(token, [statement_row_to_ledger(r) for r in pdf["rows"]])
            rows.extend(p for p in parsed if p)
            per_file.append({"file": filename, "rows": len(rows)})
            if pdf.get("currency"):
                currencies.add(pdf["currency"])
            if pdf.get("period_start") and pdf.get("period_end"):
                ranges.add((pdf["period_start"], pdf["period_end"]))
        for name, xlsx in ([] if data[:4] == b"%PDF" else statement_files(filename, data)):
            parsed = [statement_row_to_ledger(r) for r in parse_xlsx_rows(xlsx)]
            parsed = merge_fee_rows([p for p in parsed if p])
            per_file.append({"file": name, "rows": len(parsed)})
            rows.extend(parsed)
            m = STATEMENT_NAME_RE.search(name)
            if m:
                currencies.add(m.group(1))
                ranges.add((m.group(2), m.group(3)))
            currencies.update(p["currency"] for p in parsed)
        if not ranges:
            m = re.search(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})", filename)
            if m:
                ranges.add((m.group(1), m.group(2)))
        period = (body.get("period") or "").strip()
        months = sorted({a[:7] for a, _ in ranges} or {(r.get("occurred_at") or "")[:7] for r in rows if r.get("occurred_at")})
        if period and (not months or any(not in_period(m, period) for m in months)):
            return json_response({"error": f"This statement is for {', '.join(months) or 'an unknown period'}, "
                                           f"but {period} is selected on the page - switch the month first."}, 400)
        token = pb_authenticate()
        existing = load_ledger_by_key(token)
        matched = sum(1 for r in rows if r["ledger_key"] in existing)
        created, updated = upsert_ledger_rows(token, rows, existing)
        dates = sorted(r["occurred_at"] for r in rows if r.get("occurred_at"))
        # Record what this statement covers so the page can mark those months
        # "confirmed" - the period comes from Wise's own filename
        # (statement_<balanceId>_<CUR>_<from>_<to>.xlsx), not from row dates,
        # since a currency with zero activity still confirms "nothing happened".
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        import_ops = [{
            "method": "POST", "url": f"/api/collections/{IMPORTS_COLLECTION}/records",
            "body": {"filename": filename, "period_start": start, "period_end": end,
                     "currencies": sorted(currencies), "rows": len(rows), "imported_at": now_iso},
        } for start, end in sorted(ranges)]
        if import_ops:
            pb_batch(token, import_ops)
        stored = []
        already = pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"sha256 = {json.dumps(hashlib.sha256(data).hexdigest())}")
        for start, _end in sorted(ranges):
            if any(d.get("month") == start[:7] for d in already):
                continue  # same statement file uploaded again - data re-applied, file not duplicated
            stored.append(store_document(token, "statement", start[:7], filename, data))
        return json_response({
            "rows": len(rows), "alreadyKnown": matched, "created": created, "updated": updated,
            "storedAs": stored, "alreadyStored": bool(already),
            "periods": [{"from": a, "to": b} for a, b in sorted(ranges)], "currencies": sorted(currencies),
            "files": [f for f in per_file if f["rows"]], "emptyFiles": len([f for f in per_file if not f["rows"]]),
            "from": dates[0] if dates else None, "to": dates[-1] if dates else None,
        })
    except zipfile.BadZipFile:
        return json_response({"error": "Not a Wise statement .zip/.xlsx file"}, 400)
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


# ---------------------------------------------------------------- documents (statements + invoices)

_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def safe_name(text, limit=60):
    return _SAFE_RE.sub("-", text or "").strip("-.")[:limit] or "file"


def store_document(token, kind, month, filename, data, ledger_key="", prefix="", group_key="", extracted=None):
    """Writes the file to <WISE_DOCS_DIR>/<month>/ and records it in
    wise_documents. Never overwrites: an existing name gets a -2/-3 suffix."""
    folder = os.path.join(WISE_DOCS_DIR, month)
    os.makedirs(folder, exist_ok=True)
    base, ext = os.path.splitext(filename or "file")
    name = safe_name(f"{prefix}{base}", 120) + (ext.lower() if ext else "")
    path, n = os.path.join(folder, name), 1
    while os.path.exists(path):
        n += 1
        path = os.path.join(folder, f"{safe_name(f'{prefix}{base}', 120)}-{n}{ext.lower()}")
    with open(path, "wb") as fh:
        fh.write(data)
    pb_batch(token, [{
        "method": "POST", "url": f"/api/collections/{DOCUMENTS_COLLECTION}/records",
        "body": {"kind": kind, "ledger_key": ledger_key, "month": month, "filename": os.path.basename(path),
                 "stored_path": path, "size": len(data), "group_key": group_key, "extracted": extracted,
                 "sha256": hashlib.sha256(data).hexdigest(),
                 "uploaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
    }])
    return os.path.relpath(path, WISE_DOCS_DIR)


def _delete_document(token, doc):
    path = os.path.realpath(doc.get("stored_path") or "")
    if path.startswith(os.path.realpath(WISE_DOCS_DIR) + os.sep) and os.path.exists(path):
        os.remove(path)
    pb_batch(token, [{"method": "DELETE", "url": f"/api/collections/{DOCUMENTS_COLLECTION}/records/{doc['id']}"}])


def _key_ok(request, body=None):
    args = getattr(request, "args", {}) or {}
    return not WISE_REFRESH_KEY or (args.get("key") or (body or {}).get("key")) == WISE_REFRESH_KEY


def attach_invoice(token, ledger_key, filename, data, extracted=None, extra_keys=None, add=False):
    """One invoice per transaction (per the user): any existing document on
    this transaction is replaced, not added to."""
    found = pb_list_all(token, LEDGER_COLLECTION, filter_str=f"ledger_key = {json.dumps(ledger_key)}")
    if not found:
        raise KeyError("Unknown transaction")
    row = found[0]
    # A card payment Wise split across two currency balances (insufficient
    # funds in one) is ONE transaction id with a row per balance - the invoice
    # covers all parts (real case: CARD-3993926976 = EUR 10.46 + USD 13.31).
    siblings = [r["ledger_key"] for r in pb_list_all(token, LEDGER_COLLECTION, filter_str=f"wise_id = {json.dumps(row.get('wise_id') or '')}")
                if r["ledger_key"] != ledger_key and r.get("direction") == row.get("direction")] if row.get("wise_id") else []
    # One invoice paid in several transfers - the user picks the extra ones.
    siblings += [k for k in (extra_keys or []) if k and k != ledger_key and k not in siblings]
    if not add:
        # Default: one document per transaction - a new upload replaces it.
        # add=True: several invoices paid by ONE transaction (per the user,
        # e.g. 3 invoices settled in one payment) - keep the existing ones.
        for d in docs_covering(token, [ledger_key] + siblings):
            _delete_document(token, d)
    if extracted is None:
        try:
            extracted = extract_invoice_fields(filename, data)  # for the VAT summary
        except Exception:
            extracted = None
    local = local_parts(row["occurred_at"])
    prefix = f"{local:%Y-%m-%d}_{safe_name(row.get('counterparty'), 40)}_{row.get('amount') or 0:.2f}{row.get('currency')}_"
    stored = store_document(token, "invoice", f"{local:%Y-%m}", filename or "invoice", data,
                            ledger_key=ledger_key, prefix=prefix, extracted=extracted)
    if siblings:
        doc = pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"stored_path = {json.dumps(os.path.join(WISE_DOCS_DIR, stored))}")
        if doc:
            pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{DOCUMENTS_COLLECTION}/records/{doc[0]['id']}",
                              "body": {"extra_ledger_keys": siblings}}])
    return stored


def docs_covering(token, keys):
    """Invoice documents attached to any of these ledger keys (primary or as
    an extra key of a split transaction)."""
    out = []
    for d in pb_list_all(token, DOCUMENTS_COLLECTION, filter_str="kind = 'invoice'"):
        if d.get("ledger_key") in keys or set(d.get("extra_ledger_keys") or []) & set(keys):
            out.append(d)
    return out


def in_period(month, period):
    """period = "YYYY-MM" (one month selected on the page), "YYYY" (all months
    of a year) or empty (no restriction). Per the user (2026-10-01): uploads
    are only accepted for the period selected on the page."""
    return not period or (month or "").startswith(period)


def period_label(period):
    return period if period else "any period"


def find_duplicate(token, data, fields=None):
    """An already-stored copy of this invoice: identical file bytes, or the
    same invoice number from the same vendor (a re-saved/renamed copy)."""
    digest = hashlib.sha256(data).hexdigest()
    found = pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"sha256 = {json.dumps(digest)}")
    identical = bool(found)
    if not found and fields and fields.get("invoice_number"):
        number = fields["invoice_number"].strip().lower()
        vendor = normalize_name(fields.get("vendor"))
        for d in pb_list_all(token, DOCUMENTS_COLLECTION, filter_str="kind = 'invoice'"):
            ex = d.get("extracted") or {}
            # Same number AND same date: tax-office forms reuse a FORM number
            # ("3310-CA3" on every French VAT return) - only the date/period differs.
            same_date = (ex.get("invoice_date") or "") == (fields.get("invoice_date") or "")
            if (ex.get("invoice_number") or "").strip().lower() == number and normalize_name(ex.get("vendor")) == vendor and same_date:
                found = [d]
                break
    if not found:
        return None
    d = found[0]
    if d.get("ledger_key"):
        rows = pb_list_all(token, LEDGER_COLLECTION, filter_str=f"ledger_key = {json.dumps(d['ledger_key'])}")
        if rows:
            r = rows[0]
            where = f"{local_parts(r['occurred_at']):%Y-%m-%d} · {r.get('counterparty')} · {r.get('amount'):.2f} {r.get('currency')}"
        else:
            where = d["ledger_key"]
    elif d.get("group_key"):
        platform, _, cur = d["group_key"].partition("|")
        where = f"{platform.title()} {cur} {d.get('month')}" if platform not in SUPPLIER_BALANCES else f"{platform.title()} balance"
        if platform == "supplier":
            where = f"{cur.title()} balance"
    else:
        where = d.get("month")
    ex = d.get("extracted") or {}
    return {"id": d["id"], "filename": d.get("filename"), "attachedTo": where, "identical": identical,
            "number": ex.get("invoice_number"), "total": ex.get("total"), "currency": ex.get("currency")}


def UploadWiseInvoice(request):
    """POST JSON {ledgerKey, filename, contentBase64} (+ key) - saves (or
    replaces) the transaction's invoice in its month folder, named
    <date>_<counterparty>_<amount><CUR>_<original name>. With {groupKey,
    month} instead of ledgerKey it replaces a month-level group document
    (e.g. the monthly Amazon payouts document for one currency)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    body = request.get_json(silent=True) or {}
    if not _key_ok(request, body):
        return json_response({"error": "Unauthorized"}, 401)
    try:
        ledger_key = body.get("ledgerKey") or ""
        group_key, month = body.get("groupKey") or "", body.get("month") or ""
        data = base64.b64decode(body.get("contentBase64") or "")
        if group_key.startswith("supplier|") and data:
            supplier_id = group_key.split("|", 1)[1]
            if supplier_id not in SUPPLIER_BALANCES:
                return json_response({"error": "Unknown supplier"}, 400)
            token = pb_authenticate()
            stored = attach_supplier_invoice(token, supplier_id, body.get("filename"), data)
            return json_response({"ok": True, "storedAs": stored})
        if not data or not (ledger_key or (group_key and re.fullmatch(r"\d{4}-\d{2}", month))):
            return json_response({"error": "ledgerKey (or groupKey + month) and file content are required"}, 400)
        if len(data) > MAX_DOCUMENT_BYTES:
            return json_response({"error": "File too large (max 25 MB)"}, 400)
        token = pb_authenticate()
        if ledger_key:
            try:
                stored = attach_invoice(token, ledger_key, body.get("filename"), data,
                                        extra_keys=body.get("extraLedgerKeys") or [], add=bool(body.get("add")))
            except KeyError:
                return json_response({"error": "Unknown transaction"}, 404)
        else:
            for d in pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"group_key = {json.dumps(group_key)} && month = {json.dumps(month)}"):
                _delete_document(token, d)
            base, ext = os.path.splitext(body.get("filename") or "document")
            try:
                group_fields = extract_invoice_fields(body.get("filename"), data)
            except Exception:
                group_fields = None
            stored = store_document(token, "invoice", month, f"{safe_name(group_key.replace('|', '-'))}-{month}_{base}{ext}",
                                    data, group_key=group_key, extracted=group_fields)
        return json_response({"ok": True, "storedAs": stored})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def DeleteWiseDocument(request):
    """{id} (+ key) - removes a wrongly-attached invoice (file + record)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    body = request.get_json(silent=True) or {}
    if not _key_ok(request, body):
        return json_response({"error": "Unauthorized"}, 401)
    doc_id = body.get("id") or (getattr(request, "args", {}) or {}).get("id")
    try:
        token = pb_authenticate()
        found = pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"id = {json.dumps(doc_id or '')}")
        if not found:
            return json_response({"error": "Not found"}, 404)
        _delete_document(token, found[0])
        return json_response({"ok": True})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def GetWiseDocument(request):
    """?id=<wise_documents id>&key=... - streams the stored file (opened from
    the page in a new tab)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if not _key_ok(request):
        return json_response({"error": "Unauthorized"}, 401)
    doc_id = (getattr(request, "args", {}) or {}).get("id") or ""
    token = pb_authenticate()
    found = pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"id = {json.dumps(doc_id)}")
    if not found:
        return json_response({"error": "Not found"}, 404)
    path = os.path.realpath(found[0]["stored_path"])
    if not path.startswith(os.path.realpath(WISE_DOCS_DIR) + os.sep) or not os.path.exists(path):
        return json_response({"error": "File missing on disk"}, 404)
    with open(path, "rb") as fh:
        data = fh.read()
    content_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
    disposition = "inline" if content_type.startswith(("image/", "application/pdf", "text/")) else "attachment"
    return data, 200, {
        "Content-Type": content_type,
        "Content-Disposition": f'{disposition}; filename="{os.path.basename(path)}"',
        "Access-Control-Allow-Origin": cors_headers()["Access-Control-Allow-Origin"],
    }


# Suppliers paid against a running balance (advance payments, not one
# transfer per invoice) - per the user (2026-10-01), no invoice per transfer.
NO_INVOICE_PAYEES_RE = re.compile(r"malani|acacia", re.I)

# Their invoices go to a per-supplier balance document set (group_key
# "supplier|<id>") instead of a transaction; GetWiseSupplierBalances nets
# total paid against total invoiced.
SUPPLIER_BALANCES = {
    "malani": {"name": "Malani Impex", "re": re.compile(r"malani", re.I)},
    "acacia": {"name": "Acacia Fashion", "re": re.compile(r"acacia", re.I)},
    # Per the user (2026-10-01): money sent to the owner and "Other expense"
    # payments are ALSO running balances - covered by receipts added to the
    # balance, not one invoice per transfer. Matched by classification.
    # Per the user: ONLY transfers to Rafi / Refael Tamir himself.
    # Valued in what left the Wise balance (USD/EUR), not the ILS the owner
    # received - his receipts are issued in USD/EUR (2025 import, 2026-10-26).
    "owner": {"name": "Owner (Rafi Tamir)", "re": re.compile(r"\b(rafi|refael)\s+tamir\b", re.I), "value_sent": True},
    # Per the user (2026-10-03): kept only in EUR - every payment/receipt is
    # converted at the ECB rate of its own date.
    "other": {"name": "Other expenses", "classification": "Other expense", "currency": "EUR"},
}
BALANCE_CLASSIFICATIONS = {cfg["classification"] for cfg in SUPPLIER_BALANCES.values() if cfg.get("classification")}
# Payees covered by a name-matched running balance need no invoice per payment.
OWNER_RE = SUPPLIER_BALANCES["owner"]["re"]


def balance_matches(cfg, row, cls):
    if cfg.get("classification"):
        return cls == cfg["classification"]
    return bool(cfg["re"].search(row.get("counterparty") or ""))


def supplier_for(name):
    for sid, cfg in SUPPLIER_BALANCES.items():
        if cfg.get("re") and cfg["re"].search(name or ""):
            return sid
    return None


def attach_supplier_invoice(token, supplier_id, filename, data, extracted=None):
    """Adds (never replaces) an invoice to a supplier's running balance, filed
    in the month folder of the invoice date (upload month if unreadable)."""
    if extracted is None:
        try:
            extracted = extract_invoice_fields(filename, data)
        except Exception:
            extracted = None
    try:
        month = datetime.strptime((extracted or {}).get("invoice_date") or "", "%Y-%m-%d").strftime("%Y-%m")
    except ValueError:
        month = datetime.now(LOCAL_TZ).strftime("%Y-%m")
    total = (extracted or {}).get("total")
    prefix = f"{SUPPLIER_BALANCES[supplier_id]['name'].replace(' ', '-')}_" + (
        f"{total:.2f}{(extracted or {}).get('currency') or ''}_" if total else "")
    return store_document(token, "invoice", month, filename or "invoice", data,
                          prefix=prefix, group_key=f"supplier|{supplier_id}", extracted=extracted)


def invoice_required(row, reversed_card_ids):
    """Per the user (2026-10-01): every transaction needs an invoice except Wise
    fees (booked inside the transfer row, never their own row) and cashback.
    Also exempt: conversions between own balances, and card charges that were
    reversed (auth + reversal pair on the same card transaction id, net 0)."""
    details = (row.get("details_type") or "").upper()
    if details in ("CASHBACK", "CONVERSION"):
        return False
    if NO_INVOICE_PAYEES_RE.search(row.get("counterparty") or "") or \
            (row.get("direction") == "out" and OWNER_RE.search(row.get("counterparty") or "")):
        return False
    if row.get("wise_id") in reversed_card_ids:
        return False  # reversed / refunded: money came back, nothing to invoice
    return True


# ---------------------------------------------------------------- classification + read

def normalize_name(name):
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def rule_key(direction, counterparty):
    return f"{direction}|{normalize_name(counterparty)}"


def default_classification(row):
    direction = row.get("direction")
    details = (row.get("details_type") or "").upper()
    haystack = " | ".join([row.get("counterparty") or "", row.get("reference") or "", row.get("description") or ""])
    for d, types, rx, cls in _COMPILED_RULES:
        if d != direction or (types and details not in types):
            continue
        if rx.search(haystack):
            return cls
    return "Unclassified"


def classify(row, user_rules):
    # Per-transaction override (e.g. "move to Other expenses balance" from the
    # missing-invoices list) wins over the per-payee rule.
    row_rule = user_rules.get(f"row|{row.get('ledger_key')}")
    if row_rule:
        return row_rule, "row"
    rule = user_rules.get(rule_key(row.get("direction"), row.get("counterparty")))
    if rule and row.get("counterparty"):
        return rule, "rule"
    return default_classification(row), "default"


def local_parts(iso):
    dt = datetime.strptime(iso[:19], "%Y-%m-%dT%H:%M:%S" if "T" in iso[:19] else "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ)


_EUR_RATE_CACHE = {}  # (currency, "YYYY-MM-DD") -> units of currency per 1 EUR


def eur_rates_for(currencies, start_date, end_date):
    """Daily ECB reference rates (frankfurter, same source UpdateSkuSalesMonth
    uses) for converting each row to EUR on its own date. Weekends/holidays
    fall back to the latest earlier business day. Currencies frankfurter
    doesn't publish (e.g. AED) are simply absent -> that row gets no EUR value."""
    wanted = sorted(c for c in currencies if c and c != "EUR")
    if not wanted:
        return
    fetch_start = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    missing = [c for c in wanted if (c, end_date) not in _EUR_RATE_CACHE or (c, start_date) not in _EUR_RATE_CACHE]
    if not missing:
        return
    response = requests.get(
        f"https://api.frankfurter.dev/v1/{fetch_start}..{end_date}",
        params={"base": "EUR", "symbols": ",".join(missing)}, timeout=30,
    )
    response.raise_for_status()
    by_day = response.json().get("rates", {})
    day = datetime.strptime(fetch_start, "%Y-%m-%d").date()
    last_day = datetime.strptime(end_date, "%Y-%m-%d").date()
    last = {}
    while day <= last_day:
        key = day.strftime("%Y-%m-%d")
        last.update(by_day.get(key, {}))
        for cur, rate in last.items():
            _EUR_RATE_CACHE[(cur, key)] = rate
        day += timedelta(days=1)


# Currencies the ECB doesn't publish but that are pegged to one it does
# (units of the pegged currency per 1 unit of the anchor).
PEGGED_CURRENCIES = {"JOD": ("USD", 0.709), "AED": ("USD", 3.6725), "SAR": ("USD", 3.75)}


def to_eur(amount, currency, day):
    if currency == "EUR":
        return round(amount, 2)
    if currency in PEGGED_CURRENCIES:
        anchor, per_anchor = PEGGED_CURRENCIES[currency]
        anchor_rate = _EUR_RATE_CACHE.get((anchor, day))
        return round(amount / per_anchor / anchor_rate, 2) if anchor_rate else None
    rate = _EUR_RATE_CACHE.get((currency, day))
    return round(amount / rate, 2) if rate else None


def confirmed_currency_months(imports):
    """{(currency, year, month)} fully covered by an imported statement - a
    statement only confirms a calendar month if its period spans all of it."""
    out = set()
    for imp in imports:
        try:
            start = datetime.strptime(imp["period_start"], "%Y-%m-%d").date()
            end = datetime.strptime(imp["period_end"], "%Y-%m-%d").date()
        except (KeyError, ValueError, TypeError):
            continue
        y, m = start.year, start.month
        while (y, m) <= (end.year, end.month):
            first = datetime(y, m, 1).date()
            nxt = datetime(y + (m == 12), m % 12 + 1, 1).date()
            if start <= first and end >= nxt - timedelta(days=1):
                for cur in imp.get("currencies") or []:
                    out.add((cur, y, m))
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def GetWiseLedger(request):
    """Read-only. ?year=YYYY&month=M (month=0 or absent = whole year; no year =
    most recent month with data). Returns that period's ledger rows (every
    currency, classification resolved), the list of periods with data, the
    classification options, and the latest balance snapshot."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    args = getattr(request, "args", {}) or {}
    try:
        token = pb_authenticate()
        rules = {r["rule_key"]: r["classification"] for r in pb_list_all(token, RULES_COLLECTION)}
        ledger = pb_list_all(token, LEDGER_COLLECTION, sort="-occurred_at")
        periods = {}
        for row in ledger:
            if not row.get("occurred_at"):
                continue
            local = local_parts(row["occurred_at"])
            row["_y"], row["_m"] = local.year, local.month
            row["_local"] = local.strftime("%Y-%m-%d %H:%M")
            periods[(local.year, local.month)] = periods.get((local.year, local.month), 0) + 1
        period_list = [{"year": y, "month": m, "count": c} for (y, m), c in sorted(periods.items(), reverse=True)]

        year = int(args.get("year") or 0)
        month = int(args.get("month") or 0)
        if not year and period_list:
            year, month = period_list[0]["year"], period_list[0]["month"]

        # A transaction id with money both out AND back in on the same balance
        # was reversed: a card check, or a transfer refunded/cancelled later
        # (real case: TRANSFER-1901667330 "jai niwas" EUR 32.94 out 3.1, back 6.1).
        id_dirs = {}
        for row in ledger:
            if row.get("wise_id"):
                id_dirs.setdefault((row.get("wise_id"), row.get("currency")), set()).add(row.get("direction"))
        reversed_card_ids = {wid for (wid, _cur), dirs in id_dirs.items() if dirs == {"in", "out"}}

        month_keys = {f"{year}-{month:02d}"} if month else {f"{year}-{m:02d}" for m in range(1, 13)}
        docs = [d for d in pb_list_all(token, DOCUMENTS_COLLECTION, sort="uploaded_at") if d.get("month") in month_keys]
        invoices_by_key, group_docs, statements, reports = {}, {}, [], {}
        for d in docs:
            ex = d.get("extracted") or {}
            entry = {"id": d["id"], "filename": d.get("filename"), "month": d.get("month"), "groupKey": d.get("group_key") or "",
                     "number": ex.get("invoice_number") or "", "total": ex.get("total"), "currency": (ex.get("currency") or "").upper(),
                     "differenceAccepted": bool(ex.get("difference_accepted"))}
            if d.get("kind") == "report":
                reports[(d.get("month"), d.get("group_key"))] = entry
            elif d.get("kind") != "invoice":
                statements.append(entry)
            elif d.get("group_key"):
                # One sales invoice can cover several currencies of the same
                # platform/month (real case: June Amazon EUR invoice incl. GBP).
                covered = [d.get("group_key")] + list(d.get("extra_group_keys") or [])
                entry["coveredGroups"] = covered
                for gk in covered:
                    group_docs.setdefault((d.get("month"), gk), []).append(entry)
            else:
                for k in [d.get("ledger_key")] + list(d.get("extra_ledger_keys") or []):
                    invoices_by_key.setdefault(k, []).append(entry)

        rows = []
        for row in ledger:
            if row.get("_y") != year or (month and row.get("_m") != month):
                continue
            cls, cls_source = classify(row, rules)
            if row.get("wise_id") in reversed_card_ids and cls_source == "default" and \
                    (row.get("details_type") or "").upper() != "CARD":
                cls = "Refund / reversal"
            rows.append({
                "key": row["ledger_key"], "wiseId": row.get("wise_id"), "source": row.get("source"),
                "date": row.get("_local"), "currency": row.get("currency"), "direction": row.get("direction"),
                "amount": row.get("amount") or 0, "fee": row.get("fee") or 0,
                "counterparty": row.get("counterparty") or "", "counterpartyInferred": bool(row.get("counterparty_inferred")),
                "description": row.get("description") or "", "reference": row.get("reference") or "",
                "detailsType": row.get("details_type") or "",
                "isAtm": is_atm(row),
                "exchangeCurrency": row.get("exchange_currency") or "", "exchangeAmount": row.get("exchange_amount") or 0,
                "classification": cls, "classificationSource": cls_source,
                "invoiceRequired": invoice_required(row, reversed_card_ids) and cls not in BALANCE_CLASSIFICATIONS
                and f"noinv|{row['ledger_key']}" not in rules,
                "noDocument": f"noinv|{row['ledger_key']}" in rules,
                "balanceAccount": next((sid for sid, cfg in SUPPLIER_BALANCES.items()
                                        if row.get("direction") == "out" and balance_matches(cfg, row, cls)), ""),
                # Amazon payouts are covered by one document per currency per
                # month (see build_amazon_monthly_docs), not per transaction.
                "invoiceGroup": group_invoice_key(cls, row.get("currency")),
                "invoices": (group_docs.get((f"{row['_y']}-{row['_m']:02d}", group_invoice_key(cls, row.get("currency"))), [])
                             if group_invoice_key(cls, row.get("currency")) else invoices_by_key.get(row["ledger_key"], [])),
                "report": (reports.get((f"{row['_y']}-{row['_m']:02d}", amazon_report_key(row.get("currency"))))
                           if cls == AMAZON_CLASS else None),
            })

        if rows:
            days = sorted(r["date"][:10] for r in rows)
            try:
                eur_rates_for({r["currency"] for r in rows}, days[0], days[-1])
            except requests.RequestException:
                pass  # page still works per-currency; EUR totals just show as unavailable
        for r in rows:
            r["eurAmount"] = to_eur(r["amount"], r["currency"], r["date"][:10])
            r["eurFee"] = to_eur(r["fee"], r["currency"], r["date"][:10]) if r["fee"] else 0

        confirmed = confirmed_currency_months(pb_list_all(token, IMPORTS_COLLECTION))
        currencies_in_view = sorted({r["currency"] for r in rows} | {c for (c, y, m) in confirmed if y == year and (not month or m == month)})
        months_in_view = [month] if month else sorted({p["month"] for p in period_list if p["year"] == year})
        confirmation = {
            cur: {
                "confirmedMonths": [m for m in months_in_view if (cur, year, m) in confirmed],
                "partialMonths": [m for m in months_in_view if (cur, year, m) not in confirmed],
            }
            for cur in currencies_in_view
        }
        for r in rows:
            y, m = int(r["date"][:4]), int(r["date"][5:7])
            r["monthConfirmed"] = (r["currency"], y, m) in confirmed

        snapshots = pb_list_all(token, "wise_balances", sort="-snapshot_at")
        seen, balances = set(), []
        for b in snapshots:
            if b["currency"] in seen:
                continue
            seen.add(b["currency"])
            balances.append({"currency": b["currency"], "amount": b.get("amount") or 0, "asOf": b.get("snapshot_at")})

        # VAT/GST summary from the uploaded invoices. A document attached to a
        # "Taxes & VAT" transaction is a payment to a tax office (VAT return),
        # anything else is VAT charged on a purchase (input VAT).
        tax_payment_keys = {r["ledger_key"] for r in ledger
                            if r.get("direction") == "out" and classify(r, rules)[0] == "Taxes & VAT"}
        vat = {}
        for d in docs:
            ex = d.get("extracted") or {}
            tax = ex.get("tax_amount") or 0
            if d.get("kind") != "invoice" or not tax or (d.get("group_key") or "").split("|")[0] in GROUP_INVOICE_CLASSES.values():
                continue  # invoices TO Amazon are sales (output VAT), not VAT the user paid
            cur = (ex.get("currency") or "").upper()
            kind = "Paid to tax office" if d.get("ledger_key") in tax_payment_keys else "Input VAT on purchases"
            key = (kind, (ex.get("tax_country") or "??").upper(), (ex.get("tax_type") or "Tax").upper(), cur)
            b = vat.setdefault(key, {"kind": kind, "country": key[1], "taxType": key[2], "currency": cur, "tax": 0.0, "eur": 0.0,
                                     "count": 0, "eurMissing": 0, "invoices": []})
            b["tax"] += tax
            b["count"] += 1
            day = (ex.get("invoice_date") or f"{d.get('month')}-15")[:10]
            try:
                eur_rates_for({cur}, day, day)
            except requests.RequestException:
                pass
            eur = to_eur(tax, cur, day) if cur else None
            if eur is None:
                b["eurMissing"] += 1
            else:
                b["eur"] += eur
            b["invoices"].append({"id": d["id"], "vendor": ex.get("vendor"), "date": ex.get("invoice_date"),
                                  "tax": tax, "rate": ex.get("tax_rate"), "number": ex.get("invoice_number")})
        vat_summary = sorted(vat.values(), key=lambda b: (b["kind"] != "Input VAT on purchases", -b["eur"]))
        unread_invoices = sum(1 for d in docs if d.get("kind") == "invoice" and not d.get("group_key", "").startswith("amazon|")
                              and not (d.get("extracted") or {}).get("vendor"))

        return json_response({
            "year": year, "month": month, "rows": rows, "periods": period_list, "confirmation": confirmation,
            "vatSummary": vat_summary, "invoicesNotRead": unread_invoices,
            "incomeClasses": INCOME_CLASSES, "expenseClasses": EXPENSE_CLASSES, "feesClass": FEES_CLASS,
            "netAgainstIncomeClasses": NET_AGAINST_INCOME_CLASSES,
            "balances": balances, "statements": statements,
            "groupDocs": [dict(e, month=m) for (m, g), es in group_docs.items() for e in es],
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def SetWiseClassificationRule(request):
    """{direction, counterparty, classification} (+ key). Applies to every
    ledger row with that counterparty in that direction; an empty
    classification removes the rule (back to the default)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    args = getattr(request, "args", {}) or {}
    body = request.get_json(silent=True) or {}
    params = {**{k: args.get(k) for k in ("key", "direction", "counterparty", "classification") if args.get(k) is not None}, **body}
    if WISE_REFRESH_KEY and params.get("key") != WISE_REFRESH_KEY:
        return json_response({"error": "Unauthorized"}, 401)
    if params.get("ledgerKey"):
        # Single-transaction override, stored as rule_key "row|<ledger_key>".
        params["counterparty"] = f"row|{params['ledgerKey']}"
    direction = params.get("direction")
    counterparty = (params.get("counterparty") or "").strip()
    classification = (params.get("classification") or "").strip()
    if direction not in ("in", "out") or not counterparty:
        return json_response({"error": "direction (in/out) and counterparty are required"}, 400)
    allowed = INCOME_CLASSES if direction == "in" else EXPENSE_CLASSES
    if classification and classification not in allowed:
        return json_response({"error": f"Unknown classification '{classification}'"}, 400)
    try:
        token = pb_authenticate()
        key = counterparty if counterparty.startswith("row|") else rule_key(direction, counterparty)
        existing = pb_list_all(token, RULES_COLLECTION, filter_str=f'rule_key = {json.dumps(key)}')
        ops = []
        if not classification:
            ops = [{"method": "DELETE", "url": f"/api/collections/{RULES_COLLECTION}/records/{r['id']}"} for r in existing]
        elif existing:
            ops = [{"method": "PATCH", "url": f"/api/collections/{RULES_COLLECTION}/records/{existing[0]['id']}",
                    "body": {"classification": classification}}]
        else:
            ops = [{"method": "POST", "url": f"/api/collections/{RULES_COLLECTION}/records",
                    "body": {"rule_key": key, "direction": direction, "counterparty": counterparty, "classification": classification}}]
        if ops:
            pb_batch(token, ops)
        return json_response({"ok": True, "ruleKey": key, "classification": classification or None})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


# ---------------------------------------------------------------- Amazon monthly payout documents

AMAZON_CLASS = "Amazon payout"

# Income the user invoices ONCE per currency per month (per the user,
# 2026-10-01): e.g. Sep 2026 = 4 invoices - Etsy USD + Amazon USD/EUR/GBP.
GROUP_INVOICE_CLASSES = {"Amazon payout": "amazon", "Etsy payout": "etsy"}
# Fuzzy on purpose: real invoices/filenames had typos ("Amaon GB", "Etsi Inc").
GROUP_CUSTOMER_RE = {"amazon": re.compile(r"amazon|amaon|amzon|amzn", re.I), "etsy": re.compile(r"ets[yi]\b|etsy", re.I)}
OWN_COMPANY_RE = re.compile(r"mandala\s*life", re.I)


def group_invoice_key(cls, currency):
    prefix = GROUP_INVOICE_CLASSES.get(cls)
    return f"{prefix}|{currency}" if prefix else ""


def _parse_dt(text):
    return datetime.strptime(str(text)[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")


def amazon_group_key(currency):
    """The user's own monthly invoice to Amazon for this currency (required)."""
    return f"amazon|{currency}"


def amazon_report_key(currency):
    """Our generated monthly payouts report - supporting document only. Per the
    user (2026-10-01) it does NOT replace the invoice they issue to Amazon."""
    return f"amazon-report|{currency}"


def match_settlements(rows, settlements):
    """{ledger_key: settlement} for Amazon rows whose amount equals exactly one
    settlement's payout in that currency, deposited 0-10 days after Amazon's
    FundTransferDate. ConvertedTotal (stored as converted_amount_usd - despite
    the name it's in the DEPOSIT currency, e.g. EUR for SE/PL/AE payouts) is
    compared when the settlement currency differs from the Wise balance.
    Payouts that aren't Seller Central settlements (Merch by Amazon, KDP, other
    accounts) find nothing and stay unmatched."""
    by_amount = {}
    for s in settlements:
        if not s.get("fund_transfer_date"):
            continue
        for cur_amount in ((s.get("currency"), s.get("original_amount")), ("*", s.get("converted_amount_usd"))):
            if cur_amount[1]:
                by_amount.setdefault(round(cur_amount[1], 2), []).append((cur_amount[0], s))
    out = {}
    for row in rows:
        signed = round(row["amount"] if row["direction"] == "in" else -row["amount"], 2)
        when = _parse_dt(row["occurred_at"])
        found = []
        for cur, s in by_amount.get(signed, []):
            if cur not in ("*", row["currency"]):
                continue
            if cur == "*" and s.get("currency") == row["currency"]:
                continue
            if timedelta(days=-1) <= when - _parse_dt(s["fund_transfer_date"]) <= timedelta(days=10):
                found.append(s)
        if len({s["group_id"] for s in found}) == 1:
            out[row["ledger_key"]] = found[0]
    return out


def build_amazon_monthly_docs(token, only_month=None, regenerate=False):
    """Per the user (2026-10-01): Amazon payouts are covered by ONE document
    per currency per month whose total equals that month's Amazon payout value
    (deposits + Amazon card refunds - Amazon card charges). Generated only for
    finished months; an existing group document (generated or the user's own
    upload) is left alone unless regenerate=True."""
    from AmazonSettlementDoc import amazon_monthly_pdf

    rules = {r["rule_key"]: r["classification"] for r in pb_list_all(token, RULES_COLLECTION)}
    docs = pb_list_all(token, DOCUMENTS_COLLECTION)
    have = {(d.get("month"), d.get("group_key")) for d in docs if d.get("group_key")}
    this_month = datetime.now(LOCAL_TZ).strftime("%Y-%m")
    groups = {}
    for row in pb_list_all(token, LEDGER_COLLECTION):
        if not row.get("occurred_at") or classify(row, rules)[0] != AMAZON_CLASS:
            continue
        local = local_parts(row["occurred_at"])
        month = f"{local:%Y-%m}"
        if month >= this_month or (only_month and month != only_month):
            continue
        row["_local"] = local
        groups.setdefault((month, row["currency"]), []).append(row)
    todo = {k: v for k, v in groups.items() if regenerate or (k[0], amazon_report_key(k[1])) not in have}
    if not todo:
        return {"amazonMonthlyDocs": 0}
    settlements = pb_list_all(token, "amazon_settlements")
    matched = match_settlements([r for rows in todo.values() for r in rows], settlements)
    group_ids = sorted({s["group_id"] for s in matched.values()})
    events = {}
    for i in range(0, len(group_ids), 40):
        chunk = group_ids[i:i + 40]
        flt = " || ".join(f"group_id = {json.dumps(g)}" for g in chunk)
        for e in pb_list_all(token, "amazon_expense_events", filter_str=flt):
            events.setdefault(e["group_id"], []).append(e)
    written = 0
    for (month, currency), rows in sorted(todo.items()):
        rows.sort(key=lambda r: r["occurred_at"])
        entries = []
        for r in rows:
            s = matched.get(r["ledger_key"])
            entries.append({"row": r, "settlement": s, "events": events.get(s["group_id"], []) if s else []})
        if regenerate:
            for d in docs:
                if d.get("month") == month and d.get("group_key") == amazon_report_key(currency):
                    _delete_document(token, d)
        pdf = amazon_monthly_pdf(month, currency, entries)
        store_document(token, "report", month, f"Amazon-payouts-{currency}-{month}.pdf", pdf,
                       group_key=amazon_report_key(currency))
        written += 1
    return {"amazonMonthlyDocs": written}


# ---------------------------------------------------------------- bulk invoice upload + AI matching

INVOICE_SCHEMA = {
    "type": "object",
    "properties": {
        "vendor": {"type": "string", "description": "Who issued the invoice (seller / supplier / service)"},
        "customer": {"type": "string", "description": "Who the invoice is billed to"},
        "invoice_number": {"type": "string"},
        "invoice_date": {"type": "string", "description": "YYYY-MM-DD, empty if not found"},
        "total": {"type": "number", "description": "Grand total payable, incl. tax"},
        "currency": {"type": "string", "description": "ISO 4217 code, e.g. EUR, USD"},
        "is_invoice_or_receipt": {"type": "boolean"},
        "description": {"type": "string", "description": "One short line: what was bought"},
        "tax_amount": {"type": "number", "description": "Total VAT/GST/sales tax charged on the invoice, 0 if none"},
        "tax_type": {"type": "string", "description": "VAT, GST, Sales tax, ... - empty if no tax"},
        "tax_rate": {"type": "number", "description": "Main tax rate in percent, 0 if none / reverse charge"},
        "tax_country": {"type": "string", "description": "ISO 3166-1 alpha-2 country whose tax this is "
                                                          "(issuer's VAT/GST registration), empty if no tax"},
        "reverse_charge": {"type": "boolean", "description": "Invoice says reverse charge / VAT exempt intra-EU"},
    },
    "required": ["vendor", "customer", "invoice_number", "invoice_date", "total", "currency",
                 "is_invoice_or_receipt", "description", "tax_amount", "tax_type", "tax_rate", "tax_country",
                 "reverse_charge"],
    "additionalProperties": False,
}

# Per the user (2026-10-01): cheaper model for invoice/statement reading after
# the shared Anthropic credit ran out (~2.5x cheaper than Opus 5).
WISE_AI_MODEL = os.environ.get("WISE_AI_MODEL", "claude-sonnet-5")

_MEDIA_TYPES = {".pdf": "application/pdf", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".webp": "image/webp", ".gif": "image/gif"}


SHEET_EXTENSIONS = {".xls", ".xlsx", ".ods", ".csv"}
SHEET2CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools", "sheet2csv")
BUN = os.environ.get("BUN_PATH", os.path.expanduser("~/.bun/bin/bun"))


def spreadsheet_to_text(filename, data):
    """No Excel reader in this Python env - shells out to a tiny SheetJS
    script under bun (tools/sheet2csv) that prints every sheet as CSV."""
    import subprocess
    import tempfile
    ext = os.path.splitext(filename or "")[1].lower()
    if ext == ".csv":
        return data.decode("utf-8", "replace")
    with tempfile.NamedTemporaryFile(suffix=ext) as tmp:
        tmp.write(data)
        tmp.flush()
        out = subprocess.run([BUN, os.path.join(SHEET2CSV_DIR, "sheet2csv.mjs"), tmp.name],
                             capture_output=True, text=True, timeout=60, cwd=SHEET2CSV_DIR)
    if out.returncode != 0:
        raise RuntimeError(f"Could not read spreadsheet: {out.stderr[:200]}")
    return out.stdout[:200000]


def extract_invoice_fields(filename, data):
    """Reads an invoice/receipt with Claude (structured output) -> dict per
    INVOICE_SCHEMA, or None for file types it can't read (Word/Excel/zip -
    those fall back to manual assignment on the page)."""
    import anthropic

    ext = os.path.splitext(filename or "")[1].lower()
    media_type = _MEDIA_TYPES.get(ext)
    if ext in SHEET_EXTENSIONS:
        # Spreadsheet invoices (real case: Acacia's .xls invoices) - converted
        # to CSV text and sent as a plain-text document.
        block = {"type": "document", "source": {"type": "text", "media_type": "text/plain",
                                                "data": spreadsheet_to_text(filename, data)}}
    elif not media_type:
        return None
    else:
        b64 = base64.standard_b64encode(data).decode()
        block = ({"type": "document", "source": {"type": "base64", "media_type": media_type, "data": b64}}
                 if media_type == "application/pdf"
                 else {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}})
    client = anthropic.Anthropic()
    response = client.beta.messages.create(
        model=WISE_AI_MODEL,
        max_tokens=4000,
        thinking={"type": "adaptive"},
        output_config={"effort": "low", "format": {"type": "json_schema", "schema": INVOICE_SCHEMA}},
        messages=[{"role": "user", "content": [
            block,
            {"type": "text", "text": (
                f"File name: {filename}\n"
                "Extract the fields of this invoice / receipt. The customer is usually Mandala Life ART OÜ "
                "(an Estonian e-commerce company). 'total' is the final amount actually payable in the "
                "invoice's own currency. invoice_number must identify THIS document: for tax returns / "
                "declarations where only a form number is printed (e.g. French 3310-CA3), append the tax "
                "period, e.g. '3310-CA3 2026-06'; invoice_date for those = the filing / payment date. "
                "Use empty strings for anything not present."
            )},
        ]}],
    )
    if response.stop_reason == "refusal":
        return None
    text = next((b.text for b in response.content if b.type == "text"), "")
    return json.loads(text) if text else None


def _name_score(a, b):
    stop = {"ltd", "llc", "inc", "gmbh", "ug", "ou", "oü", "sa", "sarl", "bv", "the", "and", "co", "limited",
            "haftung", "com", "www", "pvt"}
    def tokens(x):
        return {t for t in re.findall(r"[a-z0-9]{3,}", (x or "").lower()) if t not in stop}
    ta, tb = tokens(a), tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / min(len(ta), len(tb))


def match_invoice_to_ledger(fields, ledger, documented_keys, rules):
    """Scores every outgoing/incoming row still missing its invoice. Strong
    match = exact amount (row amount, row amount net of Wise fee, or the
    converted amount the payee received) in the invoice currency + dated
    -15..+45 days from the invoice date. Name overlap breaks ties."""
    total = fields.get("total") or 0
    currency = (fields.get("currency") or "").upper()
    try:
        inv_date = datetime.strptime(fields.get("invoice_date") or "", "%Y-%m-%d")
    except ValueError:
        inv_date = None
    by_wise_id = {}
    for row in ledger:
        if row.get("wise_id"):
            by_wise_id.setdefault((row["wise_id"], row.get("direction")), []).append(row)
    scored, seen_groups = [], set()
    for row in ledger:
        if row["ledger_key"] in documented_keys or not row.get("occurred_at"):
            continue
        group = (row.get("wise_id"), row.get("direction"))
        if row.get("wise_id") and len(by_wise_id.get(group, [])) > 1:
            if group in seen_groups:
                continue  # one candidate per split transaction; attach covers all parts
            seen_groups.add(group)
        if not invoice_required(row, set()) or f"noinv|{row['ledger_key']}" in rules or \
                classify(row, rules)[0] in set(GROUP_INVOICE_CLASSES) | BALANCE_CLASSIFICATIONS:
            continue
        when = _parse_dt(row["occurred_at"])
        amount_hit = False
        if total:
            options = []
            if row.get("currency") == currency:
                options += [row.get("amount") or 0, (row.get("amount") or 0) - (row.get("fee") or 0)]
            if row.get("exchange_currency") == currency:
                options.append(row.get("exchange_amount") or 0)
            parts = by_wise_id.get((row.get("wise_id"), row.get("direction")), [])
            if len(parts) > 1:
                # Split payment: compare the invoice with the sum of all parts.
                if all(p.get("exchange_currency") == currency for p in parts):
                    options.append(sum(p.get("exchange_amount") or 0 for p in parts))
                if all(p.get("currency") == currency for p in parts):
                    options.append(sum(p.get("amount") or 0 for p in parts))
            amount_hit = any(abs(o - total) < 0.015 for o in options)
        days = (when - inv_date).days if inv_date else None
        # Payment usually follows the invoice within weeks; 120 days let an
        # August Google invoice "match" next month's identical charge (real case).
        date_ok = days is None or -15 <= days <= 45
        vendor = "" if OWN_COMPANY_RE.search(fields.get("vendor") or "") else fields.get("vendor")
        name = _name_score(vendor, row.get("counterparty")) + 0.5 * _name_score(vendor, row.get("description"))
        if not (amount_hit or name >= 0.5):
            continue
        score = (3 if amount_hit else 0) + (1 if date_ok and days is not None else 0) + min(name, 1.5)
        if days is not None:
            # Recurring charges (same amount every month): the payment closest
            # to the invoice date wins clearly over last/next month's.
            score -= min(abs(days), 60) / 30
        if days is not None and not date_ok:
            score -= 2
        scored.append((score, amount_hit and date_ok, row))
    scored.sort(key=lambda x: -x[0])
    return scored


def _candidate_json(row):
    local = local_parts(row["occurred_at"])
    return {"key": row["ledger_key"], "date": f"{local:%Y-%m-%d}", "counterparty": row.get("counterparty") or "",
            "amount": row.get("amount") or 0, "currency": row.get("currency"), "direction": row.get("direction")}


def AutoMatchWiseInvoice(request):
    """POST JSON {filename, contentBase64} (+ key): one file of a bulk upload.
    Reads it with Claude, and if exactly one transaction is a strong match,
    attaches it there (replacing any existing invoice is NOT done here - only
    transactions still missing an invoice are candidates). Otherwise returns
    the best candidates so the page can ask the user to pick."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    body = request.get_json(silent=True) or {}
    if not _key_ok(request, body):
        return json_response({"error": "Unauthorized"}, 401)
    try:
        filename = body.get("filename") or "invoice"
        data = base64.b64decode(body.get("contentBase64") or "")
        if not data:
            return json_response({"error": "No file content"}, 400)
        if len(data) > MAX_DOCUMENT_BYTES:
            return json_response({"error": "File too large (max 25 MB)"}, 400)
        token = pb_authenticate()
        dup = find_duplicate(token, data)
        if dup:
            return json_response({"matched": False, "duplicate": dup})
        try:
            fields = extract_invoice_fields(filename, data)
        except Exception as exc:  # unreadable file / API hiccup -> manual assignment, not a hard failure
            fields, extract_error = None, str(exc)
        else:
            extract_error = None
        dup = find_duplicate(token, data, fields)
        if dup and not dup["identical"]:
            # Same invoice number, different file = a corrected / re-issued
            # version (real case: MA2600031 re-issued for $1,514.80 instead of
            # $1,514.00) - offer to replace the old one instead of refusing.
            if body.get("replace"):
                _delete_document(token, {"id": dup["id"], "stored_path": next(
                    (d.get("stored_path") for d in pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"id = {json.dumps(dup['id'])}")), "")})
                dup = None
            else:
                old_total = f"{dup['total']:,.2f} {dup.get('currency') or ''}" if dup.get("total") else "?"
                new_total = f"{(fields or {}).get('total') or 0:,.2f} {(fields or {}).get('currency') or ''}"
                return json_response({
                    "matched": False, "extracted": fields, "canReplace": True, "candidates": [],
                    "conflict": f"Invoice {dup.get('number')} is already attached to {dup['attachedTo']} "
                                f"(old version: {old_total}, this file: {new_total}) - a corrected version?",
                })
        if dup:
            return json_response({"matched": False, "duplicate": dup, "extracted": fields})
        # The user's own monthly sales invoice TO Amazon / Etsy covers all of
        # that platform's payouts in one currency for one month. Matched by the
        # data in the summary table: customer platform + currency + the month
        # whose payout total equals the invoice total (fallback: invoice-date month).
        group = match_income_group_invoice(token, fields, filename) if fields else None
        period = (body.get("period") or "").strip()
        if group and not in_period(group[1], period):
            platform, cur = group[0].split("|")
            return json_response({"matched": False, "extracted": fields, "candidates": [], "outOfPeriod": True,
                                  "conflict": f"This is the {platform.title()} {cur} invoice for {group[1]}, "
                                              f"but {period} is selected - switch the month to upload it."})
        if group:
            gkey, gmonth = group
            occupied = pb_list_all(token, DOCUMENTS_COLLECTION,
                                   filter_str=f"group_key = {json.dumps(gkey)} && month = {json.dumps(gmonth)}")
            if occupied and body.get("replace"):
                for d in occupied:
                    _delete_document(token, d)
                occupied = []
            if occupied:
                # A different invoice already covers that month - never silently
                # replace it (that deleted MA2600030 once); ask the user instead.
                other = (occupied[0].get("extracted") or {}).get("invoice_number") or occupied[0].get("filename")
                platform, cur = gkey.split("|")
                return json_response({
                    "matched": False, "extracted": fields, "canReplace": True,
                    "conflict": f"{platform.title()} {cur} {gmonth} already has invoice {other}.",
                    "candidates": [],
                })
            base, ext = os.path.splitext(filename)
            stored = store_document(token, "invoice", gmonth, f"{safe_name(gkey.replace('|', '-'))}-{gmonth}_{base}{ext}", data,
                                    group_key=gkey, extracted=fields)
            extra = detect_extra_currencies(token, gkey, gmonth, fields.get("total") or 0)
            if extra:
                doc = pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"stored_path = {json.dumps(os.path.join(WISE_DOCS_DIR, stored))}")
                if doc:
                    pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{DOCUMENTS_COLLECTION}/records/{doc[0]['id']}",
                                      "body": {"extra_group_keys": extra}}])
            platform, cur = gkey.split("|")
            return json_response({"matched": True, "groupLabel": f"{platform.title()} invoice {cur} {gmonth}",
                                  "extracted": fields, "storedAs": stored})
        supplier_id = supplier_for((fields or {}).get("vendor"))
        if supplier_id and not in_period((fields.get("invoice_date") or "")[:7], period):
            return json_response({"matched": False, "extracted": fields, "candidates": [], "outOfPeriod": True,
                                  "conflict": f"This {SUPPLIER_BALANCES[supplier_id]['name']} invoice is dated "
                                              f"{fields.get('invoice_date') or '?'}, but {period} is selected."})
        if supplier_id:
            stored = attach_supplier_invoice(token, supplier_id, filename, data, extracted=fields)
            return json_response({"matched": True, "supplierBalance": SUPPLIER_BALANCES[supplier_id]["name"],
                                  "extracted": fields, "storedAs": stored})
        rules = {r["rule_key"]: r["classification"] for r in pb_list_all(token, RULES_COLLECTION)}
        documented = set()
        for d in pb_list_all(token, DOCUMENTS_COLLECTION, fields="ledger_key,extra_ledger_keys"):
            documented.update(k for k in [d.get("ledger_key")] + list(d.get("extra_ledger_keys") or []) if k)
        ledger = [r for r in pb_list_all(token, LEDGER_COLLECTION)
                  if r.get("occurred_at") and in_period(f"{local_parts(r['occurred_at']):%Y-%m}", period)]
        # Score against ALL rows (incl. ones that already have an invoice) so a
        # match on an occupied transaction can be offered as a replacement.
        scored = match_invoice_to_ledger(fields or {}, ledger, set(), rules) if fields else []
        strong = [x for x in scored if x[1]]
        chosen = None
        if len(strong) == 1:
            chosen = strong[0][2]
        elif len(strong) > 1 and strong[0][0] - strong[1][0] >= 0.5:
            chosen = strong[0][2]
        if not chosen and fields and fields.get("total") and not body.get("noAutoAttach"):
            combo = find_payment_combination(fields, scored, documented)
            if combo:
                first, rest = combo[0], [r["ledger_key"] for r in combo[1:]]
                stored = attach_invoice(token, first["ledger_key"], filename, data, extracted=fields, extra_keys=rest)
                return json_response({"matched": True, "transaction": _candidate_json(first), "extracted": fields,
                                      "storedAs": stored, "alsoCovers": [_candidate_json(r) for r in combo[1:]]})
        if chosen and chosen["ledger_key"] in documented and body.get("add"):
            stored = attach_invoice(token, chosen["ledger_key"], filename, data, extracted=fields, add=True)
            return json_response({"matched": True, "transaction": _candidate_json(chosen), "extracted": fields,
                                  "storedAs": stored, "added": True})
        if chosen and chosen["ledger_key"] in documented and not body.get("replace"):
            existing = pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"ledger_key = {json.dumps(chosen['ledger_key'])}")
            name = existing[0].get("filename") if existing else "another document"
            return json_response({
                "matched": False, "extracted": fields, "canReplace": True, "canAdd": True,
                "conflict": f"Matches {_candidate_json(chosen)['date']} · {chosen.get('counterparty')} · "
                            f"{chosen.get('amount'):.2f} {chosen.get('currency')}, which already has {name}.",
                "candidates": [_candidate_json(r) for _, _, r in scored[:8]],
            })
        if chosen and body.get("noAutoAttach"):
            # After an explicit "Unattach" the user wants to pick: suggest, don't attach.
            scored.sort(key=lambda x: x[2]["ledger_key"] != chosen["ledger_key"])
            chosen = None
        if chosen:
            stored = attach_invoice(token, chosen["ledger_key"], filename, data, extracted=fields)
            return json_response({"matched": True, "transaction": _candidate_json(chosen), "extracted": fields,
                                  "storedAs": stored})
        return json_response({
            "matched": False, "extracted": fields, "extractError": extract_error,
            "candidates": [_candidate_json(r) for _, _, r in scored[:8]],
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def GetWiseSupplierBalances(request):
    """Read-only: per running-balance supplier, every payment (outgoing Wise
    transfer, valued in the currency the supplier received) vs every invoice
    filed to its balance (total/currency read from the invoice by Claude).
    balance > 0 = paid in advance, < 0 = owed to the supplier."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    try:
        token = pb_authenticate()
        ledger = pb_list_all(token, LEDGER_COLLECTION, sort="occurred_at")
        docs = [d for d in pb_list_all(token, DOCUMENTS_COLLECTION, sort="uploaded_at")
                if (d.get("group_key") or "").startswith("supplier|")]
        settings = {r["supplier_id"]: r for r in pb_list_all(token, SUPPLIER_SETTINGS_COLLECTION)}
        rules = {r["rule_key"]: r["classification"] for r in pb_list_all(token, RULES_COLLECTION)}
        # A payment that came straight back (same wise_id out + in on one
        # balance, e.g. a reversed ATM attempt) never counts in a balance.
        id_dirs = {}
        for row in ledger:
            if row.get("wise_id"):
                id_dirs.setdefault((row["wise_id"], row.get("currency")), set()).add(row.get("direction"))
        reversed_ids = {k for k, dirs in id_dirs.items() if dirs == {"in", "out"}}
        out = []
        for sid, cfg in SUPPLIER_BALANCES.items():
            setting = settings.get(sid) or {}
            start = setting.get("start_date") or ""
            payments = []
            for row in ledger:
                if row.get("direction") != "out" or not row.get("occurred_at"):
                    continue
                if (row.get("wise_id"), row.get("currency")) in reversed_ids:
                    continue
                if not balance_matches(cfg, row, classify(row, rules)[0] if cfg.get("classification") else None):
                    continue
                # Payments before the balance start date don't count in the
                # balance but are kept so the supplier's own full-history ledger
                # can still be matched line by line (Acacia, 2026-10-06).
                before = bool(start and f"{local_parts(row['occurred_at']):%Y-%m-%d}" < start)
                # Value a payment in what the payee received when known (a transfer
                # converted to INR/USD); card / ATM rows whose foreign amount wasn't
                # captured (exchange_amount 0, e.g. from PDF statements) fall back
                # to what left the Wise balance - never count them as 0.
                if cfg.get("value_sent"):  # the receipts are for the full amount sent (incl. Wise's fee)
                    received_cur, received = row.get("currency"), row.get("amount") or 0
                elif row.get("exchange_currency") and (row.get("exchange_amount") or 0) > 0:
                    received_cur, received = row["exchange_currency"], row["exchange_amount"]
                elif (row.get("details_type") or "").upper() == "CARD":
                    received_cur, received = row.get("currency"), row.get("amount") or 0
                else:
                    received_cur, received = row.get("currency"), (row.get("amount") or 0) - (row.get("fee") or 0)
                payments.append({"date": f"{local_parts(row['occurred_at']):%Y-%m-%d}", "currency": received_cur,
                                 "amount": round(received or 0, 2), "paidFrom": f"{row.get('amount') or 0:.2f} {row.get('currency')}",
                                 "fromAmount": row.get("amount") or 0, "fromCurrency": row.get("currency"),
                                 "key": row["ledger_key"], "counterparty": row.get("counterparty") or "",
                                 "description": row.get("description") or "", "reference": row.get("reference") or "",
                                 "type": "ATM" if is_atm(row) else (row.get("details_type") or "").title(),
                                 "fee": row.get("fee") or 0, "feeCurrency": row.get("currency"),
                                 "time": f"{local_parts(row['occurred_at']):%H:%M}", "beforeStart": before})
            all_payments = payments
            payments = [p for p in all_payments if not p["beforeStart"]]
            invoices = []
            for d in docs:
                if d.get("group_key") != f"supplier|{sid}":
                    continue
                ex = d.get("extracted") or {}
                invoices.append({"id": d["id"], "filename": d.get("filename"), "date": ex.get("invoice_date") or d.get("month"),
                                 "number": ex.get("invoice_number") or "", "total": ex.get("total"),
                                 "currency": (ex.get("currency") or "").upper(),
                                 "beforeStart": bool(start and (ex.get("invoice_date") or d.get("month") or "") < start)})
            all_invoices = invoices
            invoices = [i for i in all_invoices if not i["beforeStart"]]
            if cfg.get("currency"):
                to_cur = cfg["currency"]
                # Convert what left the Wise balance (not the foreign cash amount).
                for p in payments:
                    p["original"] = f"{p['amount']:,.2f} {p['currency']}"
                    p["amount"], p["currency"] = p["fromAmount"], p["fromCurrency"]
                items = payments + [i for i in invoices if i["total"] and i["currency"]]
                dates = [x["date"][:10] for x in items if x.get("date")]
                if dates:
                    eur_rates_for({PEGGED_CURRENCIES.get(x["currency"], (x["currency"],))[0] for x in items},
                                  min(dates), max(dates))
                for x in items:
                    key = "amount" if x in payments else "total"
                    if x["currency"] != to_cur:
                        conv = to_eur(x[key], x["currency"], (x.get("date") or "")[:10])
                        if conv is not None:
                            x["convertedFrom"] = f"{x[key]:,.2f} {x['currency']}"
                            x[key], x["currency"] = conv, to_cur
            by_cur = {}
            if setting.get("currency"):
                by_cur[setting["currency"]] = {"paid": setting.get("opening_balance") or 0.0, "invoiced": 0.0}
            for p in payments:
                by_cur.setdefault(p["currency"], {"paid": 0.0, "invoiced": 0.0})["paid"] += p["amount"]
            for i in invoices:
                if i["total"] and i["currency"]:
                    by_cur.setdefault(i["currency"], {"paid": 0.0, "invoiced": 0.0})["invoiced"] += i["total"]
            totals = [{"currency": c, "paid": round(v["paid"], 2), "invoiced": round(v["invoiced"], 2),
                       "balance": round(v["paid"] - v["invoiced"], 2)} for c, v in sorted(by_cur.items())]
            # Per-year roll-forward (per the user): opening = previous year's
            # closing, closing = opening + paid - invoiced.
            per = {}
            for pmt in payments:
                per.setdefault((pmt["currency"], pmt["date"][:4]), [0.0, 0.0])[0] += pmt["amount"]
            for inv in invoices:
                if inv["total"] and inv["currency"]:
                    per.setdefault((inv["currency"], (inv["date"] or "")[:4]), [0.0, 0.0])[1] += inv["total"]
            years = []
            for cur in sorted({c for c, _ in per} | ({setting["currency"]} if setting.get("currency") else set())):
                balance = (setting.get("opening_balance") or 0.0) if setting.get("currency") == cur else 0.0
                for y in sorted({yy for c, yy in per if c == cur}):
                    paid, invoiced = per[(cur, y)]
                    years.append({"currency": cur, "year": y, "opening": round(balance, 2), "paid": round(paid, 2),
                                  "invoiced": round(invoiced, 2), "closing": round(balance + paid - invoiced, 2)})
                    balance += paid - invoiced
            # The supplier's own account statement of us (e.g. Acacia's ledger,
            # uploaded 2026-10-03), kept as a report document with the entries
            # typed in - shown next to our balance on the same date so any
            # difference is visible. Sign follows ours: negative = we owe.
            supplier_ledger = None
            ledger_docs = [d for d in pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f'group_key = "supplier-ledger|{sid}"')]
            if ledger_docs:
                d = max(ledger_docs, key=lambda x: (x.get("extracted") or {}).get("as_of") or "")
                ex = d.get("extracted") or {}
                as_of, cur = ex.get("as_of") or "", (ex.get("currency") or "USD").upper()
                ours = (setting.get("opening_balance") or 0.0) if setting.get("currency", cur) == cur else 0.0
                ours += sum(p["amount"] for p in payments if p["currency"] == cur and p["date"] <= as_of)
                ours -= sum(i["total"] or 0 for i in invoices if i["currency"] == cur and (i["date"] or "") <= as_of)
                # Match each of their lines to ours: a sale/charge to one of our
                # uploaded invoices (same amount, within 10 days), a receipt to a
                # Wise payment (amount within $50 - bank charges - and 15 days).
                # Unmatched sales = invoice PDF missing on our side (shown red).
                from datetime import date as _date

                def _days(a, b):
                    try:
                        return abs((_date.fromisoformat(a[:10]) - _date.fromisoformat(b[:10])).days)
                    except ValueError:
                        return 999
                used_inv, used_pay, matched = set(), set(), []
                for e in ex.get("entries") or []:
                    e = dict(e)
                    if e.get("type", "").lower().startswith("opening"):
                        e["match"] = {"status": "opening"}
                    elif e.get("debit"):
                        cand = [i for i in all_invoices if i["id"] not in used_inv and i["currency"] == cur
                                and abs((i["total"] or 0) - e["debit"]) < 0.01 and _days(i["date"] or "", e["date"]) <= 10]
                        if cand:
                            used_inv.add(cand[0]["id"])
                            e["match"] = {"status": "invoice", "number": cand[0]["number"], "date": cand[0]["date"]}
                        else:
                            e["match"] = {"status": "missing_invoice"}
                    else:
                        cand = [p for p in all_payments if p["key"] not in used_pay and p["currency"] == cur
                                and -0.01 <= p["amount"] - e["credit"] <= 50 and _days(p["date"], e["date"]) <= 15]
                        if cand:
                            used_pay.add(cand[0]["key"])
                            e["match"] = {"status": "payment", "date": cand[0]["date"], "amount": cand[0]["amount"]}
                        else:
                            e["match"] = {"status": "missing_payment"}
                    matched.append(e)
                # One Wise payment can show up as several receipts on the same
                # day (e.g. Acacia booked $21,025 as $20,975 + $50).
                for day in {e["date"] for e in matched if e["match"]["status"] == "missing_payment"}:
                    group = [e for e in matched if e["date"] == day and e["match"]["status"] == "missing_payment"]
                    total = sum(e["credit"] for e in group)
                    cand = [p for p in all_payments if p["key"] not in used_pay and p["currency"] == cur
                            and -0.01 <= p["amount"] - total <= 50 and _days(p["date"], day) <= 15]
                    if cand:
                        used_pay.add(cand[0]["key"])
                        for e in group:
                            e["match"] = {"status": "payment", "date": cand[0]["date"], "amount": cand[0]["amount"],
                                          "combined": len(group) > 1}
                ex = {**ex, "entries": matched}
                supplier_ledger = {"docId": d["id"], "filename": d.get("filename"), "asOf": as_of, "from": ex.get("from"),
                                   "currency": cur, "closing": ex.get("closing"), "opening": ex.get("opening"),
                                   "entries": ex.get("entries") or [], "notes": ex.get("notes") or [],
                                   "ourBalanceAsOf": round(ours, 2),
                                   "difference": round(ours - (ex.get("closing") or 0), 2)}
            out.append({"id": sid, "name": cfg["name"], "groupKey": f"supplier|{sid}", "payments": payments[::-1],
                        "supplierLedger": supplier_ledger,
                        "startDate": start, "openingBalance": setting.get("opening_balance") or 0,
                        "openingCurrency": setting.get("currency") or "",
                        "invoices": invoices[::-1], "totals": totals, "years": years,
                        "unreadInvoices": sum(1 for i in invoices if not i["total"])})
        return json_response({"suppliers": out})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def SetWiseSupplierOpening(request):
    """{supplierId, startDate (YYYY-MM-DD or ""), openingBalance, currency} (+ key).
    The balance counts payments/invoices from startDate on, starting from
    openingBalance (positive = advance already paid, negative = owed)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    body = request.get_json(silent=True) or {}
    if not _key_ok(request, body):
        return json_response({"error": "Unauthorized"}, 401)
    sid = body.get("supplierId")
    if sid not in SUPPLIER_BALANCES:
        return json_response({"error": "Unknown supplier"}, 400)
    start = (body.get("startDate") or "").strip()
    if start and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start):
        return json_response({"error": "startDate must be YYYY-MM-DD"}, 400)
    try:
        token = pb_authenticate()
        record = {"supplier_id": sid, "start_date": start, "opening_balance": float(body.get("openingBalance") or 0),
                  "currency": (body.get("currency") or "USD").upper()}
        existing = pb_list_all(token, SUPPLIER_SETTINGS_COLLECTION, filter_str=f"supplier_id = {json.dumps(sid)}")
        op = ({"method": "PATCH", "url": f"/api/collections/{SUPPLIER_SETTINGS_COLLECTION}/records/{existing[0]['id']}", "body": record}
              if existing else {"method": "POST", "url": f"/api/collections/{SUPPLIER_SETTINGS_COLLECTION}/records", "body": record})
        pb_batch(token, [op])
        return json_response({"ok": True})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def backfill_invoice_extraction(token, only_missing_tax=True):
    """Re-reads stored invoices that predate a schema field (e.g. VAT)."""
    done = 0
    for d in pb_list_all(token, DOCUMENTS_COLLECTION):
        if d.get("kind") != "invoice" or (d.get("group_key") or "").startswith("amazon|"):
            continue
        ex = d.get("extracted") or {}
        if only_missing_tax and "tax_amount" in ex:
            continue
        try:
            with open(d["stored_path"], "rb") as fh:
                fields = extract_invoice_fields(d.get("filename"), fh.read())
        except Exception:
            continue
        if fields:
            pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{DOCUMENTS_COLLECTION}/records/{d['id']}",
                              "body": {"extracted": fields}}])
            done += 1
    return done


def match_income_group_invoice(token, fields, filename=""):
    """-> (group_key, "YYYY-MM") for a sales invoice issued to Amazon/Etsy, else None.
    The platform comes from the customer name, or - when the invoice is issued
    BY the user's own company - from the file name as well. The month is the
    one BEFORE the invoice date (the user always invoices the previous month)."""
    customer = fields.get("customer") or ""
    haystack = customer
    if OWN_COMPANY_RE.search(fields.get("vendor") or ""):
        haystack += " " + (filename or "")
    prefix = next((p for p, rx in GROUP_CUSTOMER_RE.items() if rx.search(haystack)), None)
    cur = (fields.get("currency") or "").upper()
    if not prefix or not cur:
        return None
    cls = next(c for c, p in GROUP_INVOICE_CLASSES.items() if p == prefix)
    rules = {r["rule_key"]: r["classification"] for r in pb_list_all(token, RULES_COLLECTION)}
    totals = {}
    for row in pb_list_all(token, LEDGER_COLLECTION, filter_str=f"currency = {json.dumps(cur)}"):
        if not row.get("occurred_at") or classify(row, rules)[0] != cls:
            continue
        month = f"{local_parts(row['occurred_at']):%Y-%m}"
        signed = (row.get("amount") or 0) * (1 if row.get("direction") == "in" else -1)
        totals[month] = totals.get(month, 0) + signed
    # Per the user (2026-10-01): invoices they issue are ALWAYS for the previous
    # month - an invoice dated in September covers August's payouts.
    try:
        inv_day = datetime.strptime(fields.get("invoice_date") or "", "%Y-%m-%d")
        return f"{prefix}|{cur}", (inv_day.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    except ValueError:
        pass
    # No readable date: fall back to the month whose payout total equals the invoice total.
    total = fields.get("total") or 0
    hits = [m for m, t in totals.items() if total and abs(t - total) < 0.015]
    return (f"{prefix}|{cur}", hits[0]) if len(hits) == 1 else None


def backfill_document_hashes(token):
    ops = []
    for d in pb_list_all(token, DOCUMENTS_COLLECTION):
        if d.get("sha256") or not os.path.exists(d.get("stored_path") or ""):
            continue
        with open(d["stored_path"], "rb") as fh:
            ops.append({"method": "PATCH", "url": f"/api/collections/{DOCUMENTS_COLLECTION}/records/{d['id']}",
                        "body": {"sha256": hashlib.sha256(fh.read()).hexdigest()}})
    if ops:
        pb_batch(token, ops)
    return len(ops)


# ---------------------------------------------------------------- PDF statements

PDF_STATEMENT_SCHEMA = {
    "type": "object",
    "properties": {
        "currency": {"type": "string", "description": "ISO code of the balance this statement is for"},
        "period_start": {"type": "string", "description": "YYYY-MM-DD"},
        "period_end": {"type": "string", "description": "YYYY-MM-DD"},
        "transactions": {"type": "array", "items": {
            "type": "object",
            "properties": {
                "id": {"type": "string", "description": "Wise transaction id exactly as printed, e.g. TRANSFER-2343421870, "
                                                       "CARD-4269200489, BALANCE-5989739510, BALANCE_CASHBACK-...; empty if not printed"},
                "date": {"type": "string", "description": "YYYY-MM-DD (add ' HH:MM' if a time is printed)"},
                "description": {"type": "string"},
                "amount": {"type": "number", "description": "Signed: negative = money out of this balance"},
                "fee": {"type": "number", "description": "Wise fee for this line, 0 if none"},
                "counterparty": {"type": "string", "description": "Payer (money in) / payee or card merchant (money out)"},
                "reference": {"type": "string"},
                "exchange_from": {"type": "string"},
                "exchange_to": {"type": "string"},
                "exchange_to_amount": {"type": "number", "description": "0 if not a conversion / foreign currency payment"},
                "kind": {"type": "string", "enum": ["DEPOSIT", "TRANSFER", "CARD", "CONVERSION", "CASHBACK", "OTHER"]},
            },
            "required": ["id", "date", "description", "amount", "fee", "counterparty", "reference",
                         "exchange_from", "exchange_to", "exchange_to_amount", "kind"],
            "additionalProperties": False,
        }},
    },
    "required": ["currency", "period_start", "period_end", "transactions"],
    "additionalProperties": False,
}


def parse_pdf_statement(filename, data):
    """Wise PDF statement (one currency balance) -> {currency, period_*, rows}
    where rows use the xlsx statement's column names, so the normal
    statement_row_to_ledger path (and its precedence rules) applies."""
    import anthropic

    client = anthropic.Anthropic()
    with client.beta.messages.stream(
        model=WISE_AI_MODEL,
        max_tokens=64000,
        thinking={"type": "adaptive"},
        output_config={"effort": "low", "format": {"type": "json_schema", "schema": PDF_STATEMENT_SCHEMA}},
        messages=[{"role": "user", "content": [
            {"type": "document", "source": {"type": "base64", "media_type": "application/pdf",
                                            "data": base64.standard_b64encode(data).decode()}},
            {"type": "text", "text": (
                f"File: {filename}\nThis is a Wise (wise.com) account statement. List EVERY transaction line, in order, "
                "with no omissions and no invented lines. Amounts are from the point of view of this balance "
                "(money out negative). Copy Wise transaction ids exactly when printed."
            )},
        ]}],
    ) as stream:
        response = stream.get_final_message()
    if response.stop_reason == "refusal":
        raise RuntimeError("The PDF could not be read")
    parsed = json.loads(next(b.text for b in response.content if b.type == "text"))
    cur = (parsed.get("currency") or "").upper()
    rows = []
    for t in parsed.get("transactions", []):
        kind = t.get("kind") or "OTHER"
        rows.append({
            "ID": t.get("id") or "", "Date Time": t.get("date"), "Amount": t.get("amount") or 0, "Currency": cur,
            "Description": t.get("description") or "", "Payment Reference": t.get("reference") or "",
            "Total Fees": t.get("fee") or 0,
            "Payer Name": t.get("counterparty") if (t.get("amount") or 0) > 0 else "",
            "Payee Name": t.get("counterparty") if (t.get("amount") or 0) < 0 and kind != "CARD" else "",
            "Merchant": t.get("counterparty") if kind == "CARD" else "",
            "Exchange From": t.get("exchange_from") or "", "Exchange To": t.get("exchange_to") or "",
            "Exchange To Amount": t.get("exchange_to_amount") or 0,
            "Transaction Type": "DEBIT" if (t.get("amount") or 0) < 0 else "CREDIT",
            "Transaction Details Type": "UNKNOWN" if kind in ("CASHBACK", "OTHER") else kind,
            "_pdf_no_id": not t.get("id"),
        })
        if kind == "CASHBACK" and not rows[-1]["ID"]:
            rows[-1]["ID"] = "BALANCE_CASHBACK-pdf"
    return {"currency": cur, "period_start": parsed.get("period_start"), "period_end": parsed.get("period_end"), "rows": rows}


def resolve_pdf_statement_keys(token, rows):
    """PDF lines without a printed Wise id get the key of the existing ledger
    row with the same currency/direction/amount within a day; otherwise a
    stable synthetic key so re-importing the same PDF doesn't duplicate."""
    if not any(r and r["wise_id"] == "" for r in rows):
        return rows
    existing = pb_list_all(token, LEDGER_COLLECTION)
    for r in rows:
        if not r or r["wise_id"]:
            continue
        when = _parse_dt(r["occurred_at"]) if r.get("occurred_at") else None
        match = None
        for e in existing:
            if (e.get("currency") == r["currency"] and e.get("direction") == r["direction"]
                    and abs((e.get("amount") or 0) - r["amount"]) < 0.005 and when and e.get("occurred_at")
                    and abs((_parse_dt(e["occurred_at"]) - when).days) <= 1):
                match = e
                break
        if match:
            r["ledger_key"], r["wise_id"] = match["ledger_key"], match.get("wise_id") or ""
        else:
            r["wise_id"] = f"PDF-{(r.get('occurred_at') or '')[:10]}-{r['amount']:.2f}"
            r["ledger_key"] = f"{r['wise_id']}|{r['currency']}|{r['direction']}"
    return rows


def group_month_totals(token, prefix, month):
    """{currency: (native_total, eur_total)} for one platform's payouts in one month."""
    cls = next(c for c, p in GROUP_INVOICE_CLASSES.items() if p == prefix)
    rules = {r["rule_key"]: r["classification"] for r in pb_list_all(token, RULES_COLLECTION)}
    out = {}
    for row in pb_list_all(token, LEDGER_COLLECTION, filter_str=f"occurred_at >= {json.dumps(month + '-01')}"):
        if not row.get("occurred_at") or f"{local_parts(row['occurred_at']):%Y-%m}" != month:
            continue
        if classify(row, rules)[0] != cls:
            continue
        sign = 1 if row.get("direction") == "in" else -1
        day = f"{local_parts(row['occurred_at']):%Y-%m-%d}"
        try:
            eur_rates_for({row["currency"]}, day, day)
        except requests.RequestException:
            pass
        eur = to_eur(row.get("amount") or 0, row["currency"], day)
        nat, e = out.get(row["currency"], (0.0, 0.0))
        out[row["currency"]] = (nat + sign * (row.get("amount") or 0), e + sign * (eur or 0))
    return out


def detect_extra_currencies(token, gkey, month, total):
    """If the invoice total doesn't equal its own currency's payouts but does
    equal them PLUS other currencies' payouts (converted at the ECB rate,
    within 2% of the converted part for FX), return those other group keys."""
    prefix, cur = gkey.split("|")
    totals = group_month_totals(token, prefix, month)
    own = totals.get(cur, (0.0, 0.0))[0]
    if not total or abs(own - total) < 0.015 or len(totals) < 2:
        return []
    # value of 1 EUR in the invoice currency, from this month's own rows (or 1 for EUR)
    rate = 1.0 if cur == "EUR" else (totals[cur][0] / totals[cur][1] if totals.get(cur) and totals[cur][1] else None)
    if not rate:
        return []
    others = [c for c in totals if c != cur]
    from itertools import combinations
    for n in range(1, len(others) + 1):
        for combo in combinations(others, n):
            converted = sum(totals[c][1] for c in combo) * rate
            if abs(own + converted - total) <= max(0.02, 0.02 * abs(converted)):
                return [f"{prefix}|{c}" for c in combo]
    return []


def SetWiseNoDocument(request):
    """{ledgerKey, noDocument: true|false} (+ key). Marks one transaction as
    not needing any document (per the user 2026-10-03, e.g. an Estonian VAT
    refund with no paper behind it). Stored as rule_key "noinv|<ledger_key>"
    in the classification-rules collection; classify() ignores that prefix."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    body = request.get_json(silent=True) or {}
    if not _key_ok(request, body):
        return json_response({"error": "Unauthorized"}, 401)
    try:
        token = pb_authenticate()
        key = f"noinv|{body.get('ledgerKey') or ''}"
        existing = pb_list_all(token, RULES_COLLECTION, filter_str=f"rule_key = {json.dumps(key)}")
        if body.get("noDocument", True):
            ops = [] if existing else [{"method": "POST", "url": f"/api/collections/{RULES_COLLECTION}/records",
                                        "body": {"rule_key": key, "classification": "no-document"}}]
        else:
            ops = [{"method": "DELETE", "url": f"/api/collections/{RULES_COLLECTION}/records/{r['id']}"} for r in existing]
        if ops:
            pb_batch(token, ops)
        return json_response({"ok": True})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def SetWiseDocumentCoverage(request):
    """{id, extraGroupKeys: ["amazon|GBP", ...]} (+ key) - mark that a monthly
    Amazon/Etsy invoice also covers other currencies of the same month."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    body = request.get_json(silent=True) or {}
    if not _key_ok(request, body):
        return json_response({"error": "Unauthorized"}, 401)
    try:
        token = pb_authenticate()
        found = pb_list_all(token, DOCUMENTS_COLLECTION, filter_str=f"id = {json.dumps(body.get('id') or '')}")
        if not found or not found[0].get("group_key"):
            return json_response({"error": "Not a monthly Amazon/Etsy invoice"}, 400)
        # {id, differenceAccepted: true|false}: the user accepts a small
        # amount difference between the invoice and the payouts (2026-10-03).
        if "differenceAccepted" in body:
            ex = dict(found[0].get("extracted") or {})
            ex["difference_accepted"] = bool(body["differenceAccepted"])
            pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{DOCUMENTS_COLLECTION}/records/{found[0]['id']}",
                              "body": {"extracted": ex}}])
            return json_response({"ok": True, "differenceAccepted": ex["difference_accepted"]})
        prefix = found[0]["group_key"].split("|")[0]
        extra = [k for k in (body.get("extraGroupKeys") or []) if k.startswith(prefix + "|") and k != found[0]["group_key"]]
        pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{DOCUMENTS_COLLECTION}/records/{found[0]['id']}",
                          "body": {"extra_group_keys": extra}}])
        return json_response({"ok": True, "extraGroupKeys": extra})
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)


def find_payment_combination(fields, scored, documented):
    """One invoice settled by 2-4 separate payments to the same payee (real
    case: Taxually Feb 2026 invoice 108.34 EUR = 58.34 + 25 + 25 card charges).
    Only payee-name matches without an invoice yet, same currency, dated
    -15..+45 days; amounts (or amounts net of Wise fee) must sum exactly."""
    from itertools import combinations
    total = fields.get("total") or 0
    currency = (fields.get("currency") or "").upper()
    pool = [r for score, _strong, r in scored
            if r.get("currency") == currency and r["ledger_key"] not in documented
            and _name_score(fields.get("vendor"), r.get("counterparty")) >= 0.5][:12]
    for n in (2, 3, 4):
        hits = []
        for combo in combinations(pool, n):
            gross = sum(r.get("amount") or 0 for r in combo)
            net = sum((r.get("amount") or 0) - (r.get("fee") or 0) for r in combo)
            if abs(gross - total) < 0.015 or abs(net - total) < 0.015:
                hits.append(combo)
        if len(hits) == 1:
            return sorted(hits[0], key=lambda r: r["occurred_at"])
        if hits:
            return None  # ambiguous - let the user pick
    return None
