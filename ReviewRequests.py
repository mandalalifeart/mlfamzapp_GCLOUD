"""Automatic "Request a Review" (per the user 2026-11-03: "can you automatically
request a review?" -> "yes").

SP-API Solicitations API = the "Request a Review" button in Seller Central:
Amazon sends its own standard email (product review + seller feedback), once
per order, only 5-30 days after delivery. We can't change the text.

Daily (`RunReviewRequests`): every shipped Amazon order (not MCF/Etsy, not
cancelled, not returned) bought 10-35 days ago that we haven't asked yet:
  get actions -> if Amazon offers productReviewAndSellerFeedback -> send it.
  Not offered yet -> "pending" (retried next day); once the order is older
  than 35 days -> "not_eligible". One row per order in PocketBase
  `review_requests` (order_id unique), written right after each order, so a
  killed run just continues next time.
Weekly (`SendReviewRequestsWeekly`, Monday digest): requests sent last week
per marketplace."""
import json
import os
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

from AsinAnalytics import pb_authenticate, pb_list_all
from UpdateSkuSalesMonth import pb_batch

COLLECTION = "review_requests"
MIN_DAYS, MAX_DAYS = 10, 35
ACTION = "productReviewAndSellerFeedback"
# order_items.marketplace -> (credential set, Marketplaces enum name, marketplace id)
MARKETPLACES = {
    "usa": ("USA", "US", "ATVPDKIKX0DER"), "ca": ("USA", "CA", "A2EUQ1WTGCTBG2"),
    "mex": ("USA", "MX", "A1AM78C64UM0Y8"),
    "uk": ("EU", "UK", "A1F83G8C2ARO7P"), "de": ("EU", "DE", "A1PA6795UKMFR9"),
    "fr": ("EU", "FR", "A13V1IB3VIYZZH"), "it": ("EU", "IT", "APJ6JRA9NG5V4"),
    "es": ("EU", "ES", "A1RKKUPIHCS9HS"), "nl": ("EU", "NL", "A1805IZSGTT6HS"),
    "se": ("EU", "SE", "A2NODRKZP88ZB9"), "pl": ("EU", "PL", "A1C3SOZRARQ6R3"),
    "be": ("EU", "BE", "AMEN7PMS3EDWL"), "ie": ("EU", "IE", "A28R8C7NBKEWEA"),
    "ae": ("EU", "AE", "A2VIGQ35RCS4UG"), "sa": ("EU", "SA", "A17E79C6D8DWNP"),
    "tr": ("EU", "TR", "A33AVAJ2PDY3EV"),
    "au": ("AU", "AU", "A39IBJ37TRP1C6"), "jp": ("JP", "JP", "A1VC38T7YXB528"),
}


def _cors():
    return {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}


def _creds(scope):
    if scope == "JP":
        return dict(refresh_token=os.environ["REFRESH_TOKEN_JP"], lwa_app_id=os.environ["CLIENT_ID_USA"],
                    lwa_client_secret=os.environ["CLIENT_SECRET_USA"])
    return dict(refresh_token=os.environ[f"REFRESH_TOKEN_{scope}"], lwa_app_id=os.environ[f"CLIENT_ID_{scope}"],
                lwa_client_secret=os.environ[f"CLIENT_SECRET_{scope}"])


def _client(scope, mp_enum, cache={}):
    from sp_api.api import Solicitations
    from sp_api.base import Marketplaces
    key = (scope, mp_enum)
    if key not in cache:
        cache[key] = Solicitations(credentials=_creds(scope), marketplace=getattr(Marketplaces, mp_enum))
    return cache[key]


def _paced(fn, *a, **kw):
    """Solicitations API: 1 request/second. Retry throttling / network errors."""
    for attempt in range(5):
        time.sleep(1.1)
        try:
            return fn(*a, **kw)
        except Exception as exc:
            msg = str(exc)
            if attempt < 4 and any(s in msg for s in ("QuotaExceeded", "Throttl", "429", "timed out", "Timeout", "Connection")):
                time.sleep(5 * (attempt + 1))
                continue
            raise


def candidates(token, today):
    start = (today - timedelta(days=MAX_DAYS)).isoformat()
    end = (today - timedelta(days=MIN_DAYS)).isoformat()
    lines = pb_list_all(token, "order_items",
                        f'purchase_date >= "{start}" && purchase_date <= "{end}" && order_status = "Shipped"',
                        fields="order_id,marketplace,purchase_date")
    orders = {}
    for ln in lines:
        if ln["marketplace"] in MARKETPLACES and ln.get("order_id"):
            orders.setdefault(ln["order_id"], ln)
    returned = {r["order_id"] for r in pb_list_all(token, "amazon_returns", f'return_date >= "{start}"', fields="order_id")}
    done = {r["order_id"]: r for r in pb_list_all(token, COLLECTION, f'purchase_date >= "{start}"')}
    return orders, returned, done


def _save(token, rec_id, body):
    if rec_id:
        pb_batch(token, [{"method": "PATCH", "url": f"/api/collections/{COLLECTION}/records/{rec_id}", "body": body}])
    else:
        pb_batch(token, [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": body}])


def run(dry=False, limit=None, today=None):
    today = today or date.today()
    token = pb_authenticate()
    orders, returned, done = candidates(token, today)
    stats, errors = Counter(), []
    todo = [o for o in orders.values() if done.get(o["order_id"], {}).get("status") in (None, "", "pending", "error")]
    todo.sort(key=lambda o: o["purchase_date"])
    for n, o in enumerate(todo):
        if limit and n >= int(limit):
            break
        oid, mp = o["order_id"], o["marketplace"]
        old = done.get(oid) or {}
        body = {"order_id": oid, "marketplace": mp, "purchase_date": o["purchase_date"],
                "attempts": int(old.get("attempts") or 0) + 1, "last_try": datetime.now().isoformat(timespec="seconds")}
        if oid in returned:
            body.update(status="skipped", detail="returned")
            stats["skipped (returned)"] += 1
            if not dry:
                _save(token, old.get("id"), body)
            continue
        scope, mp_enum, mid = MARKETPLACES[mp]
        try:
            cli = _client(scope, mp_enum)
            res = _paced(cli.get_solicitation_actions_for_order, oid, marketplaceIds=[mid])
            names = [a.get("name") for a in (res.payload or {}).get("_links", {}).get("actions", [])]
            if ACTION in names:
                if dry:
                    stats[f"would send {mp}"] += 1
                    continue
                _paced(cli.create_product_review_and_seller_feedback_solicitation, oid, marketplaceIds=[mid])
                body.update(status="sent", sent_at=today.isoformat(), detail="")
                stats[f"sent {mp}"] += 1
            else:
                age = (today - date.fromisoformat(o["purchase_date"])).days
                body.update(status="not_eligible" if age >= MAX_DAYS - 1 else "pending",
                            detail="Amazon offers no review request" + (f" ({', '.join(names)})" if names else ""))
                stats[body["status"]] += 1
        except Exception as exc:
            msg = str(exc)[:500]
            # Amazon answers 403/400 for orders it no longer allows (e.g. refunded / too late).
            final = any(s in msg for s in ("not eligible", "Not eligible", "already", "Unauthorized", "InvalidInput"))
            body.update(status="not_eligible" if final else "error", detail=msg)
            stats[body["status"]] += 1
            if not final:
                errors.append(f"{oid} {mp}: {msg[:150]}")
        if not dry:
            _save(token, old.get("id"), body)
    return {"candidates": len(orders), "checked": min(len(todo), int(limit)) if limit else len(todo),
            "stats": dict(stats), "errors": errors[:20]}


def RunReviewRequests(request):
    args = getattr(request, "args", {}) or {}
    out = run(dry=args.get("dry") == "1", limit=args.get("limit"),
              today=date.fromisoformat(args["today"]) if args.get("today") else None)
    return json.dumps(out), 200, _cors()


def weekly_text(today=None):
    today = today or date.today()
    end = today - timedelta(days=today.weekday() + 1)        # last Sunday
    start = end - timedelta(days=6)
    token = pb_authenticate()
    rows = pb_list_all(token, COLLECTION, f'last_try >= "{start.isoformat()}"')
    week = [r for r in rows if start.isoformat() <= (r.get("last_try") or "")[:10] <= end.isoformat()]
    sent = Counter(r["marketplace"] for r in week if r.get("status") == "sent" and start.isoformat() <= (r.get("sent_at") or "") <= end.isoformat())
    other = Counter(r["status"] for r in week if r.get("status") != "sent")
    total_sent = pb_list_all(token, COLLECTION, 'status = "sent"', fields="id")
    lines = [f"⭐ Review requests {start:%d %b} – {end:%d %b}: {sum(sent.values())} sent"]
    for mp, n in sent.most_common():
        lines.append(f"  {mp.upper()}: {n}")
    if other:
        lines.append("Not sent: " + ", ".join(f"{k} {v}" for k, v in other.most_common()))
    if other.get("error"):
        lines.append(f"⚠️ {other['error']} orders failed with an error (retried daily)")
    lines.append(f"Since start: {len(total_sent)} requests sent")
    return "\n".join(lines)


def SendReviewRequestsWeekly(request):
    from NotificationRouting import notify
    args = getattr(request, "args", {}) or {}
    text = weekly_text()
    if args.get("dry") != "1":
        notify("review-requests-weekly", "AmzBot", text)
    return json.dumps({"text": text}), 200, _cors()
