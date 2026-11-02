"""Combined Monday digest (2026-10-25, per the user: one Monday message
instead of ~10 separate ones).

The Monday cron lines run with TELEGRAM_CAPTURE=monday, so run_local.py
writes each job's Telegram text to logs/digest/monday/<date>/<Function>.txt
instead of sending it. This job (cron 10:50 Mondays) sends ONE message with a
headline per check (title + count of red/orange/warning lines) to
@baba_update_bot, plus the full texts as one attached .txt file.
Args: date=YYYY-MM-DD, dry=1.
"""
import io
import json
import os
from datetime import date

import requests

from NotificationRouting import notify

JOB_NAME = "monday-digest"
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "digest", "monday")
TOKEN = os.environ.get("UPDATE_TELEGRAM_BOT_TOKEN") or os.environ.get("MCF_TELEGRAM_BOT_TOKEN", "")
CHAT = os.environ.get("UPDATE_TELEGRAM_CHAT_ID") or os.environ.get("MCF_TELEGRAM_CHAT_ID", "")

# Display order + label per job (anything else is listed after these).
LABELS = [
    ("SendAccountHealthWeekly", "🩺 Account health"),
    ("SendListingHealthWeekly", "🛒 Listing health / Buy Box"),
    ("AuditAmazonListings", "📝 Listing errors"),
    ("SendCouponHealthWeekly", "🏷 Coupons & promotions"),
    ("SendInventoryWarning", "📦 Low FBA stock"),
    ("SendFbaShipmentsWeekly", "🚚 FBA shipments"),
    ("UpdateFbaFees", "📦 FBA fees"),
    ("RunWeeklyUsaInventorySync", "🔄 USA inventory sync"),
    ("RunWeeklyEuInventorySync", "🔄 EU inventory sync"),
    ("SendWeeklyChangesReview", "🔎 Ads changes review"),
    ("SendWeeklyBidChangeSummary", "📈 Bid change results"),
    ("SendSearchTermHarvest", "🌾 Search-term harvest"),
    ("SendOffAmazonWeekly", "🌐 Off-Amazon ads"),
    ("SendOrganicRankWeekly", "🔍 Organic search share"),
    ("SendCampaignWatchDigest", "🍄 Campaign watch"),
    ("SendWeeklyReturnRateDigest", "↩️ Returns"),
    ("SendArcherWeekly", "🤝 Archer affiliates"),
]
MARKERS = ("🔴", "🟠", "⚠️", "🚨", "🚩", "❌")


def _json(body, status=200):
    return json.dumps(body), status, {"Access-Control-Allow-Origin": ALLOWED_ORIGIN, "Content-Type": "application/json"}


def headline(text):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    title = lines[0] if lines else "(empty)"
    counts = {m: sum(1 for ln in lines if ln.startswith(m)) for m in MARKERS}
    flags = " ".join(f"{m}{n}" for m, n in counts.items() if n)
    return title[:90], flags


def build(day):
    folder = os.path.join(BASE, day)
    files = {f[:-4]: open(os.path.join(folder, f), encoding="utf-8").read()
             for f in sorted(os.listdir(folder)) if f.endswith(".txt")} if os.path.isdir(folder) else {}
    order = [name for name, _ in LABELS if name in files] + sorted(n for n in files if n not in dict(LABELS))
    labels = dict(LABELS)
    head = [f"🗓 Monday checks – {day}", f"{len(order)} checks ran. Full texts: https://mlfamzappfire.web.app/checks?kind=weekly&period={day}", ""]
    full = []
    for name in order:
        text = files[name].strip()
        title, flags = headline(text)
        head.append(f"{labels.get(name, name)}: {flags or '✅'}\n   {title}")
        full.append(f"{'=' * 60}\n{labels.get(name, name)}\n{'=' * 60}\n{text}\n")
    missing = [labels[n] for n, _ in LABELS if n not in files]
    if missing:
        head.append("\nNo message this week from: " + ", ".join(missing))
    # Ads review check lists (2026-11-02, per the user): weekly every Monday,
    # monthly on the first Monday of the month.
    head.append("\n✅ Weekly ads review checklist: https://mlfamzappfire.web.app/ads-reviews#weekly")
    if int(day[8:10]) <= 7:
        head.append("✅ Monthly ads review checklist: https://mlfamzappfire.web.app/ads-reviews#monthly")
    return "\n".join(head), "\n".join(full), len(order)


def store_weekly(day):
    """Each captured check -> check_reports (kind weekly) for the /checks page."""
    from CheckReports import save_report
    folder = os.path.join(BASE, day)
    labels = dict(LABELS)
    for f in sorted(os.listdir(folder)) if os.path.isdir(folder) else []:
        if f.endswith(".txt"):
            name = f[:-4]
            save_report("weekly", day, name, labels.get(name, name), open(os.path.join(folder, f), encoding="utf-8").read())


def SendMondayDigest(request):
    try:
        args = getattr(request, "args", {}) or {}
        day = args.get("date") or date.today().isoformat()
        head, full, n = build(day)
        if args.get("dry") != "1":
            store_weekly(day)
        if args.get("dry") == "1":
            return _json({"head": head, "full": full})
        if n == 0:
            notify(JOB_NAME, "amzbot", f"🗓 Monday checks {day}: nothing was captured - check the Monday cron jobs.", is_error=True)
            return _json({"sent": False, "reason": "nothing captured"})
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      json={"chat_id": CHAT, "text": head[:4000]}, timeout=30).raise_for_status()
        # The full texts are on the /checks page now (per the user 2026-11-02: the .txt was unreadable).
        notify(JOB_NAME, "amzbot", head, telegram=False)
        return _json({"sent": True, "checks": n})
    except Exception as exc:
        notify(JOB_NAME, "amzbot", f"🚨 Monday digest failed: {exc}", is_error=True)
        return _json({"error": str(exc)}, 500)
