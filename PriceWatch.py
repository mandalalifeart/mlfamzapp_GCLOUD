"""Price watch (per the user 2026-11-05: "change everything and follow it, when BIL changes the
prices of the german listing to other mp"). job_state `price_watch` = list of
{mp, asin, sku, price, set_at, follow}: prices we set by API; `follow` = marketplaces whose price
BIL may change in response (a DE change → FR/IT/ES/NL/BE copy it). check() reads the live
offer prices (Listings API) and reports per watched item: still our price? overwritten? did the
BIL marketplaces follow? Run: python PriceWatch.py [check] - report only."""
import json, os, sys
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces
from AsinAnalytics import pb_authenticate, pb_list_all
from JobState import get_job_state, set_job_state

KEY = "price_watch"
MP = {"US": Marketplaces.US, "DE": Marketplaces.DE, "FR": Marketplaces.FR, "IT": Marketplaces.IT, "ES": Marketplaces.ES,
      "NL": Marketplaces.NL, "BE": Marketplaces.BE, "UK": Marketplaces.UK}
BIL_FOLLOW = ["FR", "IT", "ES", "NL", "BE"]


def _cred(mp):
    s = "USA" if mp == "US" else "EU"
    return dict(refresh_token=os.environ[f"REFRESH_TOKEN_{s}"], lwa_app_id=os.environ[f"CLIENT_ID_{s}"],
                lwa_client_secret=os.environ[f"CLIENT_SECRET_{s}"])


def _seller(mp):
    return os.environ["AMAZON_SELLER_ID"] if mp == "US" else os.environ["AMAZON_SELLER_ID_EU"]


def live_price(mp, sku):
    m = MP[mp]
    li = ListingsItems(credentials=_cred(mp), marketplace=m, version="2021-08-01", timeout=120)
    try:
        a = li.get_listings_item(_seller(mp), sku, marketplaceIds=m.marketplace_id, includedData="attributes").payload["attributes"]
    except Exception as exc:
        return None if "NOT_FOUND" in str(exc) else f"error {str(exc)[:80]}"
    for o in a.get("purchasable_offer") or []:
        if o.get("marketplace_id") == m.marketplace_id and o.get("audience", "ALL") == "ALL" and o.get("our_price"):
            return o["our_price"][0]["schedule"][0].get("value_with_tax")
    return None


def sku_for(pb, mp, asin, default):
    h = sorted(pb_list_all(pb, "price_history", f'marketplace = "{mp}" && asin = "{asin}"', fields="sku,since"),
               key=lambda r: r["since"])
    return h[-1]["sku"] if h else default


def check():
    pb = pb_authenticate()
    items = json.loads(get_job_state(pb, KEY) or "[]")
    lines = ["👀 Price watch (prices we set by API)"]
    for it in items:
        now = live_price(it["mp"], it["sku"])
        ok = isinstance(now, (int, float)) and abs(now - it["price"]) < 0.005
        line = f"{'✅' if ok else '🔴'} {it['mp']} {it['sku']}: set {it['price']} on {it['set_at']}, now {now}"
        fol = []
        for f in it.get("follow") or []:
            fp = live_price(f, sku_for(pb, f, it["asin"], it["sku"]))
            if fp is not None:
                fol.append(f"{f} {fp}{'✓' if isinstance(fp, (int, float)) and abs(fp - it['price']) < 0.005 else ''}")
        if fol:
            line += " · BIL: " + ", ".join(fol)
        lines.append(line)
    return "\n".join(lines)


if __name__ == "__main__":
    print(check())


def SendPriceWatch(request):
    """Daily Telegram (@baba_update_bot) with the price watch; dry=1 returns the text only."""
    from NotificationRouting import notify
    args = getattr(request, "args", {}) or {}
    try:
        text = check()
        if args.get("dry") != "1":
            notify("price-watch-daily", "amzbot", text)
        return json.dumps({"text": text}), 200, {"Content-Type": "application/json"}
    except Exception as exc:
        notify("price-watch-daily", "amzbot", f"🚨 Price watch failed: {exc}", is_error=True)
        return json.dumps({"error": str(exc)}), 500, {"Content-Type": "application/json"}
