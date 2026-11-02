"""Winter pause of loss-making pareo product ads (user OK 2026-11-02, "a, b"):
A: US product ads of PAREO_ACA_6C, Pareo20RayonGeom, PAREO_ACA_1A, PAREO_B_10,
   PAREO_B_544, PAREO_B_550; B: FR PAREO_ACA_6D, PAREO_ZEN_22, PAREO_B_13 and UK
PAREO_B_12. Every SP product ad of those ASINs in an ENABLED campaign of that
country that spent in October is paused until the pareo season review
(2027-02-20), tracked in job_state product_ad_pauses (unpause proposed then).
Preview by default, --apply to pause."""
import json
import sys
from datetime import date

from AsinAnalytics import pb_authenticate, pb_list_all
from ProductAdPauses import _dept, _load, _set_state
from JobState import get_job_state, set_job_state

TARGETS = {"US": ["PAREO_ACA_6C", "Pareo20RayonGeom", "PAREO_ACA_1A", "PAREO_B_10", "PAREO_B_544", "PAREO_B_550"],
           "FR": ["PAREO_ACA_6D", "PAREO_ZEN_22", "PAREO_B_13"],
           "UK": ["PAREO_B_12"]}
UNTIL = "2027-02-20"


def main(apply):
    pb = pb_authenticate()
    sku_asin = {r["sku"]: r["asin"] for r in pb_list_all(pb, "asin_group_mapping", 'asin != ""', fields="sku,asin")}
    camps, _dept_of = _dept(pb)
    by = _load(pb, "2026-10-01", "2026-11-01")
    items = []
    for cc, skus in TARGETS.items():
        asins = {sku_asin[s]: s for s in skus}
        for key, rows in by.items():
            r = rows[0]
            if r["country_code"].upper() != cc or r["asin"] not in asins:
                continue
            spend = sum(x.get("spend") or 0 for x in rows)
            camp = camps.get(r["campaign_id"]) or {}
            if spend <= 0 or camp.get("campaign_status") != "ENABLED":
                continue
            items.append({"key": key, "profile_id": r["profile_id"], "campaign_id": r["campaign_id"],
                          "campaign": camp.get("campaign_name") or r["campaign_name"], "ad_group_id": r["ad_group_id"],
                          "asin": r["asin"], "sku": asins[r["asin"]], "country": cc, "history": "pareo-winter",
                          "spend": round(spend, 2), "orders": sum(x.get("orders") or 0 for x in rows)})
    for x in items:
        print(x["country"], x["campaign"], x["sku"], "Oct spend", x["spend"], "orders", x["orders"])
    if not apply:
        return
    pauses = json.loads(get_job_state(pb, "product_ad_pauses") or "[]")
    today = date.today().isoformat()
    for x in items:
        try:
            ad_id = _set_state(pb, x, "PAUSED", f"Pareo winter pause until {UNTIL}: negative profit Oct 2026 (user OK 2026-11-02)")
        except Exception as exc:
            print("FAILED", x["campaign"], x["sku"], exc)
            continue
        print("paused", x["country"], x["campaign"], x["sku"], ad_id)
        pauses = [p for p in pauses if p["key"] != x["key"]]
        pauses.append({**{k: x[k] for k in ("key", "profile_id", "campaign_id", "campaign", "ad_group_id", "asin",
                                            "sku", "country", "history")},
                       "ad_id": ad_id, "paused_on": today, "unpause_on": UNTIL, "status": "active"})
    set_job_state(pb, "product_ad_pauses", json.dumps(pauses))


if __name__ == "__main__":
    main("--apply" in sys.argv)
