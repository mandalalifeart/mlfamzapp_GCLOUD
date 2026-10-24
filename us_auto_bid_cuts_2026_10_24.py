"""USAC AUTO bid changes approved by the user 2026-10-24 ("2-4 do the change"):
undo our own 2026-10-03 raises on the Rug Round targets (ACOS got worse after
them) and cut the Velvet close-match bid 20% (142 clicks, 0 orders in October,
good history). WOOL close-match left alone - re-check 2026-10-31 (its October
spike was off-Amazon traffic, already stopped by MINIMIZE_SPEND on 2026-10-20).
Preview by default, --apply writes and logs every change."""
import json
import sys
from datetime import date

import requests

from AdsBidWriter import find_profile_connection, pb_authenticate, record_bid_change, refresh_access_token, update_sp_target_bid

PROFILE = "1649312585287580"
AUTO = "276405042438478"
TARGETS = [  # target id, text, expected live bid, new bid, reason
    ("192332385737078", "Rug Round AUTO close-match", 0.24, 0.20,
     "Undo our 2026-10-03 raise 0.20->0.24: CPC 26c->32c, spend ~2x, Oct ACOS 75% vs Sep 15% (user approved 2026-10-24)"),
    ("274361863028611", "Rug Round AUTO substitutes", 0.18, 0.15,
     "Undo our 2026-10-03 raise 0.15->0.18: 4 weeks 0 sales since (~$100) (user approved 2026-10-24)"),
    ("383653642441507", "Velvet close-match", 0.23, 0.18,
     "Bid -20%: Oct 142 clicks 0 orders, good history (Sep 22%) - temporary cut, re-check in 2 weeks (user approved 2026-10-24)"),
]
apply = "--apply" in sys.argv
if apply:
    sys.exit("Already applied on 2026-10-24 - preview only now.")
pb = pb_authenticate()
conn = find_profile_connection(pb, PROFILE)
tok = refresh_access_token(conn["profile_key"], conn["refresh_token"])
H = {"Authorization": f"Bearer {tok}", "Amazon-Advertising-API-ClientId": conn["client_id"], "Amazon-Advertising-API-Scope": PROFILE}
ct = "application/vnd.spTargetingClause.v3+json"
r = requests.post(conn["base_url"] + "/sp/targets/list", json={"targetIdFilter": {"include": [t[0] for t in TARGETS]}},
                  headers={**H, "Content-Type": ct, "Accept": ct}, timeout=60)
r.raise_for_status()
live = {str(t["targetId"]): t for t in r.json()["targetingClauses"]}
for tid, text, expected, new, reason in TARGETS:
    t = live.get(tid)
    if not t or str(t.get("state")).upper() != "ENABLED":
        print("SKIP", text, t and t.get("state"))
        continue
    cur = t.get("bid")
    print(f"{text}: live {cur} (expected {expected}) -> {new}")
    if not apply or cur == new:
        continue
    resp = update_sp_target_bid(conn["base_url"], tok, conn["client_id"], PROFILE, tid, new)
    record_bid_change(pb, {"action": "bid", "ad_product": "SPONSORED_PRODUCTS", "campaign_id": AUTO, "campaign_name": "USAC AUTO",
                           "changed_at": date.today().isoformat(), "country_code": "US", "profile_id": PROFILE,
                           "target_id": tid, "target_text": text, "old_bid": cur, "new_bid": new, "reason": reason,
                           "status": "applied", "amazon_response": json.dumps(resp)[:5000]})
    print("   applied", json.dumps(resp)[:200])
