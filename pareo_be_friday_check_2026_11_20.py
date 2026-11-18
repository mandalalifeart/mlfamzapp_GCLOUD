"""One-off, Friday 2026-11-20 morning (per the user 2026-11-18): the BE parent
PAREO_PARENT (ASIN B0CY2ST1TP) was deleted on 2026-11-18. Check whether its 28
former children are now standalone in the BE catalog; if ALL are, recreate the
pareo family (Colour only) with pareo_parent_per_store_2026_11_18.py MP=BE
(preview, then --apply if the preview has no errors). Result to Telegram via
notify("pareo-be-family"). Args: dry=1 -> check + preview only."""
import json
import subprocess
import sys
import time

from sp_api.api import CatalogItems
from sp_api.base import Marketplaces

from WeeklyChecks import creds

OLD = "B0CY2ST1TP"


def main():
    dry = "dry=1" in sys.argv
    c, _ = creds("DE")
    m = Marketplaces.BE
    mid = m.marketplace_id
    kids = json.load(open("logs/pareo_parent_BE_children_2026_11_18.json"))
    ci = CatalogItems(credentials=c, marketplace=m, version="2022-04-01")
    still = []
    for a in kids:
        try:
            rel = ci.get_catalog_item(a, marketplaceIds=[mid], includedData=["relationships"]).payload.get("relationships") or []
            parents = [p for r in rel for y in r.get("relationships", []) for p in (y.get("parentAsins") or [])]
        except Exception as e:
            parents = [f"ERR {str(e)[:60]}"]
        if parents:
            still.append(f"{a} -> {', '.join(parents)}")
        time.sleep(0.5)
    lines = [f"🧩 BE pareo family check (old parent {OLD} deleted 18 Nov)"]
    if still:
        lines.append(f"❌ {len(still)} of {len(kids)} BE pareos still have a parent in the catalog - not recreating the family yet:")
        lines += still
    else:
        lines.append(f"✅ All {len(kids)} former children are standalone now.")
        prev = subprocess.run([sys.executable, "pareo_parent_per_store_2026_11_18.py", "MP=BE"], capture_output=True, text=True, timeout=1200)
        out = prev.stdout + prev.stderr
        bad = "INVALID" in out or "Traceback" in out or '"ERROR"' in out or "missing/duplicate" in out
        lines.append("Preview:\n" + out[-2500:])
        if bad or dry:
            lines.append("⚠️ Not applied (preview had problems or dry run) - Claude to review." if not dry else "dry run - not applied")
        else:
            res = subprocess.run([sys.executable, "pareo_parent_per_store_2026_11_18.py", "MP=BE", "--apply"], capture_output=True, text=True, timeout=1200)
            lines.append("Applied - new parent PAREO_PARENT_BE (Colour only):\n" + (res.stdout + res.stderr)[-2500:])
    text = "\n".join(lines)
    print(text)
    if not dry:
        from NotificationRouting import notify
        notify("pareo-be-family", "AmzBot", text)


if __name__ == "__main__":
    main()
