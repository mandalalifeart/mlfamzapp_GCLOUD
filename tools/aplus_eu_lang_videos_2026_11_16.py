"""Switch every EU A+ page (rollout 2026-11-16) to the hero video in its own
language. The videos were uploaded by the user's Chrome session into the 24
"VIDEO HOLDER <Family> <LANG>" drafts; their heroVideo objects are in
logs/aplus_rollout_state.json "video_ids". Languages without their own video
(pl/tr/cs/da) get English (per the user); de-DE pages keep the German video.
Preview by default, --apply = update + submit. Optional filter: MP=DE etc."""
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sp_api.api import AplusContent
from sp_api.base import Marketplaces
from WeeklyChecks import creds

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APPLY = "--apply" in sys.argv
ONLY_MP = next((a.split("=")[1] for a in sys.argv if a.startswith("MP=")), None)
LANG = {"en-GB": "en", "fr-FR": "fr", "fr-BE": "fr", "it-IT": "it", "es-ES": "es", "nl-NL": "nl", "nl-BE": "nl",
        "sv-SE": "sv", "pl-PL": "en", "tr-TR": "en", "cs-CZ": "en", "da-DK": "en"}
FAM = {"vintage": "Vintage", "boho": "Boho", "velvet": "Velvet", "wool": "WOOL"}
st = json.load(open(os.path.join(HERE, "logs/aplus_rollout_state.json")))
vids = st["video_ids"]
apis = {}
log = []
for key, k in sorted(st["docs"].items()):
    mp, locale, fam = key.split("|")
    if fam not in FAM or mp == "US" or locale not in LANG or (ONLY_MP and mp != ONLY_MP):
        continue
    if mp not in apis:
        c, _ = creds(mp)
        m = getattr(Marketplaces, mp)
        apis[mp] = (AplusContent(credentials=c, marketplace=m, timeout=120), m.marketplace_id)
    ap, mid = apis[mp]
    hv = vids[f"{FAM[fam]}|{LANG[locale]}"]
    res = ""
    try:
        doc = ap.get_content_document(k, marketplaceId=mid, includedDataSet=["CONTENTS"]).payload["contentRecord"]["contentDocument"]
        hero = [m_["premiumHeroVideo"]["heroVideo"] for m_ in doc["contentModuleList"] if (m_.get("premiumHeroVideo") or {}).get("heroVideo")]
        if not hero:
            res = "no hero video module"
        elif hero[0]["videoMediaId"] == hv["videoMediaId"]:
            res = "already"
        else:
            hero[0].clear(); hero[0].update(json.loads(json.dumps(hv)))
            rel = [a["asin"] for a in ap.list_content_document_asin_relations(k, marketplaceId=mid).payload.get("asinMetadataSet", [])]
            try:
                ap.validate_content_document_asin_relations(body={"contentDocument": doc}, marketplaceId=mid, asinSet=rel)
                errs = []
            except Exception as e:
                errs = re.findall(r"'details': '([^']+)'", str(e)) or re.findall(r"'message': '([^']+)'", str(e)) or [str(e)[:300]]
            if errs:
                res = "ERRORS " + "; ".join(errs)[:300]
            elif APPLY:
                ap.update_content_document(k, body={"contentDocument": doc}, marketplaceId=mid)
                ap.post_content_document_approval_submission(k, marketplaceId=mid)
                res = f"switched to {LANG[locale]} + submitted"
            else:
                res = f"ok -> {LANG[locale]}"
    except Exception as e:
        res = "ERR " + str(e)[:200]
    print(mp, locale, fam, res)
    log.append({"key": key, "doc": k, "result": res})
    time.sleep(0.3)
if APPLY:
    json.dump(log, open(os.path.join(HERE, "logs/aplus_eu_lang_videos_2026_11_16.json"), "w"), indent=1)
