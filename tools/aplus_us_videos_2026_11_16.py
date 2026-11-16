"""After the user's Chrome session swapped the hero video in the 4 US en-US drafts
(2026-11-16), copy each en-US hero video (video + cover image + crop) into the
matching es-US draft, then submit all 8. Preview by default, --apply to write."""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sp_api.api import AplusContent
from sp_api.base import Marketplaces
from WeeklyChecks import creds

APPLY = "--apply" in sys.argv
MID = "ATVPDKIKX0DER"
st = json.load(open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs/aplus_rollout_state.json")))
c, _ = creds("US")
ap = AplusContent(credentials=c, marketplace=Marketplaces.US, timeout=120)


def get(k):
    return ap.get_content_document(k, marketplaceId=MID, includedDataSet=["CONTENTS"]).payload["contentRecord"]["contentDocument"]


def hero(doc):
    for m in doc["contentModuleList"]:
        if (m.get("premiumHeroVideo") or {}).get("heroVideo"):
            return m["premiumHeroVideo"]["heroVideo"]


for fam in ("vintage", "boho", "velvet", "wool"):
    ken, kes = st["docs"][f"US|en-US|{fam}"], st["docs"][f"US|es-US|{fam}"]
    en, es = get(ken), get(kes)
    h_en, h_es = hero(en), hero(es)
    print(fam, "en video", h_en["videoMediaId"], "| es video", h_es["videoMediaId"])
    h_es.clear(); h_es.update(json.loads(json.dumps(h_en)))
    for key, doc in ((ken, en), (kes, es)):
        rel = [a["asin"] for a in ap.list_content_document_asin_relations(key, marketplaceId=MID).payload.get("asinMetadataSet", [])]
        try:
            ap.validate_content_document_asin_relations(body={"contentDocument": doc}, marketplaceId=MID, asinSet=rel)
            errs = []
        except Exception as e:
            errs = re.findall(r"'details': '([^']+)'", str(e)) or re.findall(r"'message': '([^']+)'", str(e)) or [str(e)[:500]]
        print("  ", doc["locale"], len(rel), "asins, errors", errs[:3])
        if APPLY and not errs:
            if key == kes:
                ap.update_content_document(key, body={"contentDocument": doc}, marketplaceId=MID)
            print("    submit", ap.post_content_document_approval_submission(key, marketplaceId=MID).payload)
