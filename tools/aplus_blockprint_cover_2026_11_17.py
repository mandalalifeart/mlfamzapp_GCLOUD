"""Copy the block-print video module's video+cover (set by the user's Chrome session
2026-11-17 on DE de-DE b5886bf4 and US en-US 1b3be16a) into the other block-print
pages (15 EU locales from DE, es-US from en-US). Preview by default, --apply = update + submit."""
import json, os, re, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sp_api.api import AplusContent
from sp_api.base import Marketplaces
from WeeklyChecks import creds
APPLY = "--apply" in sys.argv
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
st = json.load(open(os.path.join(HERE, "logs/aplus_rollout_state.json")))
SRC = {"EU": ("DE", "b5886bf4-edbd-45d3-8e65-b78d327805a1"), "US": ("US", "1b3be16a-404f-4753-902a-ad26a3c478d5")}
apis = {}
def api(mp):
    if mp not in apis:
        c, _ = creds(mp); m = getattr(Marketplaces, mp)
        apis[mp] = (AplusContent(credentials=c, marketplace=m, timeout=120), m.marketplace_id)
    return apis[mp]
def get(mp, k):
    ap, mid = api(mp)
    return ap.get_content_document(k, marketplaceId=mid, includedDataSet=["CONTENTS"]).payload["contentRecord"]["contentDocument"]
def vids(doc):
    return [m["premiumVideoText"]["video"] for m in doc["contentModuleList"] if (m.get("premiumVideoText") or {}).get("video")]
src = {r: vids(get(mp, k))[0] for r, (mp, k) in SRC.items()}
print("source videos:", json.dumps(src))
for key, k in sorted(st["docs"].items()):
    mp, locale, fam = key.split("|")
    if fam != "blockprint" or k in [v[1] for v in SRC.values()]:
        continue
    ap, mid = api(mp)
    sv = src["US" if mp == "US" else "EU"]
    try:
        doc = get(mp, k); vs = vids(doc)
        if not vs:
            print(key, "no video module"); continue
        if vs[0].get("imageMediaId") == sv["imageMediaId"] and vs[0]["videoMediaId"] == sv["videoMediaId"]:
            print(key, "already"); continue
        old = dict(vs[0]); vs[0].clear(); vs[0].update(json.loads(json.dumps(sv)))
        def trim(o):  # some saved alt texts are > 100 chars (Amazon max)
            if isinstance(o, dict):
                for kk, vv in o.items():
                    if kk == "altText" and isinstance(vv, str) and len(vv) > 100:
                        o[kk] = vv[:100].rsplit(" ", 1)[0]
                    else:
                        trim(vv)
            elif isinstance(o, list):
                for vv in o: trim(vv)
        trim(doc)
        rel = [a["asin"] for a in ap.list_content_document_asin_relations(k, marketplaceId=mid).payload.get("asinMetadataSet", [])]
        try:
            ap.validate_content_document_asin_relations(body={"contentDocument": doc}, marketplaceId=mid, asinSet=rel); errs = []
        except Exception as e:
            errs = re.findall(r"'details': '([^']+)'", str(e)) or [str(e)[:300]]
        res = ("ERRORS " + "; ".join(errs)[:300]) if errs else "ok"
        if APPLY and not errs:
            ap.update_content_document(k, body={"contentDocument": doc}, marketplaceId=mid)
            ap.post_content_document_approval_submission(k, marketplaceId=mid)
            res = "updated + submitted"
        print(key, len(rel), "asins", old.get("imageMediaId"), "->", sv["imageMediaId"], res)
    except Exception as e:
        print(key, "ERR", str(e)[:300])
    time.sleep(0.3)
