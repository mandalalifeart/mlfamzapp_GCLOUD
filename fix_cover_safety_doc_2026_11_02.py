"""2026-11-02, per the user ("upload it to all our CoverPillowV items"): Amazon
rejected our multilingual "Warning and Care Instructions" PDF because it was
filed as a Safety Data Sheet (a chemical SDS). Re-file it as "Safety
Information" on CoverPoufV251 + CoverPillowV251A and add it the same way to
every CoverPillowV listing (US). Preview by default, --apply."""
import json
import sys

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

import WeeklyChecks as W

DOC = "https://mlfamzappfire.web.app/Covers_Warning_and_Care_Instructions_Multilingual.pdf"
MP = Marketplaces.US
cred, seller = W.creds("US")
li = ListingsItems(credentials=cred, marketplace=MP, version="2021-08-01", timeout=120)
lst = W.listings("US", "summaries,attributes")
skus = sorted(s for s in lst if s.startswith("CoverPillowV") or s == "CoverPoufV251")
for sku in skus:
    it = li.get_listings_item(seller, sku, marketplaceIds=MP.marketplace_id, includedData="attributes,summaries").payload
    ptype = it["summaries"][0]["productType"]
    cur = (it.get("attributes") or {}).get("compliance_media") or []
    new = [x for x in cur if x.get("source_location") != DOC]
    new.append({"source_location": DOC, "content_type": "safety_information", "content_language": "en_US",
                "marketplace_id": MP.marketplace_id})
    print(sku, ptype, "old:", [(x.get("content_type"), x.get("content_language")) for x in cur], "-> safety_information en_US")
    mode = None if "--apply" in sys.argv else "VALIDATION_PREVIEW"
    kw = {"marketplaceIds": MP.marketplace_id, "body": {"productType": ptype, "patches": [
        {"op": "replace", "path": "/attributes/compliance_media", "value": new}]}}
    if mode:
        kw["mode"] = mode
    r = li.patch_listings_item(seller, sku, **kw).payload
    print("   ", r.get("status"), json.dumps([i.get("message") for i in r.get("issues") or []])[:300])
