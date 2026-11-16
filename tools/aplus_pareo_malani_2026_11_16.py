"""Translate the German mandala-pareo Premium page "2026 DE PAEO MALANI" into the
EXISTING pareo Premium pages of every marketplace/language (per the user
2026-11-16: "do not create, only change"; no block-print pareos - they have
their own A+; all PAREO_ACA/ZEN, MALANI, SOL pareos belong here). Where a
store/language has no pareo page yet, one is created. US keeps its own US video.
usage: python tools/aplus_pareo_malani_2026_11_16.py [MP ...] [--apply]"""
import copy
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import aplus_rollout_2026_11_16 as R
from sp_api.api import AplusContent, CatalogItems
from sp_api.base import Marketplaces
from WeeklyChecks import creds, listings
from AsinAnalytics import pb_authenticate, pb_list_all

SRC = "54343e5c-ffd4-42a9-893c-12079980c91a"
GROUPS = {"PAREO_MALANI", "PAREO_ACA", "PAREO_SOL"}
TARGETS = [  # (mp, locale, existing key or None)
    ("DE", "en-GB", "afda6b6f-28a9-4bd1-b756-2e47724dc10f"), ("DE", "pl-PL", "b2199060-25fe-4c29-b46d-999acdd8508b"),
    ("DE", "tr-TR", "7338dd3d-70b0-47a4-aa67-328edbe0086f"), ("DE", "nl-NL", "572987ca-9610-40d3-bd28-b4d9b1c5495a"),
    ("DE", "da-DK", "9e80be64-73c9-4a43-b9bb-fabd4bf11369"), ("DE", "cs-CZ", None),
    ("UK", "en-GB", None),
    ("FR", "fr-FR", "3b2a49dc-c46a-4ec8-997e-e17bb2c9a7a0"), ("IT", "it-IT", "7846dc21-2c29-46be-97c0-56d696c1e009"),
    ("ES", "es-ES", "e22835f9-a42e-4431-93ac-0ae2b83b45b8"), ("NL", "nl-NL", "1efada04-d194-4cf0-8d58-e1e90667389d"),
    ("NL", "en-GB", "45e859c0-8893-4930-95d1-abcef89f38d7"), ("SE", "sv-SE", "dfdb7eee-176f-48af-9ac0-799c17dda83a"),
    ("PL", "pl-PL", "e1e124a8-54ee-4f34-8567-1dd64b52a7d9"), ("BE", "fr-BE", "b06463de-9a79-483b-baf0-c9bb90775654"),
    ("BE", "nl-BE", None),
    ("US", "en-US", "c831e78b-f5cd-4ec6-8e62-839598d76d54"), ("US", "es-US", "9c96db31-a911-4593-a260-ea6d0d4c195e"),
]
APPLY = "--apply" in sys.argv
ONLY = [a for a in sys.argv[1:] if not a.startswith("--")]


def video_of(doc):
    for m in doc["contentModuleList"]:
        v = (m.get("premiumVideoText") or {}).get("video") or (m.get("premiumHeroVideo") or {}).get("heroVideo")
        if v and v.get("videoMediaId"):
            return v


def main():
    st = R.load_state()
    cde, _ = creds("DE")
    src = R.call(AplusContent(credentials=cde, marketplace=Marketplaces.DE, timeout=120).get_content_document, SRC,
                 marketplaceId=R.DE_MID, includedDataSet=["CONTENTS"]).payload["contentRecord"]["contentDocument"]
    rows = pb_list_all(pb_authenticate(), "asin_group_mapping", 'asin != ""', fields="asin,sku,group")
    by_sku = {r["sku"]: r["group"] for r in rows}
    by_asin = {r["asin"]: r["group"] for r in rows}
    cache = {}
    for mp, locale, key in TARGETS:
        if ONLY and mp not in ONLY:
            continue
        cred, _ = creds(mp)
        m = getattr(Marketplaces, mp)
        mid = m.marketplace_id
        ap = AplusContent(credentials=cred, marketplace=m, timeout=120)
        ci = CatalogItems(credentials=cred, marketplace=m, version="2022-04-01")
        if mp not in cache:
            live, fam = set(), []
            for sku, l in listings(mp, "summaries").items():
                a = l["asin"]
                if a and {"BUYABLE", "DISCOVERABLE"} & set(l["status"]):
                    live.add(a)
                    if (by_sku.get(sku) or by_asin.get(a)) in GROUPS and a not in fam:
                        fam.append(a)
            cache[mp] = (live, fam)
        live, fam = cache[mp]
        doc = copy.deepcopy(src)
        doc["locale"] = locale
        notes = []
        if key:
            old = R.call(ap.get_content_document, key, marketplaceId=mid, includedDataSet=["CONTENTS"]).payload["contentRecord"]["contentDocument"]
            doc["name"] = old["name"]
            if mp == "US":  # US keeps its own video (EU video ids do not exist there)
                uv = video_of(old)
                for mm in doc["contentModuleList"]:
                    if (mm.get("premiumVideoText") or {}).get("video"):
                        mm["premiumVideoText"]["video"] = copy.deepcopy(uv)
        else:
            doc["name"] = f"2026 {mp} PAREO MALANI {locale}"
            if mp == "US":
                raise SystemExit("no US page to take the video from")
        R.fix_links(doc, live, fam, ci, mid, notes, by_asin)
        if mp == "US":
            # images only (videos already replaced above)
            def remap(o):
                if isinstance(o, dict):
                    for k, v in list(o.items()):
                        if k == "uploadDestinationId" and isinstance(v, str) and not v.startswith("/images/") and not v.startswith("aplus-media/sc/") or (k == "uploadDestinationId" and isinstance(v, str) and v in st["media_us"]):
                            if v not in st["media_us"]:
                                _, _, data = R.jpeg_dims("https://m.media-amazon.com/images/S/" + v)
                                st["media_us"][v] = R.upload_image(cred, m, data); R.save_state(st)
                            o[k] = st["media_us"][v]
                        else:
                            remap(v)
                elif isinstance(o, list):
                    for v in o:
                        remap(v)
            remap([mm for mm in doc["contentModuleList"] if not mm.get("premiumVideoText")])
        items = []
        R.collect(doc["contentModuleList"], "contentModuleList", items)
        tkey = f"malani|{R.SHARE_TRANSLATION.get(locale, locale)}"
        srcs = [h[k] for _, h, k in items]
        if st["tr"].get(tkey, {}).get("src") == srcs:
            outs = st["tr"][tkey]["out"]
        else:
            outs = R.translate(srcs, R.SHARE_TRANSLATION.get(locale, locale), topic="beach")
            st["tr"][tkey] = {"src": srcs, "out": outs}; R.save_state(st)
        for (_, h, k), t in zip(items, outs):
            h[k] = t
        R.fill_crops_alts(doc["contentModuleList"])
        errs = []
        for rnd in range(3):
            try:
                R.call(ap.validate_content_document_asin_relations, body={"contentDocument": doc}, marketplaceId=mid, asinSet=fam)
                errs = []
            except Exception as e:
                errs = R.errors_of(e)
            too_long = {}
            for e in errs:
                m_ = re.search(r"contentDocument\.(contentModuleList\[.*?)\.value: length must be between \d+ and (\d+)", e)
                if m_:
                    too_long[m_.group(1) + ".value"] = int(m_.group(2))
                m4 = re.search(r"contentDocument\.(contentModuleList\[.*?\.textList): Total text length (\d+) exceeds maximum of (\d+)", e)
                if m4:
                    for pp, hh, kk in items:
                        if pp.startswith(m4.group(1) + "["):
                            too_long[pp] = max(5, int(len(hh[kk]) * int(m4.group(3)) / int(m4.group(2))) - 4)
            idx = {p: (h, k) for p, h, k in items}
            fix = [(p, n) for p, n in too_long.items() if p in idx]
            if not fix or rnd == 2:
                break
            short = R.translate([idx[p][0][idx[p][1]] for p, _ in fix], R.SHARE_TRANSLATION.get(locale, locale), shorten=[n - 3 for _, n in fix])
            for (p, n), t in zip(fix, short):
                h, k = idx[p]; h[k] = t[:n]
        res = "validated" if not errs else "ERRORS " + "; ".join(errs)[:300]
        if APPLY and not errs:
            try:
                if key:
                    R.call(ap.update_content_document, key, body={"contentDocument": doc}, marketplaceId=mid)
                else:
                    key = R.call(ap.create_content_document, body={"contentDocument": doc}, marketplaceId=mid).payload["contentReferenceKey"]
                aset, dropped = list(fam), []
                for attempt in range(3):
                    R.call(ap.post_content_document_asin_relations, key, body={"asinSet": aset}, marketplaceId=mid)
                    try:
                        R.call(ap.post_content_document_approval_submission, key, marketplaceId=mid)
                        break
                    except Exception as e:
                        bad = set(re.findall(r"B0[A-Z0-9]{8}", str(e))) & set(aset)
                        if not bad or attempt == 2:
                            raise
                        dropped += sorted(bad); aset = [a for a in aset if a not in bad]
                res = f"SUBMITTED {key}" + (f" (dropped {dropped})" if dropped else "")
            except Exception as e:
                res = "ERR " + "; ".join(R.errors_of(e))[:300]
        print(mp, locale, len(fam), "asins", res, notes[:4])
        st.setdefault("malani", {})[f"{mp}|{locale}"] = {"key": key, "result": res}
        R.save_state(st)
        time.sleep(0.5)


if __name__ == "__main__":
    main()
