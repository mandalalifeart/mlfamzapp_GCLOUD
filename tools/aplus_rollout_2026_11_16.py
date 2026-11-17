"""Roll the German A+ pages (2026 DE Vintage/Boho/Velvet/WOOL + HOME DECOR BRAND STORY)
out to every language / marketplace (per the user 2026-11-16: "translate to all languages and submit,
then copy/update in all EU and USA marketplaces, same images, videos").

usage: python tools/aplus_rollout_2026_11_16.py MP LOCALE [--docs=vintage,boho,velvet,wool,story] [--apply]
  default = build + translate + validate only; --apply = create/update, attach ASINs, submit.
State (created doc keys, translations, US media map) in logs/aplus_rollout_state.json.
"""
import copy
import io
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

import anthropic
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sp_api.api import AplusContent, CatalogItems, Upload
from sp_api.base import Marketplaces
from WeeklyChecks import creds, listings
from AsinAnalytics import pb_authenticate, pb_list_all

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(HERE, "logs/aplus_rollout_state.json")
DE_MID = "A1PA6795UKMFR9"
SOURCE = {  # German master docs in the DE marketplace
    "vintage": "aa852a80-ab83-4b74-b7ad-f958b13927a7",
    "boho": "808773cf-d98d-4dd7-8070-06020130bc1f",
    "velvet": "f6b2f8f5-4412-4a3e-b016-e6eb7ab99306",
    "wool": "685c519a-ec58-400d-99a2-2ec89cdb49d1",
    "story": "e8c3841d-b2e8-47a7-b9df-a67d5eaf3921",
    "beach": "dac32247-52aa-4d72-9783-500da941f735",  # BEACH brand story (pareos), user 2026-11-16
    "blockprint": "b5886bf4-edbd-45d3-8e65-b78d327805a1",  # 2026 DE PAREO Block Print (Premium), user 2026-11-16
}
NAMES = {"vintage": "Vintage", "boho": "Boho", "velvet": "Velvet", "wool": "WOOL", "story": "HOME DECOR BRAND STORY", "beach": "BEACH BRAND STORY", "blockprint": "PAREO Block Print"}
WOOL_ASIN = "B08FH9ZH5Y"
LANG = {"en-GB": "British English", "en-US": "American English", "nl-NL": "Dutch", "nl-BE": "Dutch (Belgium)",
        "pl-PL": "Polish", "tr-TR": "Turkish", "cs-CZ": "Czech", "da-DK": "Danish", "fr-FR": "French",
        "fr-BE": "French (Belgium)", "it-IT": "Italian", "es-ES": "Spanish (Spain)", "es-US": "US Spanish",
        "sv-SE": "Swedish", "de-DE": "German"}
SHARE_TRANSLATION = {"nl-BE": "nl-NL", "fr-BE": "fr-FR"}
US_VIDEO = {"vintage": "b2102229-4816-4bfc-84eb-5fd0cd43d7f7", "boho": "20124737-5ae2-43a5-a934-4a8e53703d74",
            "wool": "db2af94b-f16f-414e-8bd8-7128df345fc1",
            "velvet": "b2102229-4816-4bfc-84eb-5fd0cd43d7f7"}  # placeholder until the user's English velvet video is picked in the UI
NO_TRANSLATE_KEYS = {"productAsin", "asin", "comparisonType", "positionType", "brandStoreId"}


def load_state():
    try:
        return json.load(open(STATE_FILE))
    except Exception:
        return {"docs": {}, "tr": {}, "media_us": {}}


def save_state(st):
    json.dump(st, open(STATE_FILE, "w"), indent=1, ensure_ascii=False)


def call(f, *a, **k):
    for i in range(5):
        try:
            return f(*a, **k)
        except Exception as e:
            s = str(e)
            if i < 4 and ("Quota" in s or "Throttl" in s or "Timeout" in type(e).__name__ or "timed out" in s):
                time.sleep(3 * (i + 1)); continue
            raise


def errors_of(exc):
    s = str(exc)
    return re.findall(r"'details': '([^']+)'", s) or re.findall(r"'message': '([^']+)'", s) or [s[:800]]


# ---------- text extraction ----------
def collect(o, path, out):
    """out: list of (path, holder_dict, key) for translatable strings."""
    if isinstance(o, dict):
        for k, v in o.items():
            p = f"{path}.{k}" if path else k
            if k in NO_TRANSLATE_KEYS:
                continue
            if k == "value" and isinstance(v, str):
                if re.search(r"[A-Za-zÄÖÜäöüß]", v) and v not in ("true", "false") and not re.fullmatch(r"B0[A-Z0-9]{8}", v):
                    out.append((p, o, k))
            elif k == "altText" and isinstance(v, str) and v.strip():
                out.append((p, o, k))
            else:
                collect(v, p, out)
    elif isinstance(o, list):
        for i, v in enumerate(o):
            collect(v, f"{path}[{i}]", out)


PAREO_KW = (" This is the PAREO / beachwear brand story: write it like a native Amazon listing copywriter for that "
            "marketplace and use the words real shoppers search there for these products (e.g. pareo, sarong, beach wrap, "
            "bikini cover up, beach towel / pareo, paréo, sarong, paréo de plage / pareo, copricostume, telo mare / pareo, "
            "fular, pareo playa, salida de baño / pareo, sarong, strandlaken / pareo, sarong, strandhandduk / pareo, "
            "chusta plażowa, sarong ...), keep 'block print' as the local term for hand block printing.")


def translate(texts, locale, shorten=None, avoid=None, topic=None):
    lang = LANG[locale]
    units = ""
    if locale in ("en-US", "es-US"):
        units = (" Convert measurements for US shoppers: 60 x 20 cm -> 24 x 8 in, 60 x 60 x 20 cm -> 24 x 24 x 8 in, "
                 "50 x 50 x 50 cm -> 20 x 20 x 20 in, 60 cm -> 24 in, 2,3 kg -> 5 lb, 3,2 kg -> 7 lb, 2,5 kg -> 5.5 lb, "
                 "3,5 kg -> 7.7 lb, 5 kg -> 11 lb, 180 x 115 cm -> 71 x 45 in. Use the US spelling.")
    rules = ("You translate Amazon A+ content for Mandala Life ART (handmade pouf covers from Rajasthan, India) "
             f"from German into {lang}. Natural, native marketing language, not word for word. Keep 'Mandala Life ART' as is. "
             "Keep product facts exactly (sizes, kg, materials, 'cover only / no filling'). Keep each item about as short "
             "as the German or shorter (Amazon has hard length limits). No claims that are not in the German text. "
             "Short table values (Eckig, Rund, Baumwolle, Samt, Polyester, ...) stay short." + units +
             (PAREO_KW if topic in ("beach", "blockprint") else "") +
             " Answer with ONLY a JSON array of strings, same length and order as the input.")
    payload = texts
    if avoid:
        rules += " Amazon rejects these words, do NOT use them (use a synonym): " + ", ".join(avoid) + "."
    if shorten:
        rules += " Each item has a max character count given as [max N] - the translation MUST be at most N characters."
        payload = [f"{t} [max {n}]" for t, n in zip(texts, shorten)]
    client = anthropic.Anthropic()
    for attempt in range(3):
        msg = client.messages.create(model="claude-sonnet-5-5", max_tokens=16000, system=rules,
                                     messages=[{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}])
        txt = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text").strip()
        txt = re.sub(r"^```(?:json)?|```$", "", txt).strip()
        try:
            arr = json.loads(txt)
            if isinstance(arr, list) and len(arr) == len(texts):
                return [re.sub(r"\s*\[max \d+\]$", "", str(a)) for a in arr]
        except Exception:
            pass
    if len(texts) > 1:
        return [translate([t], locale, [n] if shorten else None, avoid)[0] for t, n in zip(texts, shorten or [None] * len(texts))]
    raise RuntimeError(f"translation failed for {locale}")


# ---------- media / links ----------
def jpeg_dims(url):
    data = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=60).read()
    i = 2
    while i < len(data):
        if data[i] != 0xFF:
            i += 1; continue
        if data[i + 1] in (0xC0, 0xC1, 0xC2):
            import struct
            h, w = struct.unpack(">HH", data[i + 5:i + 9]); return w, h, data
        i += 2 + int.from_bytes(data[i + 2:i + 4], "big")
    return None, None, data


def upload_image(cred, mp, data):
    q = 90
    while len(data) > 3000000 and q >= 50:
        from PIL import Image
        im = Image.open(io.BytesIO(data)).convert("RGB"); b = io.BytesIO()
        im.save(b, "JPEG", quality=q, optimize=True); data = b.getvalue(); q -= 10
    up = Upload(credentials=cred, marketplace=mp)
    res = call(up.upload_document, "aplus/2020-11-01/contentDocuments", io.BytesIO(data), content_type="image/jpeg").payload
    u = urllib.parse.urlsplit(res["url"])
    r = requests.post(f"{u.scheme}://{u.netloc}{u.path}", data=dict(urllib.parse.parse_qsl(u.query)),
                      files={"file": ("img.jpg", data, "image/jpeg")}, timeout=180)
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"upload failed {r.status_code} {r.text[:200]}")
    return res["uploadDestinationId"]


def us_hero(fam):
    d = json.load(open(os.path.join(HERE, "logs/aplus_us_premium_docs_2026_11_16.json")))
    src = {"vintage": "2024 Vintage Pouf", "boho": "2024 Boho Pouf", "wool": "2024 WOOL", "velvet": "2024 Vintage Pouf", "blockprint": "2024 Vintage Pouf"}[fam]
    for m in d[src]["doc"]["contentModuleList"]:
        if (m.get("premiumHeroVideo") or {}).get("heroVideo"):
            return copy.deepcopy(m["premiumHeroVideo"]["heroVideo"])


def remap_media_us(o, st, cred, mp, fam):
    if isinstance(o, dict) and "videoMediaId" in o and "imageMediaId" in o:
        o.clear(); o.update(us_hero(fam)); return
    if isinstance(o, dict):
        for k, v in list(o.items()):
            if k == "uploadDestinationId" and isinstance(v, str) and not v.startswith("/images/"):
                if v not in st["media_us"]:
                    _, _, data = jpeg_dims("https://m.media-amazon.com/images/S/" + v)
                    st["media_us"][v] = upload_image(cred, mp, data); save_state(st)
                o[k] = st["media_us"][v]
            elif k == "videoMediaId" and isinstance(v, str):
                if fam not in US_VIDEO:
                    raise RuntimeError(f"no US video for {fam}")
                o[k] = US_VIDEO[fam]
            else:
                remap_media_us(v, st, cred, mp, fam)
    elif isinstance(o, list):
        for v in o:
            remap_media_us(v, st, cred, mp, fam)


def catalog_main(ci, mid, asin):
    p = call(ci.get_catalog_item, asin, marketplaceIds=[mid], includedData=["images"]).payload
    imgs = [i for i in p["images"][0]["images"] if i["variant"] == "MAIN"] if p.get("images") else []
    best = [i for i in imgs if i["width"] == 500] or imgs
    return best[0]["link"].replace("https://m.media-amazon.com", "") if best else None


def fix_links(doc, live, fam_live, ci, mid, notes, group_of=None):
    group_of = group_of or {}
    for m in doc["contentModuleList"]:
        car = m.get("premiumImageCarousel")
        if car:
            for c in car["carouselCards"]:
                ip = c["imagePanel"]; a = (ip.get("asin") or {}).get("value") if isinstance(ip.get("asin"), dict) else None
                if a and a not in live:
                    ip["asin"] = None; ip["buttonText"] = None; notes.append(f"carousel link {a} removed")
        sc = m.get("premiumComparisonScroller")
        if sc:
            keep = [i for i, p in enumerate(sc["comparisonProducts"]) if p["comparisonProduct"]["productAsin"]["value"] in live]
            gone = [p["comparisonProduct"]["productAsin"]["value"] for i, p in enumerate(sc["comparisonProducts"]) if i not in keep]
            if gone:
                notes.append(f"comparison removed {gone}")
                sc["comparisonProducts"] = [sc["comparisonProducts"][i] for i in keep]
                for n, p in enumerate(sc["comparisonProducts"]):
                    p["comparisonProduct"]["position"] = n + 1
                for r in sc["comparisonRows"]:
                    f = r["comparisonRow"]["comparisonFields"]
                    r["comparisonRow"]["comparisonFields"] = [f[i] for i in keep if i < len(f)]
                    for n, x in enumerate(r["comparisonRow"]["comparisonFields"]):
                        if isinstance(x.get("comparisonField"), dict):
                            x["comparisonField"]["position"] = n + 1
        fa = m.get("brandStoryFourAsin")
        if fa:
            used = {x["asinImage"]["productAsin"]["value"] for x in fa["asinImages"]}
            spare = [a for a in fam_live if a not in used]
            for x in fa["asinImages"]:
                a = x["asinImage"]["productAsin"]["value"]
                if a not in live:
                    spare.sort(key=lambda b: group_of.get(b) != group_of.get(a))  # same product group first
                    while spare:
                        b = spare.pop(0); img = catalog_main(ci, mid, b)
                        if img:
                            x["asinImage"]["productAsin"]["value"] = b
                            pi = x["asinImage"]["productImage"]; pi["uploadDestinationId"] = img
                            pi["imageCropSpecification"] = None; pi["altText"] = "Mandala Life ART"
                            notes.append(f"brand story {a} -> {b}"); break


def fill_crops_alts(o):
    if isinstance(o, dict):
        if o.get("uploadDestinationId"):
            if not o.get("altText") or len(o["altText"]) > 100:
                o["altText"] = (o.get("altText") or "Mandala Life ART")[:100]
            if "imageCropSpecification" in o and not (o["imageCropSpecification"] or {}).get("size"):
                u = o["uploadDestinationId"]
                url = "https://m.media-amazon.com" + u if u.startswith("/images/") else "https://m.media-amazon.com/images/S/" + u
                w, h, _ = jpeg_dims(url)
                px = lambda v: {"value": v, "units": "pixels"}
                if "productImage" in str(o.get("_ctx", "")):
                    pass
                o["imageCropSpecification"] = {"size": {"width": px(w), "height": px(h)}, "offset": {"x": px(0), "y": px(0)}}
        for v in o.values():
            fill_crops_alts(v)
    elif isinstance(o, list):
        for v in o:
            fill_crops_alts(v)


# ---------- main ----------
def main():
    mpc, locale = sys.argv[1], sys.argv[2]
    apply_ = "--apply" in sys.argv
    only = next((a.split("=", 1)[1].split(",") for a in sys.argv if a.startswith("--docs=")), list(SOURCE))
    st = load_state()
    cde, _ = creds("DE"); ap_de = AplusContent(credentials=cde, marketplace=Marketplaces.DE, timeout=120)
    cred, _ = creds(mpc); mp = getattr(Marketplaces, mpc); mid = mp.marketplace_id
    ap = AplusContent(credentials=cred, marketplace=mp, timeout=120)
    ci = CatalogItems(credentials=cred, marketplace=mp, version="2022-04-01")
    # family ASINs live in this marketplace
    rows = pb_list_all(pb_authenticate(), "asin_group_mapping", 'asin != ""', fields="asin,sku,group")
    by_sku = {r["sku"]: r["group"] for r in rows}; by_asin = {r["asin"]: r["group"] for r in rows}
    lst = listings(mpc, "summaries")
    live, fam = set(), {"vintage": [], "boho": [], "velvet": [], "wool": [], "story": [], "beach": [], "blockprint": []}
    for sku, l in lst.items():
        a = l["asin"]
        if not a or not ({"BUYABLE", "DISCOVERABLE"} & set(l["status"])):
            continue
        live.add(a)
        g = by_sku.get(sku) or by_asin.get(a) or ""
        f = {"COVER_VINTAGE": "vintage", "COVER_BOHO": "boho", "VELVET": "velvet"}.get(g)
        if a == WOOL_ASIN:
            f = "wool"
        if f and a not in fam[f]:
            fam[f].append(a)
        if g == "PAREO_B" and a not in fam["blockprint"]:
            fam["blockprint"].append(a)
        if g.startswith("PAREO") and a not in fam["beach"]:
            fam["beach"].append(a)
        if g in ("COVER_VINTAGE", "COVER_BOHO", "VELVET", "STUFFED") and a not in fam["story"]:
            fam["story"].append(a)
    print(mpc, locale, {k: len(v) for k, v in fam.items()})
    report = []
    for name in only:
        if not fam[name]:
            print(f"  {name}: no live products in {mpc} - skipped"); continue
        src = call(ap_de.get_content_document, SOURCE[name], marketplaceId=DE_MID, includedDataSet=["CONTENTS"]).payload["contentRecord"]["contentDocument"]
        doc = copy.deepcopy(src)
        doc["locale"] = locale
        doc["name"] = f"2026 {mpc} {NAMES[name]} {locale}" if name not in ("story", "beach") else f"{NAMES[name]} 2026 {locale}"
        notes = []
        fix_links(doc, live, fam[name], ci, mid, notes, by_asin)
        if mpc == "US":
            remap_media_us(doc["contentModuleList"], st, cred, mp, name)
        # translate
        items = []; collect(doc["contentModuleList"], "contentModuleList", items)
        german = {id(h): h[k] for _, h, k in items}
        if locale != "de-DE":
            tkey = f"{name}|{SHARE_TRANSLATION.get(locale, locale)}"
            srcs = [h[k] for _, h, k in items]
            cached = st["tr"].get(tkey)
            if cached and cached.get("src") == srcs:
                outs = cached["out"]
            else:
                outs = translate(srcs, SHARE_TRANSLATION.get(locale, locale), topic=name)
                st["tr"][tkey] = {"src": srcs, "out": outs}; save_state(st)
            for (_, h, k), t in zip(items, outs):
                h[k] = t
        src_of = german
        fill_crops_alts(doc["contentModuleList"])
        # validate, shorten over-limit strings (max 2 rounds)
        for rnd in range(3):
            try:
                call(ap.validate_content_document_asin_relations, body={"contentDocument": doc}, marketplaceId=mid, asinSet=fam[name])
                errs = []
            except Exception as e:
                errs = errors_of(e)
            too_long = {}
            for e in errs:
                m_ = re.search(r"contentDocument\.(contentModuleList\[.*?)\.value: length must be between \d+ and (\d+)", e)
                if m_:
                    too_long[m_.group(1) + ".value"] = int(m_.group(2))
                m4 = re.search(r"contentDocument\.(contentModuleList\[.*?\.textList): Total text length (\d+) exceeds maximum of (\d+)", e)
                if m4:
                    pre = m4.group(1) + "["; tot, mx = int(m4.group(2)), int(m4.group(3))
                    for pp, hh, kk in items:
                        if pp.startswith(pre):
                            too_long[pp] = max(5, int(len(hh[kk]) * mx / tot) - 4)
                m2 = re.search(r"contentDocument\.(contentModuleList\[.*?)\.altText: size must be between 1 and (\d+)", e)
                if m2:
                    too_long[m2.group(1) + ".altText"] = int(m2.group(2))
            banned = [w.strip() for e in errs for m3 in [re.search(r"community guidelines: (.+?)\.?$", e)] if m3 for w in m3.group(1).split(",")]
            if banned and rnd < 2:
                bad = [(h, k, src_of.get(id(h), h[k])) for _, h, k in items if any(b.lower() in str(h[k]).lower() for b in banned)]
                if bad:
                    outs2 = translate([b[2] for b in bad], SHARE_TRANSLATION.get(locale, locale), avoid=banned)
                    for (h, k, _), t in zip(bad, outs2):
                        for b in banned:
                            t = re.sub(re.escape(b), "", t, flags=re.I) if b.lower() in t.lower() else t
                        h[k] = t
                    continue
            if not too_long or rnd == 2:
                break
            idx = {p: (h, k) for p, h, k in items}
            fix = [(p, n) for p, n in too_long.items() if p in idx]
            if not fix:
                break
            short = translate([idx[p][0][idx[p][1]] for p, _ in fix], SHARE_TRANSLATION.get(locale, locale) if locale != "de-DE" else "de-DE",
                              shorten=[max(5, n - 3) for _, n in fix])
            for (p, n), t in zip(fix, short):
                h, k = idx[p]; h[k] = t[:n]
        key_ = f"{mpc}|{locale}|{name}"
        print(f"  {name}: {len(fam[name])} asins, errors={errs[:4]} notes={notes[:6]}")
        res = "validated" if not errs else "ERRORS"
        if apply_ and not errs:
            k = st["docs"].get(key_)
            try:
                if k:
                    call(ap.update_content_document, k, body={"contentDocument": doc}, marketplaceId=mid)
                else:
                    k = call(ap.create_content_document, body={"contentDocument": doc}, marketplaceId=mid).payload["contentReferenceKey"]
                    st["docs"][key_] = k; save_state(st)
                aset = list(fam[name]); dropped = []
                for attempt in range(3):
                    call(ap.post_content_document_asin_relations, k, body={"asinSet": aset}, marketplaceId=mid)
                    if "--draft" in sys.argv:
                        break
                    try:
                        call(ap.post_content_document_approval_submission, k, marketplaceId=mid)
                        break
                    except Exception as e:
                        s_ = str(e)
                        bad = set()
                        for m5 in re.finditer(r"(?:do not exist in the catalog: |controlled by another project|you do not control: \[)([^'\]]*)", s_):
                            bad |= set(re.findall(r"B0[A-Z0-9]{8}", m5.group(0)))
                        bad &= set(aset)
                        if not bad or attempt == 2:
                            raise
                        dropped += sorted(bad); aset = [a for a in aset if a not in bad]
                res = ("DRAFT " if "--draft" in sys.argv else "SUBMITTED ") + k + (f" (dropped {dropped})" if dropped else "")
            except Exception as e:
                res = "ERR " + "; ".join(errors_of(e))[:400]
            print("   ", res)
        report.append({"doc": name, "mp": mpc, "locale": locale, "asins": len(fam[name]), "result": res, "errors": errs[:6], "notes": notes})
    st.setdefault("runs", []).append({"at": time.strftime("%Y-%m-%d %H:%M"), "report": report}); save_state(st)


if __name__ == "__main__":
    main()
