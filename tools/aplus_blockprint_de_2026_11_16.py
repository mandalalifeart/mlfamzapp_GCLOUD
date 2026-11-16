"""German Premium A+ for the block-print pareos (PAREO_B), per the user
2026-11-16 (texts approved in chat: "ok" + "Fisch" card fixed). Images = the
user's block-print photos (SocialMarketting media) + the printer photo from the
EU A+ library, pre-cropped to 1464x600 (desktop) / 4:3 (mobile, columns),
uploaded to the EU A+ library. Built as DRAFT; --submit attaches the DE PAREO_B
ASINs and submits."""
import copy
import io
import json
import os
import re
import sys
import urllib.parse

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sp_api.api import AplusContent, Upload
from sp_api.base import Marketplaces
from WeeklyChecks import creds, listings
from AsinAnalytics import pb_authenticate, pb_list_all

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMG = "/tmp/claude-1000/-home-bababot/7e859b51-fcac-4563-9743-a3131a35fa27/scratchpad/bp/out/"
TEMPLATE = "/tmp/claude-1000/-home-bababot/7e859b51-fcac-4563-9743-a3131a35fa27/scratchpad/de_pareo_premium.json"
MID = "A1PA6795UKMFR9"
STATE = os.path.join(HERE, "logs/aplus_rollout_state.json")
c, _ = creds("DE")
ap = AplusContent(credentials=c, marketplace=Marketplaces.DE, timeout=120)
up = Upload(credentials=c, marketplace=Marketplaces.DE)
st = json.load(open(STATE))
media = st.setdefault("bp_media", {})


def upload(name):
    if name in media:
        return media[name]
    data = open(IMG + name, "rb").read()
    res = up.upload_document("aplus/2020-11-01/contentDocuments", io.BytesIO(data), content_type="image/jpeg").payload
    u = urllib.parse.urlsplit(res["url"])
    r = requests.post(f"{u.scheme}://{u.netloc}{u.path}", data=dict(urllib.parse.parse_qsl(u.query)),
                      files={"file": ("img.jpg", data, "image/jpeg")}, timeout=180)
    assert r.status_code in (200, 201, 204), r.text[:200]
    media[name] = res["uploadDestinationId"]
    json.dump(st, open(STATE, "w"), indent=1, ensure_ascii=False)
    return media[name]


px = lambda v: {"value": v, "units": "pixels"}
tv = lambda v: {"value": v, "decoratorSet": []}
tl = lambda *v: {"textList": [tv(x) for x in v]}


def img(name, w, h, alt):
    return {"uploadDestinationId": upload(name), "altText": alt,
            "imageCropSpecification": {"size": {"width": px(w), "height": px(h)}, "offset": {"x": px(0), "y": px(0)}}}


tmpl = json.load(open(TEMPLATE))["contentModuleList"]
FB = next(m for m in tmpl if m["contentModuleType"] == "PREMIUM_FULL_BACKGROUND_IMAGE")
FC = next(m for m in tmpl if m["contentModuleType"] == "PREMIUM_FOUR_COLUMN_IMAGES")


def full_bg(base, headline, footer, alt):
    m = copy.deepcopy(FB)
    b = m["premiumFullBackgroundImage"]
    b["headline"] = tv(headline)
    b["footer"] = tl(footer) if footer else None
    b["desktopImage"] = img(f"{base}_d.jpg", 2196, 900, alt)
    b["mobileImage"] = img(f"{base}_m.jpg", 1600, 1200, alt)
    return m


def four_col(headline, cols):
    m = copy.deepcopy(FC)
    f = m["premiumFourColumnImages"]
    f["headline"] = tv(headline)
    for col, (name, h, d, alt) in zip(f["columns"], cols):
        ic = col["imageColumn"]
        ic["image"] = img(name, 1600, 1200, alt)
        ic["headline"] = tv(h)
        ic["description"] = tl(d)
    return m


modules = [
    full_bg("hero", "Handbedruckte Baumwoll-Pareos",
            "In Rajasthan stempeln Kunsthandwerker jedes Muster mit geschnitzten Holzstempeln von Hand auf reine Baumwolle – "
            "so wird jeder Pareo ein Unikat.", "Handbedruckter Blockdruck-Pareo aus Baumwolle am Strand"),
    four_col("Unsere Blockdruck-Designs", [
        ("des_flower.jpg", "Blumenmuster", "Florale Blockdruck-Muster, zart und sommerlich.", "Blockdruck-Pareo mit Blumenmuster"),
        ("des_fish.jpg", "Fisch", "Verspielte Fischmotive in Blau – wie gemacht für den Strand.", "Blauer Blockdruck-Pareo mit Fischmuster"),
        ("des_classic.jpg", "Klassisch", "Traditionelle Muster aus Rajasthan.", "Klassischer Blockdruck-Pareo"),
        ("des_modern.jpg", "Modern", "Moderne Muster, traditionell von Hand gedruckt.", "Moderner Blockdruck-Pareo")]),
    full_bg("printer", "Vom Holzstempel auf den Stoff", "Jeder Druck entsteht von Hand – Schicht für Schicht, Farbe für Farbe.",
            "Kunsthandwerker beim Handblockdruck in Rajasthan"),
    full_bg("size", "Großzügige 180 x 115 cm aus reiner Baumwolle",
            "Leicht, weich und atmungsaktiv – ideal für heiße Tage und leicht im Gepäck.", "Blockdruck-Pareo aus Baumwolle"),
    full_bg("care", "Pflegeleicht und maschinenwaschbar", "Für die erste Wäsche empfehlen wir, den Pareo separat zu waschen.",
            "Vier Frauen mit Blockdruck-Pareos am Strand"),
    four_col("Ein Tuch – viele Möglichkeiten", [
        ("use_dress.jpg", "Kleid & Bikini-Cover-up", "Als Kleid, Rock oder Bikini-Cover-up gebunden – in Sekunden.", "Blockdruck-Pareo als Kleid"),
        ("use_towel.jpg", "Strandtuch", "Leicht und schnell trocken – perfekt für Strand und Picknick.", "Blockdruck-Pareo als Strandtuch"),
        ("use_scarf.jpg", "Schal & Überwurf", "Kuschelig über die Schultern an kühlen Abenden.", "Blockdruck-Pareo als Überwurf"),
        ("use_table.jpg", "Tischdecke", "Bringt Boho-Flair auf Tisch und Terrasse.", "Blockdruck-Pareo als Tischdecke")]),
]
doc = {"name": "2026 DE Block Print", "contentType": "EBC", "contentSubType": "EMC", "locale": "de-DE", "contentModuleList": modules}
src = json.load(open(TEMPLATE))
for k in ("contentType", "contentSubType"):
    if k in src:
        doc[k] = src[k]
rows = pb_list_all(pb_authenticate(), "asin_group_mapping", 'asin != ""', fields="asin,sku,group")
grp = {r["sku"]: r["group"] for r in rows}
asins = sorted({v["asin"] for s, v in listings("DE", "summaries").items()
                if grp.get(s) == "PAREO_B" and v["asin"] and {"BUYABLE", "DISCOVERABLE"} & set(v["status"])})
try:
    ap.validate_content_document_asin_relations(body={"contentDocument": doc}, marketplaceId=MID, asinSet=asins)
    errs = []
except Exception as e:
    errs = re.findall(r"'details': '([^']+)'", str(e)) or re.findall(r"'message': '([^']+)'", str(e)) or [str(e)[:800]]
print(len(asins), "asins; errors:", errs[:8])
if not errs:
    k = st.get("bp_doc_de")
    if k:
        ap.update_content_document(k, body={"contentDocument": doc}, marketplaceId=MID)
    else:
        k = ap.create_content_document(body={"contentDocument": doc}, marketplaceId=MID).payload["contentReferenceKey"]
        st["bp_doc_de"] = k
        json.dump(st, open(STATE, "w"), indent=1, ensure_ascii=False)
    print("draft", k)
    if "--submit" in sys.argv:
        ap.post_content_document_asin_relations(k, body={"asinSet": asins}, marketplaceId=MID)
        print("submit", ap.post_content_document_approval_submission(k, marketplaceId=MID).payload)
