"""First Google Ads campaign (2026-11-20, per the user "ok"): US home decor
Search ads straight to amazon.com, PAUSED until the user says go.
Default = validateOnly (Google checks everything, creates nothing); --apply creates.
Amazon Attribution: final URL suffix = the US Attribution Google tag (publisher 1)
with Google's ValueTrack macros, so every Amazon sale shows per campaign/ad group/ad."""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

E = os.environ
V = E.get("GOOGLE_ADS_API_VERSION", "v26")
CID = E["GOOGLE_ADS_CUSTOMER_ID"]
APPLY = "--apply" in sys.argv
SUFFIX = ("maas=maas_adg_api_577005570021012570_macro_1_964&ref_=aa_maas&tag=maas"
          "&aa_campaignid={campaignid}&aa_adgroupid={adgroupid}&aa_creativeid=ad-{creative}_{targetid}")
NAME = "US Home Decor - Search - Amazon"
DAILY_USD = 10
NEGATIVES = ["stuffed", "filled", "insert", "diy", "pattern", "crochet", "cheap", "ikea", "walmart", "target", "free"]

COMMON_H = ["Mandala Life ART", "Handmade in India", "Fast Prime Delivery", "Easy Returns on Amazon",
            "Shop Now on Amazon", "Ships Flat - Fill at Home"]
GROUPS = [
    {"name": "Moroccan floor pillows", "bid": 0.50, "url": "https://www.amazon.com/dp/B07N2YYGXL",
     "kw": ["moroccan floor pillow", "moroccan floor pillows", "moroccan floor cushion", "boho floor cushions",
            "bohemian floor cushion", "boho floor pillows", "floor pillow cover"],
     "h": ["Moroccan Floor Pillow Covers", "Boho Floor Cushion Covers", "Bohemian Floor Cushions",
           "Round 24x8 Floor Pillow", "Meditation Floor Cushion", "Artisan Printed Cotton",
           "Boho Living Room Decor", "Moroccan Style Seating", "Colorful Boho Floor Pouf"],
     "d": ["Handmade boho floor pillow covers from India. Fill with blankets or a pouf insert.",
           "Unstuffed round 24x8 inch covers in printed cotton. Fast Prime delivery on Amazon.",
           "Moroccan style floor cushions for living room, kids room and meditation corner.",
           "Loved by thousands of customers. Easy returns through Amazon. Shop the collection."]},
    {"name": "Pouf covers", "bid": 0.50, "url": "https://www.amazon.com/mandalalifeart",
     "kw": ["pouf cover", "moroccan pouf cover", "pouf ottoman cover", "unstuffed pouf ottoman",
            "24x24x8 cushion cover", "unstuffed pouf", "boho pouf cover"],
     "h": ["Boho Pouf Covers", "Moroccan Pouf Cover 24x8", "Unstuffed Pouf Ottoman",
           "Round & Square Pouf Covers", "Pouf Ottoman Cover", "Cotton & Velvet Pouf Covers",
           "Footrest & Extra Seating", "Over 40 Designs", "Boho Living Room Decor"],
     "d": ["Unstuffed boho pouf covers, round and square. Fill with old blankets or an insert.",
           "Handmade in India in printed cotton and velvet. Fast Prime delivery on Amazon.",
           "A footrest, side table and extra seat in one. Ships flat, zipper closure.",
           "Loved by thousands of customers. Easy returns through Amazon. Shop the collection."]},
    {"name": "Velvet and mushroom", "bid": 0.50, "url": "https://www.amazon.com/dp/B0GLMNGPCJ",
     "kw": ["velvet pouf cover", "velvet pouf", "mushroom pouf", "mushroom ottoman", "retro mushroom decor",
            "embroidered velvet pouf"],
     "h": ["70s Retro Mushroom Pouf", "Magic Mushroom Floor Pouf", "Embroidered Velvet Pouf",
           "Velvet Pouf Cover 24x8", "Groovy Retro Home Decor", "Psychedelic Mushroom Decor",
           "Soft Velvet Ottoman Cover", "Cottagecore Floor Cushion", "Unique Mushroom Gift"],
     "d": ["Hand embroidered velvet pouf cover with a retro mushroom design. A real statement piece.",
           "Unstuffed 24x8 inch velvet pouf cover. Fill at home. Fast Prime delivery on Amazon.",
           "Groovy 70s style decor for living room, bedroom or kids room. Handmade in India.",
           "Loved by thousands of customers. Easy returns through Amazon. Shop the collection."]},
    {"name": "Brand", "bid": 0.30, "url": "https://www.amazon.com/mandalalifeart",
     "kw": ["mandala life art", "mandala life art pouf", "mandala life art floor cushion"],
     "h": ["Mandala Life ART Official", "Mandala Life ART Store", "Boho Pouf Covers", "Velvet & Cotton Poufs",
           "Beach Pareos & Sarongs", "Boho Home Decor", "Moroccan Floor Pillows", "Shop the Full Collection",
           "Official Amazon Store"],
     "d": ["The official Mandala Life ART store on Amazon. Boho poufs, floor pillows and pareos.",
           "Handmade in India in printed cotton and velvet. Fast Prime delivery on Amazon.",
           "Unstuffed pouf covers that ship flat. Fill at home with blankets or an insert.",
           "Loved by thousands of customers. Easy returns through Amazon. Shop the collection."]},
]


def token():
    body = urllib.parse.urlencode({"client_id": E["GOOGLE_ADS_CLIENT_ID"], "client_secret": E["GOOGLE_ADS_CLIENT_SECRET"],
                                   "refresh_token": E["GOOGLE_ADS_REFRESH_TOKEN"], "grant_type": "refresh_token"}).encode()
    return json.load(urllib.request.urlopen("https://oauth2.googleapis.com/token", data=body))["access_token"]


def main():
    c = f"customers/{CID}"
    ops = [
        {"campaignBudgetOperation": {"create": {"resourceName": f"{c}/campaignBudgets/-1", "name": f"{NAME} budget",
                                                "amountMicros": str(DAILY_USD * 1_000_000), "deliveryMethod": "STANDARD",
                                                "explicitlyShared": False}}},
        {"campaignOperation": {"create": {
            "resourceName": f"{c}/campaigns/-2", "name": NAME, "status": "PAUSED", "advertisingChannelType": "SEARCH",
            "campaignBudget": f"{c}/campaignBudgets/-1", "manualCpc": {"enhancedCpcEnabled": False},
            "networkSettings": {"targetGoogleSearch": True, "targetSearchNetwork": False,
                                "targetContentNetwork": False, "targetPartnerSearchNetwork": False},
            "geoTargetTypeSetting": {"positiveGeoTargetType": "PRESENCE"},
            "finalUrlSuffix": SUFFIX,
            "containsEuPoliticalAdvertising": "DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING"}}},
        {"campaignCriterionOperation": {"create": {"campaign": f"{c}/campaigns/-2",
                                                   "location": {"geoTargetConstant": "geoTargetConstants/2840"}}}},
        {"campaignCriterionOperation": {"create": {"campaign": f"{c}/campaigns/-2",
                                                   "language": {"languageConstant": "languageConstants/1000"}}}},
    ]
    for n in NEGATIVES:
        ops.append({"campaignCriterionOperation": {"create": {"campaign": f"{c}/campaigns/-2", "negative": True,
                                                              "keyword": {"text": n, "matchType": "PHRASE"}}}})
    for i, g in enumerate(GROUPS):
        ag = f"{c}/adGroups/-{10 + i}"
        ops.append({"adGroupOperation": {"create": {"resourceName": ag, "name": g["name"], "campaign": f"{c}/campaigns/-2",
                                                    "status": "ENABLED", "type": "SEARCH_STANDARD",
                                                    "cpcBidMicros": str(int(g["bid"] * 1_000_000))}}})
        for kw in g["kw"]:
            for mt in ("EXACT", "PHRASE"):
                ops.append({"adGroupCriterionOperation": {"create": {"adGroup": ag, "status": "ENABLED",
                                                                     "keyword": {"text": kw, "matchType": mt}}}})
        heads = (g["h"] + COMMON_H)[:15]
        for t in heads + g["d"]:
            lim = 30 if t in heads else 90
            assert len(t) <= lim, (len(t), t)
        ops.append({"adGroupAdOperation": {"create": {"adGroup": ag, "status": "ENABLED", "ad": {
            "finalUrls": [g["url"]],
            "responsiveSearchAd": {"headlines": [{"text": t} for t in heads],
                                   "descriptions": [{"text": t} for t in g["d"]]}}}}})
    h = {"Authorization": "Bearer " + token(), "developer-token": E["GOOGLE_ADS_DEVELOPER_TOKEN"],
         "login-customer-id": E["GOOGLE_ADS_LOGIN_CUSTOMER_ID"], "Content-Type": "application/json"}
    body = {"mutateOperations": ops, "validateOnly": not APPLY, "partialFailure": False}
    req = urllib.request.Request(f"https://googleads.googleapis.com/{V}/{c}/googleAds:mutate",
                                 data=json.dumps(body).encode(), headers=h)
    try:
        r = json.load(urllib.request.urlopen(req))
        print("APPLIED" if APPLY else "VALID (validateOnly)", len(ops), "operations")
        if APPLY:
            json.dump(r, open("logs/google_ads_us_home_decor_2026_11_20.json", "w"), indent=1)
            camp = [x for x in r.get("mutateOperationResponses", []) if "campaignResult" in x]
            print(camp)
    except urllib.error.HTTPError as x:
        d = json.loads(x.read())
        for err in (d["error"].get("details") or [{}])[0].get("errors", []):
            print(err.get("errorCode"), err.get("message"), json.dumps(err.get("location"))[:200])


if __name__ == "__main__":
    main()
