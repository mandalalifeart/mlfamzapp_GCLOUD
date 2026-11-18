import os,json,copy,sys
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces
from WeeklyChecks import creds
S=os.environ["AMAZON_SELLER_ID_EU"]
cc,_=creds("DE")
de=ListingsItems(credentials=cc, marketplace=Marketplaces.DE, timeout=120).get_listings_item(S,"CoverPillowRoundYellow94D",marketplaceIds=[Marketplaces.DE.marketplace_id],includedData=["attributes"]).payload["attributes"]
m=Marketplaces.IE; mid=m.marketplace_id
li=ListingsItems(credentials=cc, marketplace=m, timeout=120)
EN={"item_name":"Mandala Life ART Round Floor Cushion Cover 24 Inch (60 cm) with Fringe, Turquoise & Pink Oriental Print – Cover Only, Bohemian Cotton Floor Pillow Cover for Meditation, Yoga & Boho Seating",
 "color":"Turquoise Print","material":"Cotton","fabric_type":"100% Cotton","brand":"Mandala Life ART","manufacturer":"Mandala Life ART",
 "product_description":"Add bohemian charm to your home with this round floor cushion cover with fringe. Made of 100% cotton with a turquoise and pink oriental medallion print and a turquoise fringe edge - perfect as a floor cushion, meditation cushion or decorative accent. Ships without filling."}
BUL=["ROUND 60 CM WITH FRINGE - Flat round floor cushion cover (approx. 60 cm diameter) with a playful turquoise fringe all around and an oriental medallion print in turquoise, pink and cream.",
 "COVER ONLY - Ships without filling. Zip closure; fill it with a round cushion insert, bean bag or hobby filling.",
 "100% COTTON - Soft, breathable cotton fabric, durable and easy care for everyday use.",
 "AUTHENTIC ARTISAN QUALITY - Handmade by artisans in Rajasthan, India.",
 "BOHO CHIC DECOR - Perfect as a floor pillow, meditation cushion or seat cushion in the living room, bedroom, yoga room or kids' corner. A lovely housewarming gift."]
attrs={}
for k,vals in de.items():
    if k in ("purchasable_offer","list_price","merchant_shipping_group","bullet_point","unit_count","fulfillment_availability"): continue
    new=[]
    for x in vals:
        if not isinstance(x,dict) or x.get("language_tag")=="it_IT": continue
        y=copy.deepcopy(x); y["marketplace_id"]=mid
        if "language_tag" in y: y["language_tag"]="en_IE"
        if k in EN: y["value"]=EN[k]
        new.append(y)
    if new: attrs[k]=new[:1]
attrs["bullet_point"]=[{"value":b,"language_tag":"en_IE","marketplace_id":mid} for b in BUL]
attrs["purchasable_offer"]=[{"currency":"EUR","audience":"ALL","our_price":[{"schedule":[{"value_with_tax":39.99}]}],"marketplace_id":mid}]
attrs["list_price"]=[{"value_with_tax":39.99,"currency":"EUR","marketplace_id":mid}]
attrs["unit_count"]=[{"type":{"value":"count","language_tag":"en_IE"},"value":1.0,"marketplace_id":mid}]
attrs["fulfillment_availability"]=[{"fulfillment_channel_code":"AMAZON_EU"}]
attrs["item_shape"]=[{"value":"Round","language_tag":"en_IE","marketplace_id":mid}]
for mode in ("VALIDATION_PREVIEW", None):
    if mode is None and "--apply" not in sys.argv: break
    r=li.put_listings_item(S,"CoverPillowRoundYellow94D",marketplaceIds=[mid],body={"productType":"DECORATIVE_PILLOW_COVER","requirements":"LISTING","attributes":attrs},includedData=["issues"],**({"mode":mode} if mode else {})).payload
    print(mode,r.get("status"),[(i["code"],i["severity"],i["message"][:110]) for i in r.get("issues",[])])
    if r.get("status") not in ("VALID","ACCEPTED"): break
