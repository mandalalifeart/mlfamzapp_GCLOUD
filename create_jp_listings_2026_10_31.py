"""Create 8 Amazon JP listings (per the user 2026-10-31) that Build International
Listings could not create: PAREO_B_539/542/543/544/545/549 (SWIMWEAR) and
CoverPoufRectBlue93 / CoverPoufRoundYellow94 (DECORATIVE_PILLOW_COVER).
Japanese content is copied from the BIL-created JP listings of the same
families (PAREO_B_550 = B0GY1RHXMG, CoverPoufRectGray101 = B083NR5LPC), images,
EANs and dimensions from the US listings. FBA (AMAZON_JP), price = US x 158
(same ratio BIL uses). Default is VALIDATION_PREVIEW; --apply submits.
Usage: python create_jp_listings_2026_10_31.py [--apply] [--only=SKU,SKU]"""
import json
import os
import sys
import time

from sp_api.api import CatalogItems, ListingsItems
from sp_api.base import Marketplaces

JP = Marketplaces.JP
JP_ID = JP.marketplace_id
US = Marketplaces.US
SELLER_JP = os.environ["AMAZON_SELLER_ID_JP"]
CRED_JP = dict(refresh_token=os.environ["REFRESH_TOKEN_JP"], lwa_app_id=os.environ["CLIENT_ID_USA"],
               lwa_client_secret=os.environ["CLIENT_SECRET_USA"])
CRED_US = dict(refresh_token=os.environ["REFRESH_TOKEN_USA"], lwa_app_id=os.environ["CLIENT_ID_USA"],
               lwa_client_secret=os.environ["CLIENT_SECRET_USA"])
JPY_PER_USD = 158.0

PAREOS = {
    "PAREO_B_539": {"color": "ブロック 15", "pattern": "魚柄",
                    "name": "Mandala Life ART ブロックプリント コットン ビーチパレオ レディース 魚柄 ブルー - サロン ビキニカバーアップ 水着カバーアップ ラップスカート"},
    "PAREO_B_542": {"color": "フラワー 5"},
    "PAREO_B_543": {"color": "フラワー 6"},
    "PAREO_B_544": {"color": "フラワー 7"},
    "PAREO_B_545": {"color": "フラワー 8"},
    "PAREO_B_549": {"color": "フラワー 12"},
}
PAREO_NAME = "Mandala Life ART プレミアムコットン ハンドブロックプリント ビーチパレオ レディース - サロン ビキニカバーアップ 水着カバーアップ 軽量ラップスカート, {color}"
PAREO_EXTRA_BULLET = "ゆったりサイズと上質な生地 – 180cm x 110cm(72インチ x 42インチ)。100%プレミアムコットンで、とても軽く肌にやさしい着心地です。"

COVERS = {
    "CoverPoufRectBlue93": {"color": "ブルー", "shape": "正方形", "size": "61x61x20cm カバーのみ",
                            "catalog_name": "Blue Square Floor Pillow Cover Large Yoga Décor Cushion Moroccan Pouf Ottoman Boho Furniture Perfect for Living Room, Bedroom, Nursary or Children Room Seating Area Beanbag Footstool 24x8 Unstuffed",
                            "name": "Mandala Life ART ボヘミアン フロアクッションカバー ブルー 正方形 61x61x20cm - モロッコ風 プーフ オットマンカバー(中身なし) ヨガ 瞑想 クッション コットン"},
    "CoverPoufRoundYellow94": {"color": "イエロー", "shape": "円形", "size": "直径61x20cm カバーのみ",
                               "name": "Mandala Life ART ボヘミアン フロアクッションカバー イエロー 丸型 直径61x20cm - モロッコ風 プーフ オットマンカバー(中身なし) ヨガ 瞑想 クッション コットン"},
}
COVER_BULLETS = [
    "ボホシックなインテリア – リビング、寝室、子ども部屋にボヘミアンな雰囲気をプラス。床に置くだけで、創造的で実用的、そして贅沢なアクセントになります。",
    "本物の職人品質 – インド・ラジャスタンの職人がひとつひとつ丁寧に手作りしたフロアクッションカバー。品質、細部、仕上げにこだわった伝統の技です。",
    "お好みで詰め物を – カバーのみ(中身なし)の商品です。丸型は約2.3kg、正方形は約3.2kgの細かく刻んだウレタンフォームがおすすめ。古いクッションや布団、毛布を再利用することもできます。",
    "空間に彩りを – 豊富な色とデザインからお部屋にぴったりの一枚を。ひとつでアクセントに、いくつか組み合わせて心地よく鮮やかな空間に。",
    "サイズと素材 – 約61cm x 20cm、コットン100%のプリント生地。通気性がよく丈夫で、フロアクッション、瞑想クッション、オットマンとして毎日使えます。",
]
COVER_DESCRIPTION = ("ヴィンテージ・コレクションのフロアクッションカバーで、時代を超えたボホの魅力をお部屋に。"
                     "コットン100%のプリント生地に、ヴィンテージ風の繊細なマンダラ柄をあしらった約61x20cmのカバーです。"
                     "フロアクッション、瞑想クッション、ボヘミアンなアクセントとして最適。通気性のよいコットンは丈夫でお手入れも簡単で、"
                     "毎日のくつろぎにも、ボホシックなインテリアの主役にもぴったりです。カバーのみの販売で、中身は含まれません。")

COPY_FROM_US = ("main_product_image_locator", "swatch_product_image_locator",
                *[f"other_product_image_locator_{i}" for i in range(1, 9)])


def jpv(value, lang=True):
    d = {"value": value, "marketplace_id": JP_ID}
    if lang:
        d["language_tag"] = "ja_JP"
    return [d]


def strip_mp(values):
    out = []
    for v in values:
        v = {k: x for k, x in v.items() if k != "marketplace_id"}
        v["marketplace_id"] = JP_ID
        out.append(v)
    return out


def us_listing(li_us, sku):
    return li_us.get_listings_item(os.environ["AMAZON_SELLER_ID"], sku, marketplaceIds=[US.marketplace_id],
                                   includedData=["attributes", "offers", "summaries"]).payload


def us_price(us):
    return float(next(o["price"]["amount"] for o in us.get("offers", []) if o["price"]["currencyCode"] == "USD"))


def offer_attrs(sku, us):
    price = round(us_price(us) * JPY_PER_USD)
    return price, {
        "condition_type": [{"value": "new_new", "marketplace_id": JP_ID}],
        "fulfillment_availability": [{"fulfillment_channel_code": "AMAZON_JP"}],
        "purchasable_offer": [{"marketplace_id": JP_ID, "currency": "JPY", "audience": "ALL",
                               "our_price": [{"schedule": [{"value_with_tax": price}]}]}],
        # model/part number left out: the global catalog already has its own
        # values (e.g. PAREO_B_542 = "PAREO_B_7") and a different value is an
        # 8541 conflict.
        "distribution_designation": [{"value": "default", "marketplace_id": JP_ID}],
        "is_exclusive_product": [{"value": False, "marketplace_id": JP_ID}],
        "country_of_origin": [{"value": "IN", "marketplace_id": JP_ID}],
        "list_price": [{"value": price, "currency": "JPY", "marketplace_id": JP_ID}],
    }


def pareo_attrs(sku, us, template):
    cfg = PAREOS[sku]
    a = {k: strip_mp(v) for k, v in template.items()
         if k not in ("product_site_launch_date", "model_number", "part_number", "variation_theme",
                      "externally_assigned_product_identifier", "item_name", "color", "pattern")}
    ua = us["attributes"]
    a["item_name"] = jpv(cfg.get("name") or PAREO_NAME.format(color=cfg["color"]))
    a["color"] = jpv(cfg["color"])
    if cfg.get("pattern"):
        a["pattern"] = jpv(cfg["pattern"])
    a["bullet_point"] = a["bullet_point"] + jpv(PAREO_EXTRA_BULLET)
    a["special_size_type"] = jpv("標準")
    a["model_name"] = jpv("ブロックプリント パレオ")
    a["externally_assigned_product_identifier"] = strip_mp(ua["externally_assigned_product_identifier"])
    for k in COPY_FROM_US:
        if k in ua:
            a[k] = strip_mp(ua[k])
    return a


def cover_attrs(sku, us, template):
    cfg = COVERS[sku]
    a = {k: strip_mp(v) for k, v in template.items()
         if k not in ("product_site_launch_date", "model_number", "part_number", "item_name", "color",
                      "externally_assigned_product_identifier", "item_shape", "size", "style", "theme",
                      "pattern", "material", "special_feature", "item_dimensions", "item_length_width")}
    ua = us["attributes"]
    round_ = cfg["shape"] == "円形"
    # The JP catalog already holds an English title for B07SMQ1SYH; a different
    # title is an 8541 conflict, so keep the catalog's own.
    a["item_name"] = jpv(cfg.get("catalog_name") or cfg["name"])
    a["color"] = jpv(cfg["color"])
    a["item_shape"] = jpv(cfg["shape"])
    a["model_number"] = [{"value": sku, "marketplace_id": JP_ID}]
    a["size"] = jpv(cfg["size"])
    a["material"] = jpv("コットン")
    a["fabric_type"] = jpv("コットン100%")
    a["pattern"] = jpv("マンダラ")
    a["style"] = jpv("ボヘミアン")
    a["bullet_point"] = [x for b in COVER_BULLETS for x in jpv(b)]
    a["product_description"] = jpv(COVER_DESCRIPTION)
    a["care_instructions"] = jpv("ドライクリーニング" if not round_ else "洗濯機洗い")
    a["item_length_width"] = [{"length": {"unit": "centimeters", "value": 61.0},
                               "width": {"unit": "centimeters", "value": 61.0}, "marketplace_id": JP_ID}]
    a["item_dimensions"] = [{"length": {"unit": "centimeters", "value": 61.0}, "width": {"unit": "centimeters", "value": 61.0},
                             "height": {"unit": "centimeters", "value": 20.0}, "marketplace_id": JP_ID}]
    ean = {"B07SMQ1SYH": "0750440983585", "B07SLQY5F8": "0750440983578"}[us["summaries"][0]["asin"]]
    a["externally_assigned_product_identifier"] = [{"value": ean, "type": "ean", "marketplace_id": JP_ID}]
    for k in COPY_FROM_US:
        if k in ua:
            a[k] = strip_mp(ua[k])
    return a


def main():
    apply = "--apply" in sys.argv
    only = next((x.split("=", 1)[1].split(",") for x in sys.argv if x.startswith("--only=")), None)
    li_us = ListingsItems(credentials=CRED_US, marketplace=US, timeout=120)
    li_jp = ListingsItems(credentials=CRED_JP, marketplace=JP, timeout=120)
    cat = CatalogItems(credentials=CRED_JP, marketplace=JP, version="2022-04-01")
    tpl = {asin: cat.get_catalog_item(asin, marketplaceIds=JP_ID, includedData="attributes").payload["attributes"]
           for asin in ("B0GY1RHXMG", "B083NR5LPC")}
    results = {}
    for sku in [*PAREOS, *COVERS]:
        if only and sku not in only:
            continue
        us = us_listing(li_us, sku)
        if sku in PAREOS:
            ptype, attrs = "SWIMWEAR", pareo_attrs(sku, us, tpl["B0GY1RHXMG"])
        else:
            ptype, attrs = "DECORATIVE_PILLOW_COVER", cover_attrs(sku, us, tpl["B083NR5LPC"])
        price, offer = offer_attrs(sku, us)
        attrs.update(offer)
        body = {"productType": ptype, "requirements": "LISTING", "attributes": attrs}
        kw = {"marketplaceIds": [JP_ID], "body": body}
        if not apply:
            kw["mode"] = "VALIDATION_PREVIEW"
        r = li_jp.put_listings_item(SELLER_JP, sku, **kw).payload
        issues = [(i.get("code"), i.get("severity"), i.get("attributeNames"), i.get("message", "")[:900]) for i in r.get("issues", [])]
        results[sku] = {"status": r.get("status"), "price": price, "issues": issues}
        print(sku, ptype, f"¥{price}", r.get("status"))
        for i in issues:
            print("   ", i)
        time.sleep(1)
    print(json.dumps({k: v["status"] for k, v in results.items()}))


if __name__ == "__main__":
    main()
