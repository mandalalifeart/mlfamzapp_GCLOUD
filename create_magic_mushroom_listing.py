"""One-off script: creates the new standalone Etsy listing for the Magic
Mushroom velvet pouf cover (SKU CoverPoufVMagic535), per the user's request
2026-09-04. Template values (shipping/category/materials/policies) copied
from the real, live sibling listing 1820250173 - see EtsyListingCreate.py.
Creates as a DRAFT (private, reviewable, not yet public) and uploads all 6
product photos the user sent via Telegram."""
import sys

sys.path.insert(0, ".")

from EtsyListingCreate import create_draft_listing, get_etsy_access_token, set_listing_sku_and_price, upload_listing_image

SKU = "CoverPoufVMagic535"
PRICE = 89.00
QUANTITY = 20

TITLE = "Magic Mushroom Velvet Pouf Cover - Embroidered Floral Round Ottoman, Boho Home Decor"

DESCRIPTION = """COVER ONLY - insert not included
Size: 24 x 8 in (60 x 20 cm)
100% Cotton Velvet with embroidered floral & mushroom design
Backside zipper for easy on/off
Machine washable
Handmade in India

A whimsical garden of embroidered wildflowers, butterflies, and magic mushrooms on soft sage-green velvet - this pouf cover adds a playful cottagecore charm to any room.

LUXURIOUS VELVET FABRIC - Crafted from premium velvet, this pouf cover offers unmatched softness and a rich, luxurious texture that enhances any decor.

BOHO CHIC MEETS COTTAGECORE - A bohemian vibe with a whimsical, storybook edge. Perfect in living rooms, bedrooms, or reading nooks, this floor cushion adds a creative, useful, and luxurious accent to any space.

FITS YOUR EXISTING INSERT - This listing is for the cover only; fits a standard 24in x 8in (60cm x 20cm) round pouf insert. Zipper closure on the underside for easy filling and cleaning."""

TAGS = [
    "velvet floor pillow",
    "round pouf",
    "home decor",
    "room decor",
    "floor cushion cover",
    "meditation cushion",
    "mushroom decor",
    "embroidered pouf",
    "boho pouf cover",
    "cottagecore decor",
    "floral embroidery",
    "pouf ottoman cover",
    "bohemian accent",
]

IMAGE_DIR = "/home/bababot/.claude/channels/telegram/inbox"
IMAGE_FILES = [
    "1788529904040-AQADwhFrG_-00FB-.jpg",  # hero shot
    "1788529904547-AQADxBFrG_-00FB-.jpg",  # spec/infographic lifestyle
    "1788529904804-AQADxRFrG_-00FB-.jpg",  # lifestyle scene
    "1788529905043-AQADxhFrG_-00FB-.jpg",  # top view
    "1788529905240-AQADxxFrG_-00FB-.jpg",  # side angle
    "1788529905535-AQADyBFrG_-00FB-.jpg",  # embroidery macro detail
]


def main():
    shop_id, access_token = get_etsy_access_token()
    print("Authenticated. shop_id:", shop_id)

    listing = create_draft_listing(shop_id, access_token, {
        "title": TITLE,
        "description": DESCRIPTION,
        "price": PRICE,
        "quantity": QUANTITY,
        "tags": TAGS,
    })
    listing_id = listing["listing_id"]
    print("Created draft listing_id:", listing_id, "state:", listing.get("state"))

    set_listing_sku_and_price(shop_id, listing_id, access_token, SKU, PRICE, QUANTITY)
    print("Set SKU/price/quantity on inventory.")

    for rank, filename in enumerate(IMAGE_FILES, start=1):
        path = f"{IMAGE_DIR}/{filename}"
        result = upload_listing_image(shop_id, listing_id, access_token, path, rank)
        print(f"Uploaded image {rank}: {filename} -> listing_image_id {result.get('listing_image_id')}")

    print()
    print("DONE. Edit URL:", f"https://www.etsy.com/your/shops/me/listing-editor/edit/{listing_id}")


if __name__ == "__main__":
    main()
