"""Every Amazon order line (per the user 2026-11-01: "store all orders, so we
know exactly how items were sold").

Source: the same XML orders report (GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL)
the daily sales update already downloads - UpdateSkuSalesMonth calls
write_order_items() for each region report it parses. One row per order item in
PocketBase `order_items`, delete-then-insert per (region, year, month), so a
re-run of a month is safe. Month = America/Los_Angeles purchase month, the same
convention as sku_sales.

Money is in the order's currency: principal = item price x qty BEFORE
promotions (Amazon's Principal component), promo_discount = coupons/promotions
on the item, net_amount = principal - promo_discount (= what the buyer paid
for the item, excl. tax and shipping). Cancelled lines are kept (qty 0,
order_status Cancelled). MCF orders (Etsy etc.) are kept as marketplace "mcf"
- exclude them for Amazon sales. No buyer name/city/postcode is stored, only
state and country."""
import xml.etree.ElementTree as ET

from MlfReport import DB_MARKETPLACE_MAP
from UpdateSkuSalesMonth import (
    POCKETBASE_BATCH_SIZE,
    extract_amzn_gr_value,
    parse_input_datetime,
    parse_item_promotions,
    pb_authenticate,
    pb_batch,
    safe_strip,
)
from AsinAnalytics import pb_list_all

COLLECTION = "order_items"


def _money(order_item, path):
    """-> {type: (amount, currency)} for ItemPrice / ShippingPrice-style component lists."""
    out = {}
    for comp in order_item.findall(f"{path}/Component"):
        amount_el = comp.find("Amount")
        if amount_el is None:
            continue
        try:
            amount = float(safe_strip(amount_el.text) or 0)
        except ValueError:
            continue
        kind = safe_strip(comp.findtext("Type"))
        prev = out.get(kind, (0.0, ""))
        out[kind] = (prev[0] + amount, amount_el.get("currency") or prev[1])
    return out


def parse_order_items(xml_payload, region):
    root = ET.fromstring(xml_payload)
    rows = []
    for order in root.findall(".//Order"):
        channel = safe_strip(order.findtext("SalesChannel"))
        low = channel.lower()
        if low.startswith("non-amazon"):
            marketplace = "mcf"
        else:
            marketplace = DB_MARKETPLACE_MAP.get(low)
            if not marketplace:
                continue
        purchase_raw = safe_strip(order.findtext("PurchaseDate"))
        try:
            la = parse_input_datetime(purchase_raw)
        except (ValueError, TypeError):
            continue
        ful = order.find("FulfillmentData")
        addr = ful.find("Address") if ful is not None else None
        base = {
            "order_id": safe_strip(order.findtext("AmazonOrderID")),
            "purchase_at": purchase_raw,
            "purchase_date": la.strftime("%Y-%m-%d"),
            "year": la.year,
            "month": la.month,
            "region": region,
            "marketplace": marketplace,
            "sales_channel": channel,
            "order_status": safe_strip(order.findtext("OrderStatus")),
            "fulfillment_channel": safe_strip(ful.findtext("FulfillmentChannel")) if ful is not None else "",
            "ship_service": safe_strip(ful.findtext("ShipServiceLevel")) if ful is not None else "",
            "ship_state": safe_strip(addr.findtext("State")) if addr is not None else "",
            "ship_country": safe_strip(addr.findtext("Country")) if addr is not None else "",
            "is_business": safe_strip(order.findtext("IsBusinessOrder")).lower() == "true",
        }
        for item in order.findall("OrderItem"):
            sku = extract_amzn_gr_value(safe_strip(item.findtext("SKU")))
            if not sku:
                continue
            try:
                qty = int(float(safe_strip(item.findtext("Quantity")) or 0))
            except ValueError:
                qty = 0
            price = _money(item, "ItemPrice")
            principal, cur = price.get("Principal", (0.0, ""))
            promos = parse_item_promotions(item)
            discount = round(sum(d for _p, d in promos), 2)
            rows.append({
                **base,
                "order_item_id": safe_strip(item.findtext("AmazonOrderItemCode")),
                "item_status": safe_strip(item.findtext("ItemStatus")),
                "sku": sku,
                "asin": safe_strip(item.findtext("ASIN")),
                "qty": qty,
                "currency": cur or next((c for _a, c in price.values() if c), ""),
                "principal": round(principal, 2),
                "tax": round(price.get("Tax", (0.0, ""))[0], 2),
                "shipping": round(price.get("Shipping", (0.0, ""))[0], 2),
                "promo_discount": discount,
                "promo_ids": ",".join(sorted({p for p, _d in promos})),
                "net_amount": round(principal - discount, 2),
            })
    return rows


def write_order_items(rows, region, year, month):
    """Delete-then-insert the (region, year, month) slice. Rows of other months
    in the same report (a report window crossing a month edge) are ignored."""
    rows = [r for r in rows if r["year"] == year and r["month"] == month and r["region"] == region]
    token = pb_authenticate()
    old = pb_list_all(token, COLLECTION, f'region = "{region}" && year = {year} && month = {month}', fields="id")
    ops = [{"method": "DELETE", "url": f"/api/collections/{COLLECTION}/records/{r['id']}"} for r in old]
    ops += [{"method": "POST", "url": f"/api/collections/{COLLECTION}/records", "body": r} for r in rows]
    for i in range(0, len(ops), POCKETBASE_BATCH_SIZE):
        pb_batch(token, ops[i:i + POCKETBASE_BATCH_SIZE])
    return {"deleted": len(old), "inserted": len(rows)}
