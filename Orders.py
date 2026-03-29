from sp_api.base import Marketplaces, Credentials
from sp_api.api import Orders
import time
import pytz
from datetime import datetime, timedelta
from sp_api.base import Marketplaces
from time import sleep

mlf_debug=True

def print_if_true(input_string):
    if mlf_debug:
        print(input_string)


def print_sorted_list(item_list):
    result="";
    sorted_list = sorted(item_list, key=lambda x: x['quantity'], reverse=True)
    for item in sorted_list:
        print_if_true(f"{item['sku']} : {item['quantity']}")
        result+=f"<BR>{item['sku']} : {item['quantity']}"
    print_if_true("    ")    
    return result;

def add_item_to_list(item_list, sku, quantity):
    # Check if the SKU already exists in the list
    for item in item_list:
        if item['sku'] == sku:
            item['quantity'] += quantity  # Update the quantity if SKU is found
            break
    else:
        # If SKU is not found, add a new item to the list
        item_list.append({'sku': sku, 'quantity': quantity})

def get_country_code(marketplace):
    # Dictionary mapping Marketplaces to country codes
    marketplace_to_country = {
        Marketplaces.US: "US",
        Marketplaces.CA: "CA",
        Marketplaces.MX: "MX",
        Marketplaces.GB: "GB",
        Marketplaces.DE: "DE",
        Marketplaces.FR: "FR",
        Marketplaces.IT: "IT",
        Marketplaces.ES: "ES",
        Marketplaces.NL: "NL",
        Marketplaces.SE: "SE",
        Marketplaces.BE: "BE",
        Marketplaces.PL: "PL",
        Marketplaces.IN: "IN",
        Marketplaces.SG: "SG",
        Marketplaces.AU: "AU",
        Marketplaces.JP: "JP",
        Marketplaces.AE: "AE",
        Marketplaces.SA: "SA",
        Marketplaces.BR: "BR",
    }

    # Return the country code for the given marketplace
    return marketplace_to_country.get(marketplace, "Unknown Marketplace")



def get_order_mlf(created_after,created_before,credentials,marketplace):
    try:
        result=""
        marketplace_string = get_country_code(marketplace)  # Outputs: DE
        orders_api = Orders(credentials=credentials, marketplace=marketplace)
        max_attempts = 10  # Set a maximum number of attempts to avoid infinite loops
        attempts = 0
        while attempts < max_attempts:
            try:
                res = orders_api.get_orders(CreatedAfter=created_after, CreatedBefore=created_before)
                break  # Exit the loop if successful
            #except QuotaExceededException  as e:
            except Exception as e:
                # Handle the exception (log it, wait, and then retry)
                print(f"An error occurred get_orders: {e}. Retrying...")
                attempts += 1
                time.sleep(5)  # Wait for 1 second before retrying, can adjust based on the scenario

        # print(res.payload)
        orders_list = res.payload.get('Orders', [])
                
        # Loop through each order and print ordered items and quantities
        a=0
        item_list = []
        for order in orders_list:
            amazon_order_id = order.get('AmazonOrderId')
            #print_if_true(f"Order ID: {amazon_order_id}")
                
            # Fetch and print items for the current order
            order_items = get_order_items(orders_api, amazon_order_id)
            for item in order_items:
                print_if_true(f"{item.get('SellerSKU')} {item.get('QuantityOrdered')}")
                #result+=str(item.get('SellerSKU'))+" "+str(item.get('QuantityOrdered'))+"<BR>"
                add_item_to_list(item_list, item.get('SellerSKU'), item.get('QuantityOrdered'))
                a=a+item.get('QuantityOrdered')
            sleep(1)
        print_if_true(marketplace_string+": "+str(a))
        result+="<BR><B>"+marketplace_string+": "+str(a)+"</B>"
        result+=print_sorted_list(item_list)
        result+="<BR>"
        #print("RRR"+result)
        return result

    except Exception as e:
        print("An error occurred get_order_mlf:", e)


# Function to fetch order items
def get_order_items(api, amazon_order_id):
    try:
        response = api.get_order_items(order_id=amazon_order_id)
        return response.payload.get('OrderItems', [])  # Returns a list of order items
    except Exception as e:
        print(f"An error occurred while fetching items for order {amazon_order_id}: ", e)
        return []

import os

def orders_mlf(shift):

    # Load credentials from environment
    credentials_usa = dict(
        refresh_token=os.environ["REFRESH_TOKEN_USA"],
        lwa_app_id=os.environ["CLIENT_ID_USA"],
        lwa_client_secret=os.environ["CLIENT_SECRET_USA"]
    )

    credentials_eu = dict(
        refresh_token=os.environ["REFRESH_TOKEN_EU"],
        lwa_app_id=os.environ["CLIENT_ID_EU"],
        lwa_client_secret=os.environ["CLIENT_SECRET_EU"]


    # Format dates for API
    utc_now = datetime.utcnow()

    pacific_time = pytz.timezone('America/Los_Angeles')
    pdt_time = utc_now.replace(tzinfo=pytz.utc).astimezone(pacific_time)
    yesterday_pdt = pdt_time.date() - timedelta(days=1)

    if shift==1:
        created_after = yesterday_pdt.isoformat()+"T07:00:00.000000Z"
    else:
        created_after = pdt_time.date().isoformat()+"T07:00:00.000000Z"

    if shift==1:
        created_before = pdt_time.date().isoformat()+"T06:59:59.999999Z"
        #created_before = datetime.combine(yesterday_pdt, datetime.max.time()).isoformat()
    else:
        two_minutes_ago = utc_now - timedelta(minutes=2)
        created_before = str(two_minutes_ago.isoformat())+"Z"
    result = "" 
    print("created_after:", created_after)
    print("created_before:", created_before)
    result +="created_after:"+ str(created_after);
    result +="created_before:"+ str(created_before);
    
    result +=str(get_order_mlf(created_after,created_before,credentials_usa,Marketplaces.US));
    result +=str(get_order_mlf(created_after,created_before,credentials_usa,Marketplaces.CA));
    result +=str(get_order_mlf(created_after,created_before,credentials_usa,Marketplaces.MX));

    yesterday_utc = utc_now.date() - timedelta(days=1)
    
    if shift==1:
        created_after = yesterday_utc.isoformat()
    else:
        created_after = utc_now.date().isoformat()

    if shift==1:
        created_before = datetime.combine(yesterday_utc, datetime.max.time()).isoformat()
    else:
        two_minutes_ago = utc_now - timedelta(minutes=2)
        created_before = str(two_minutes_ago.isoformat())+"Z"

    print("created_after:", created_after)
    print("created_before:", created_before)
    result +="created_after:"+ str(created_after);
    result +="created_before:"+ str(created_before);
 

    result +=str(get_order_mlf(created_after,created_before,credentials_eu,Marketplaces.DE));
    result +=str(get_order_mlf(created_after,created_before,credentials_eu,Marketplaces.FR));
    result +=str(get_order_mlf(created_after,created_before,credentials_eu,Marketplaces.IT));
    result +=str(get_order_mlf(created_after,created_before,credentials_eu,Marketplaces.ES));

    result +=str(get_order_mlf(created_after,created_before,credentials_eu,Marketplaces.NL));
    result +=str(get_order_mlf(created_after,created_before,credentials_eu,Marketplaces.BE));
    result +=str(get_order_mlf(created_after,created_before,credentials_eu,Marketplaces.PL));

    #print("RESULT    "+result)
    return result

if __name__ == '__main__':
    result = orders_mlf(0)
    print("Result"+result)    
