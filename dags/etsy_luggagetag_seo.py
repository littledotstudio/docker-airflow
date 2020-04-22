from bs4 import BeautifulSoup
import urllib.request
from fake_useragent import UserAgent
import csv
import time
import pandas as pd
import requests
import re
import numpy as np

import gspread
from oauth2client.service_account import ServiceAccountCredentials


import gspread_dataframe as gd
from gspread_dataframe import get_as_dataframe, set_with_dataframe

from datetime import date

today = date.today()

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
# use creds to create a client to interact with the Google Drive API
#scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

def get_query_df(query):

    query = query
    etsy_url = "https://www.etsy.com/search/?q=" + query
    ua = UserAgent()

    etsy_page_search = requests.get(etsy_url, {"User-Agent": ua.random})
    soup_search = BeautifulSoup(etsy_page_search.content,"html5lib")
    #This is the listing id list
    listing_id = soup_search.find_all("a")
    #This holds the listing url
    list_id_records = []
    title_records = []
    tags = []
    descriptions = []
    shop = []

    #this gather listing url by listing id and adding to website address
    for listing in listing_id:
        list_id = (listing.get("data-listing-id"))
        title_id = (listing.get("title"))
        if list_id != None and title_id != None:
            url_product = "http://www.etsy.com/listing/" + str(list_id) +"/"
            list_id_records.append(url_product)
            title_records.append(title_id)

    for i in range(0, len(list_id_records)):

        etsy_page_product = requests.get(list_id_records[i], {"User-Agent": ua.random})
        soup_product = BeautifulSoup(etsy_page_product.content,"html.parser")

        # Descriptions
        temp_description = soup_product.find_all("meta")[15]
        temp_description2 = re.search(r'meta content="(.*?)" name=', str(temp_description)).group(1)
        descriptions.append(temp_description2)

        # Shop Owner
        temp_shop = soup_product.find_all("meta")[24]
        temp_shop2 = re.search(r'meta content="(.*?)" property=', str(temp_shop)).group(1)
        shop.append(temp_shop2)

        keywords_list = soup_product.find_all("a", {"class":"btn btn-secondary"})

        temp_tags = []
        for i in range(0, len(keywords_list)):
            temp_tags2 = re.search(r'blank">(.*?)</a>', str(keywords_list[i])).group(1)
            temp_tags.append(temp_tags2)

        temp_tags = [x for x in temp_tags if "&" not in x ]
        #temp_tags = " ".join(word for word in temp_tags).lower()
        tags.append(temp_tags)
        #time.sleep(1)

    shop = [x.strip("https://www.etsy.com/shop/") for x in shop]
    df = pd.DataFrame({'list_id': list_id_records, 
                       'title': title_records, 
                       'tags': tags,
                       'descriptions': descriptions,
                       'shop': shop})
    return(df)

luggage_search = ["luggage tag", "business trip",  "travel tag", "personal tag"]

df = pd.DataFrame()

for i in range(0, len(luggage_search)):
    print("Getting data for tag: ", luggage_search[i])
    df_temp = get_query_df(luggage_search[i])
    df_temp['date'] = today
    df_temp['rank'] = np.arange(len(df_temp))
    df_temp['tag'] = luggage_search[i]
    df = df.append(df_temp)


# Connecting with `gspread` here
sheet = client.open("etsydata").get_worksheet(2)
existing = gd.get_as_dataframe(sheet)
n = existing.list_id.count()
set_with_dataframe(sheet, df, row=n+2, include_column_header=False)
