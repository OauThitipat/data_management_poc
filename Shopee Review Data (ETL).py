# Databricks notebook source
# MAGIC %md
# MAGIC เป็นโค้ด Python ที่ใช้สำหรับดึงรีวิวสินค้าจาก Shopee โดยใช้ API ของ Shopee โดยระบุข้อมูลต่างๆ เช่น item ID, shop ID, offset, limit, filter และ flag เพื่อให้ API ส่งข้อมูลรีวิวมาให้โดยตรง และจัดเก็บข้อมูลรีวิวทั้งหมดไว้ในลิสต์ all_reviews เมื่อดึงข้อมูลรีวิวได้ทั้งหมดแล้ว โค้ดจะแสดงผลลัพธ์ออกทางหน้าจอโดยแสดงจำนวนรีวิวทั้งหมดและลิสต์ของรีวิวทั้งหมดออกมา

# COMMAND ----------

# DBTITLE 1,Install Required libralies
!pip install google-auth
!pip install pandas-gbq

# COMMAND ----------

# DBTITLE 1,Extract Shopee Reviews Data via API
import requests

# Set the API endpoint and parameters
url = "https://shopee.co.th/api/v2/item/get_ratings"
params = {
    "itemid": 20440963289,  # Replace with the actual item ID
    "shopid": 301786571,  # Replace with the actual shop ID
    "offset": 0, # Set the starting index of the reviews to retrieve
    "limit": 30,  # Set the limit to the desired number of reviews per request (50 in this case)
    "filter": 0, # Set the filter for the reviews (0: all, 1: with images)
    "flag": 1, # Set the flag to 1 to include the buyer's username in the reviews
}

# Send a GET request to the API endpoint with the parameters
response = requests.get(url, params=params)

# Parse the JSON response and extract the total number of reviews
data = response.json()
total_reviews = data["data"]["item_rating_summary"]["rating_total"]

# Loop through the reviews until all reviews are retrieved
all_reviews = []
while len(all_reviews) < total_reviews:
    # Send a GET request to the API endpoint with the parameters
    response = requests.get(url, params=params)

    # Parse the JSON response and extract the reviews
    data = response.json()
    reviews = data["data"]["ratings"]

    # Add the reviews to the list of all reviews
    all_reviews.extend(reviews)

    # Increment the offset parameter to get the next subset of reviews
    params["offset"] += params["limit"]

# Print the total number of reviews and the list of all reviews
print("Total reviews:", len(all_reviews))
# print(all_reviews)

# COMMAND ----------

# DBTITLE 1,Transform Shopee Reviews Data using Pandas
import pandas as pd 
import json
import numpy as np

df = pd.DataFrame(all_reviews)

df2 = df[['orderid','userid','comment','rating_star','cmtid']]


df2['itemid'] = df['product_items'].apply(lambda x: x[0]['itemid'])
df2['name'] = df['product_items'].apply(lambda x: x[0]['name'])
df2['modelid'] = df['product_items'].apply(lambda x: x[0]['modelid'])
df2['options_color'] = df['product_items'].apply(lambda x: x[0]['options']).apply(lambda x: tuple(x)[0])
df2['options_size']  = df['product_items'].apply(lambda x: x[0]['options']).apply(lambda x: tuple(x)[1])

df2['product_image'] = df['videos'].apply(lambda x: x[0]['cover'] if x and len(x) > 0 else np.nan)
df2['product_video'] = df['videos'].apply(lambda x: x[0]['url'] if x and len(x) > 0 else np.nan)

df2 = df2.replace('',np.nan)

# COMMAND ----------

# DBTITLE 1,Load Shopee Reviews Data into BigQuery
from google.oauth2 import service_account

service_account_key_path = "/dbfs/FileStore/data_engineering_poc_383207_a6473f740fee.json"
# Set up credentials to authenticate with BigQuery
credentials = service_account.Credentials.from_service_account_file(service_account_key_path)

# Upload the dataframe to a BigQuery table
table_id = 'data-engineering-poc-383207.shopee_dataset.iphone_data'
df2.to_gbq(destination_table=table_id, project_id='data-engineering-poc-383207', credentials=credentials, if_exists='replace')

