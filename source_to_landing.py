import boto3
import requests as r 
import json
from datetime import datetime
import os
from .utils import s3_client

load_dotenv()

base_url = 'https://dummyjson.com'
# different end points to fetch data from
endpoints = ['products', 'users', 'carts']
# list of buckets to create in s3
buckets = ['ecomerce-landing123', 'ecomerce-bronze123', 'ecomerce-silver123', 'ecomerce-gold321', 'athena-query-results-w3']


# create an s3 client to make api calls to s3 bucket in aws and create bucket

def create_s3_bucket():
    for bucket in buckets:
        try:
            s3_client.create_bucket(Bucket=bucket)
            print(f"Created: {bucket}")
        except Exception as e:
            print(f"Error creating {bucket}: {e}")

def product_data_from_api(url, bucket_name="ecomerce-landing123"):
    """
    Fetch products from DummyJSON API and store each product as a JSON file in S3.
    """

    response = r.get(f"{url}/products")

    if response.status_code != 200:
        raise Exception("Failed to fetch products")

    products = response.json()["products"]

    for product in products:
        product_id = product["id"]

        s3_key = f"products/product_id={product_id}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(product),
            ContentType="application/json"
        )

    return(f"Uploaded {len(products)}")


def user_data_from_api(url, bucket_name="ecomerce-landing123"):
    """
    Fetch users from DummyJSON API and store each product as a JSON file in S3.
    """

    response = r.get(f"{url}/users")

    if response.status_code != 200:
        raise Exception("Failed to fetch products")

    users = response.json()["users"]

    for user in users:
        user_id = user["id"]

        s3_key = f"users/user_id={user_id}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(user),
            ContentType="application/json"
        )

    return(f"Uploaded {len(user)}")

def cart_data_from_api(url, bucket_name="ecomerce-landing123"):
    """
    Fetch carts from DummyJSON API and store each cart as a JSON file in S3.
    """

    response = r.get(f"{url}/carts")

    if response.status_code != 200:
        raise Exception("Failed to fetch carts")

    carts = response.json()["carts"]

    for cart in carts:
        cart_id = cart["id"]

        s3_key = f"carts/cart_id={cart_id}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(cart),
            ContentType="application/json"
        )

    return(f"Uploaded {len(cart)}")



create_s3_bucket()
product_data_from_api(base_url)
user_data_from_api(base_url)
cart_data_from_api(base_url)