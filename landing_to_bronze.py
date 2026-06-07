import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import s3_client, glue_client, athena_client, get_spark

spark = get_spark()

#Read data from landing folder in aws s3
carts_df = spark.read.json('s3a://ecomerce-landing123/carts/')
products_df = spark.read.json('s3a://ecomerce-landing123/products/')
users_df = spark.read.json('s3a://ecomerce-landing123/users/')

# Getting headers for carts
def cart_header_create(carts_header):
  bronze_cart_header = carts_header.select(
      col("id").cast("int").alias("cart_id"),
      col("userId").cast("int").alias("user_id"),
      col("total").cast("double"),
      col("discountedTotal").cast("double"),
      col("totalProducts").cast("int"),
      col("totalQuantity").cast("int")
  )
  load_date = datetime.now().strftime("%Y%m%d_%H%M%S")

  bronze_cart_header.write.mode("append").parquet(
      f"s3a://ecomerce-bronze123/bronze_cart_header/load_date={load_date}/"
  )


# Getting line items for carts
def carts_line_items(carts):

    # Flatten carts into individual products
    cart_items = carts.select(
        col("id").cast("int").alias("cart_id"),
        col("userId").cast("int").alias("user_id"),
        explode("products").alias("product")
    )

    # Select required fields from exploded structure
    bronze_cart_items = cart_items.select(
        col("cart_id").cast("int"),
        col("user_id").cast("int"),
        col("product.id").cast("int").alias("product_id"),
        col("product.title").cast("string").alias("title"),
        col("product.price").cast("double").alias("price"),
        col("product.quantity").cast("int").alias("quantity"),
        col("product.total").cast("double").alias("line_total"),
        col("product.discountedTotal").cast("double").alias("discounted_total"),
        col("product.discountPercentage").cast("double").alias("discount_pct")
    )

    # Load timestamp partition
    load_date = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write to S3 (Bronze layer)
    bronze_cart_items.write \
        .mode("append") \
        .parquet(
            f"s3a://ecomerce-bronze123/bronze_cart_items/load_date={load_date}/"
        )




cart_header_create(carts_df)
carts_line_items(carts_df)








