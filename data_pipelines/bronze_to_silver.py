from datetime import datetime
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext


# initialize glue 
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# read from the bronze bucket
cart_header_df = spark.read.parquet("s3://ecommerce-bronze90/bronze_cart_header/").drop("run_date")

cart_items_df = spark.read.parquet("s3://ecommerce-bronze90/bronze_cart_items/").drop("run_date")

products_df = spark.read.parquet("s3://ecommerce-bronze90/bronze_products/").drop("run_date")

product_reviews_df = spark.read.parquet("s3://ecommerce-bronze90/bronze_reviews/").drop("run_date")

users_df = spark.read.parquet("s3://ecommerce-bronze90/bronze_users/").drop("run_date")

user_company = spark.read.parquet("s3://ecommerce-bronze90/bronze_user_companies/").drop("run_date")

user_location = spark.read.parquet("s3://ecommerce-bronze90/bronze_user_locations/").drop("run_date")



# create silver users
def create_silver_user(users, user_company, user_location):

    user_company_clean = user_company.select(
        "user_id",
        col("city").alias("company_city"),
        col("company_name"),
        col("job_title")
    )

    user_location_clean = user_location.select(
        "user_id",
        col("city").alias("home_city"),
        col("country"),
        col("lat"),
        col("lng")
    )


    silver_users = (users.join(user_company_clean, "user_id", "inner")\
                            .join(user_location_clean, "user_id", "inner"))
    
    return silver_users


# create silver products 
def create_silver_product(products, reviews):
    
    product_reviews_agg = (reviews.groupBy("product_id").agg(
        avg("product_rating").alias("avg_rating"),
        count("product_rating").alias("review_count")
    
    ))

    silver_products = (products.join(product_reviews_agg, "product_id", "inner"))

    
    return silver_products



# create silver orders
def create_silver_orders(cart_header, cart_items, products):

    header_clean = cart_header.select(
        "cart_id",
        "user_id",
        "total",
        "discountedTotal",
        "totalProducts",
        "totalQuantity"
    )

    items_clean = cart_items.select(
        "cart_id",
        "user_id",
        "product_id",
        "title",
        "price",
        "quantity",
        "line_total",
        "discounted_total",
        "discount_pct"
    )

    products_clean = products.select(
        "product_id",
        "category",
        "brand"
    )

    silver_orders = (
        header_clean
        .join(items_clean, ["cart_id", "user_id"], "inner")
        .join(products_clean, "product_id", "inner")
    )

    return silver_orders


# call the silver table creation functions
users_silver_df=create_silver_user(users_df, user_company, user_location)
product_silver_df=create_silver_product(products_df, product_reviews_df)
order_silver_df=create_silver_orders(cart_header_df, cart_items_df, products_df)



# write function
def write_to_silver(df, path):
    writer = df.write.mode("append").parquet(path)

    return writer



# write to the silver bucket
write_to_silver(users_silver_df, "s3://ecommerce-silver90/silver_user_profiles/")
write_to_silver(product_silver_df, "s3://ecommerce-silver90/silver_products/")
write_to_silver(order_silver_df, "s3://ecommerce-silver90/silver_orders/")