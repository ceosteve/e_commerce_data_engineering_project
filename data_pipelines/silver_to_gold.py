from datetime import datetime
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext


# initialize glue 
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session




# read from the silver bucket
orders_df = spark.read.parquet("s3://ecommerce-silver90/silver_orders/")
products_df = spark.read.parquet("s3://ecommerce-silver90/silver_products/")
users_df = spark.read.parquet("s3://ecommerce-silver90/silver_user_profiles/")



# create dim users
def create_dim_users(users):
    return users.select(
        "user_id",
        concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
        "age",
        "gender",
        "role",
        "company_name",
        "job_title",
        "home_city",
        "country",
        "lat",
        "lng"
        )



# create dim products
def create_dim_products(products):
    return products.select(
        "product_id",
        "title",
        "category",
        "brand",
        "price",
        "quantity_in_stock",
        "avg_rating",
        "review_count",
        "product_sku"
    )


# create fact orders
def create_fact_orders(orders):
    return orders.select(
        "cart_id", 
        "user_id",
        "product_id",
        "totalproducts",
        "totalquantity",
        "title",
        "category",
        "brand",
        "quantity",
        "price",
        "discount_pct",
        "discounted_total",
        "discountedtotal",
        "line_total",
        "total",
            
    )



# call the transformation functions
dim_users = create_dim_users(users_df)
dim_products = create_dim_products(products_df)
fact_orders = create_fact_orders(orders_df)



# write to gold bucket
def write_to_gold(df, path):
    return df.write.mode("overwrite").parquet(path)


# save the tables to the gold bucket
write_to_gold(dim_users,"s3://ecommerce-gold90/dim_users/")
write_to_gold(dim_products,"s3://ecommerce-gold90/dim_products/")
write_to_gold(fact_orders, "s3://ecommmerce-gold90/fact_orders/")