from datetime import datetime
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext



# initialize glue 

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

RUN_DATE = datetime.now().strftime("%Y%m%d")


# read from the landing bucket 
carts_df = spark.read.json("s3://ecommerce-landing90/carts/")
products_df = spark.read.json("s3://ecommerce-landing90/products/")
users_df = spark.read.json("s3://ecommerce-landing90/users/")


print("CARTS:", carts_df.count())
print("PRODUCTS:", products_df.count())
print("USERS:", users_df.count())

def cart_header(df):
     return df.select(
        col("id").cast("int").alias("cart_id"),
        col("userId").cast("int").alias("user_id"),
        col("total").cast("double"),
        col("discountedTotal").cast("double"),
        col("totalProducts").cast("int"),
        col("totalQuantity").cast("int"),
    )


def carts_line_items(df):
    exploded = df.select(
        col("id").cast("int").alias("cart_id"),
        col("userId").cast("int").alias("user_id"),
        explode("products").alias("p")
    )

    return exploded.select(
        col("cart_id"),
        col("user_id"),
        col("p.id").cast("int").alias("product_id"),
        col("p.title").alias("title"),
        col("p.price").cast("double").alias("price"),
        col("p.quantity").cast("int").alias("quantity"),
        col("p.total").cast("double").alias("line_total"),
        col("p.discountedTotal").cast("double").alias("discounted_total"),
        col("p.discountPercentage").cast("double").alias("discount_pct"),
    )
                                   

def get_products(df):
    return df.select(
        col("id").cast("int").alias("product_id"),
        col("title").cast("string").alias("title"),
        col("category").cast("string").alias("category"),
        col("price").cast("double").alias("price"),
        col("discountPercentage").cast("double").alias("discount_percentage"),
        col("rating").cast("double").alias("product_rating"),
        col("stock").cast("int").alias("quantity_in_stock"),
        col("tags")[0].cast("string").alias("tag_1"),
        col("tags")[1].cast("string").alias("tag_2"),
        col("brand").cast("string").alias("brand"),
        col("sku").cast("string").alias("product_sku"),
        col("weight").cast("double").alias("weight"),
        col("dimensions.width").cast("double").alias("product_width"),
        col("dimensions.height").cast("double").alias("product_height"),
        col("dimensions.depth").cast("double").alias("product_depth"),
        col("warrantyInformation").cast("string").alias("warranty_information"),
        col("shippingInformation").cast("string").alias("shipping_information"),
        col("availabilityStatus").cast("string").alias("availability_status"),
        col("returnPolicy").cast("string").alias("return_policy"),
        col("minimumOrderQuantity").cast("int").alias("minimum_order_quantity"),
        col("meta.createdAt").cast("timestamp").alias("created_at"),
        col("meta.updatedAt").cast("timestamp").alias("updated_at"),
        col("meta.barcode").cast("string").alias("bar_code"),
        col("meta.qrCode").cast("string").alias("qr_code"),
        col("images").cast("string").alias("product_image"),
        col("thumbnail").cast("string").alias("product_thumbnail"),
    )

                                   
def product_reviews(df):
    exploded = df.select(
        col("id").cast("int").alias("product_id"),
        explode("reviews").alias("r")
    )

    return exploded.select(
        col("product_id"),
        col("r.rating").cast("int").alias("product_rating"),
        col("r.comment").alias("comment"),
        col("r.date").cast("timestamp").alias("review_date"),
        col("r.reviewerName").alias("reviewer_name"),
        col("r.reviewerEmail").alias("reviewer_email"),
    )

                                   
def user_info(df):
    return df.select(
        col("id").cast("int").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("maidenName").alias("maiden_name"),
        col("age").cast("int"),
        col("gender"),
        col("username").alias("user_name"),
        col("role"),
        col("birthDate").cast("date").alias("birth_date"),
        col("image"),
        sha2(col("email"), 256).alias("email_address"),
        sha2(col("phone"), 256).alias("phone_number"),
        sha2(col("ip"), 256).alias("ip_address"),
        sha2(col("macAddress"), 256).alias("mac_address"),
        sha2(col("ssn"), 256).alias("ssn"),
        sha2(col("ein"), 256).alias("ein"),
        sha2(col("userAgent"), 256).alias("user_agent"),
        col("bloodGroup").alias("blood_group"),
        col("height").cast("double"),
        col("weight").cast("double"),
        col("eyeColor").alias("eye_color"),
        col("hair.color").alias("hair_color"),
        col("hair.type").alias("hair_type"),
        col("university"),
        col("crypto.coin").alias("crypto_coin"),
        sha2(col("crypto.wallet"), 256).alias("crypto_wallet_hash"),
        col("crypto.network").alias("crypto_network"),
    )

                                   
def user_company_info(df):
    return df.select(
        col("id").alias("user_id"),
        col("company.name").alias("company_name"),
        col("company.title").alias("job_title"),
        col("company.address.city").alias("city"),
    )
                                  
                                   
def user_location_info(df):
    return df.select(
        col("id").alias("user_id"),
        col("address.city").alias("city"),
        col("address.country").alias("country"),
        col("address.coordinates.lat").cast("double").alias("lat"),
        col("address.coordinates.lng").cast("double").alias("lng"),
    )
                                   
# write function
def write_to_s3(df, path):
    writer = df.write.mode("append").parquet(path)


    return writer



# Execute Transformation                                
cart_header_df = cart_header(carts_df)
cart_items_df = carts_line_items(carts_df)
products_df_clean = get_products(products_df)
reviews_df = product_reviews(products_df)
users_df_clean = user_info(users_df)
users_company_df = user_company_info(users_df)
users_location_df = user_location_info(users_df)

                                   
                                   
                                   
write_to_s3(
    cart_header_df,
    f"s3://ecommerce-bronze90/bronze_cart_header/run_date={RUN_DATE}/"
)

write_to_s3(
    cart_items_df,
    f"s3://ecommerce-bronze90/bronze_cart_items/run_date={RUN_DATE}/"
)

write_to_s3(
    products_df_clean,
    f"s3://ecommerce-bronze90/bronze_products/run_date={RUN_DATE}/"
)

write_to_s3(
    reviews_df,
    f"s3://ecommerce-bronze90/bronze_reviews/run_date={RUN_DATE}/"
)

write_to_s3(
    users_df_clean,
    f"s3://ecommerce-bronze90/bronze_users/run_date={RUN_DATE}/"
)

write_to_s3(
    users_company_df,
    f"s3://ecommerce-bronze90/bronze_user_companies/run_date={RUN_DATE}/"
)

write_to_s3(
    users_location_df,
    f"s3://ecommerce-bronze90/bronze_user_locations/run_date={RUN_DATE}/"
)



