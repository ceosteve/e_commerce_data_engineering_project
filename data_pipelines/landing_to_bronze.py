from datetime import datetime
from pyspark.sql.functions import *
from utils import boto3, s3_client, glue_client, athena_client, get_spark

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


# Getting products information from products folder s3 bucket
def get_products(products_infor):
   bronze_products = products_infor.select(
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
      col("meta.Barcode").cast("string").alias("bar_code"),
      col("meta.qrCode").cast("string").alias("qr_code"),
      col("images").cast("string").alias("product_image"),
      col("thumbnail").cast("string").alias("product_thumbnail")      
      
   )

    # Load timestamp partition
    load_date = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write to S3 (Bronze layer)
    bronze_products.write \
        .mode("append") \
        .parquet(
            f"s3a://ecomerce-bronze123/bronze_products/load_date={load_date}/"
        )

# Fetching product reviews 
def product_reviews(product_reviews):
   product_info = product_reviews.select(
      col("id").cast("int").alias("product_id"),
      explode("reviews").alias("reviews")
   )

   bronze_reviews = product_info.select(
      col("product_id").cast("int"),
      col("reviews.rating").cast("int").alias("product_rating"),
      col("reviews.comment").cast("string").alias("comment"),
      col("reviews.date").cast("timestamp").alias("review_date"),
      col("reviews.reviewerName").cast("string").alias("reviewer_name"),
      col("reviews.reviewerEmail").cast("string").alias("reviewer_email")
   )

    # Load timestamp partition
    load_date = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write to S3 (Bronze layer)
    bronze_reviews.write \
        .mode("append") \
        .parquet(
            f"s3a://ecomerce-bronze123/bronze_product_reviews/load_date={load_date}/"
        )
   

def user_info(users_infor):

    bronze_users = users_infor.select(

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
        col("crypto.network").alias("crypto_network")

    )

        # Load timestamp partition
    load_date = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write to S3 (Bronze layer)
    bronze_users.write \
        .mode("append") \
        .parquet(
            f"s3a://ecomerce-bronze123/bronze_user_infor/load_date={load_date}/"
        )

def company_info(user_company):

    bronze_company = user_company.select(
        col("id").cast("int").alias("user_id"),
        # company identity
        col("company.name").alias("company_name"),
        col("company.department").alias("department"),
        col("company.title").alias("job_title"),
        # company address
        col("company.address.address").alias("company_address"),
        col("company.address.city").alias("company_city"),
        col("company.address.state").alias("company_state"),
        col("company.address.stateCode").alias("company_state_code"),
        col("company.address.postalCode").alias("company_postal_code"),
        col("company.address.country").alias("company_country"),
        # coordinates
        col("company.address.coordinates.lat").cast("double").alias("company_lat"),
        col("company.address.coordinates.lng").cast("double").alias("company_lng")
    )

        # Load timestamp partition
    load_date = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write to S3 (Bronze layer)
    bronze_company.write \
        .mode("append") \
        .parquet(
            f"s3a://ecomerce-bronze123/bronze_user_company/load_date={load_date}/"
        )

def location_info(user_location):

    bronze_location = user_location.select(
        col("id").cast("int").alias("user_id"),
        col("address.address").alias("address_line"),
        col("address.city").alias("city"),
        col("address.state").alias("state"),
        col("address.stateCode").alias("state_code"),
        col("address.postalCode").alias("postal_code"),
        col("address.country").alias("country"),
        col("address.coordinates.lat").cast("double").alias("lat"),
        col("address.coordinates.lng").cast("double").alias("lng")
    )

        # Load timestamp partition
    load_date = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write to S3 (Bronze layer)
    bronze_location.write \
        .mode("append") \
        .parquet(
            f"s3a://ecomerce-bronze123/bronze_user_location/load_date={load_date}/"
        )

cart_header_create(carts_df)
carts_line_items(carts_df)








