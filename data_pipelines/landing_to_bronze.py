from datetime import datetime
from pyspark.sql.functions import *
from utils import boto3, s3_client, glue_client, athena_client, get_spark


# InIT Spark
spark = get_spark()
spark.conf.set("spark.sql.shuffle.partitions", "50")


# compress parquet files
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")


#Read data from landing buckets in aws s3
carts_df = spark.read.json('s3a://ecomerce-landing123/carts/').cache()
products_df = spark.read.json('s3a://ecomerce-landing123/products/').cache()
users_df = spark.read.json('s3a://ecomerce-landing123/users/').cache()


# materialize cache once
users_df.count()
carts_df.count()
products_df.count()


# Getting headers for carts
def cart_header_create(carts):
  """
  get cart header columns from s3 and write to bronze with versioning
  """
  bronze_cart_header = carts.select(
      col("id").cast("int").alias("cart_id"),
      col("userId").cast("int").alias("user_id"),
      col("total").cast("double"),
      col("discountedTotal").cast("double"),
      col("totalProducts").cast("int"),
      col("totalQuantity").cast("int")
  )



# Getting line items for carts
def carts_line_items(carts):
    """
    get cart items columns from s3 and write to bronze with versioning
    """
    # Flatten carts into individual products
    cart_items = carts.select(
        col("id").cast("int").alias("cart_id"),
        col("userId").cast("int").alias("user_id"),
        explode("products").alias("product")
    ).cache()

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




# Getting products information from products folder s3 bucket
def get_products(products_infor):
   """
    get product columns from s3 and write to bronze with versioning
    """
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



# Fetching product reviews 
def product_reviews(product_reviews):
   """
    get product review columns from s3 and write to bronze with versioning
    """
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



# user info
def user_info(users_infor):
    """
    get user columns from s3 and write to bronze with versioning
    """
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



# extract user company info
def user_company_info(user_company):
    """
    get user company columns from s3 and write to bronze with versioning
    """
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



# extract user location info
def user_location_info(user_location):
    """
    get user location columns from s3 and write to bronze with versioning
    """
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

 
#  write to s3
def write_to_s3(df, path, partition_col=None, coalesce_n=4):
    writer = df.coalesce(coalesce_n).write.mode("append").format("parquet")

    if partition_col:
        writer = writer.partitionBy(partition_col)

    writer.parquet(path)



# run the pipeline 

cart_header_df=cart_header_create(carts_df)
cart_items_df=carts_line_items(carts_df)
products_clean=get_products(products_df)
reviews_df=product_reviews(products_df)
users_clean=user_info(users_df)
users_company=user_company_info(users_df)
users_location=user_location_info(users_df)


# write to s3
write_to_s3(
    cart_header_df,
    f"s3a://ecommerce-bronze90/bronze_cart_header/",
    partition_col="cart_id"
)

write_to_s3(
    cart_items_df,
    f"s3a://ecommerce-bronze90/bronze_cart_items/",
    partition_col="cart_id"
)

write_to_s3(
    products_clean,
    f"s3a://ecommerce-bronze90/bronze_products/",
    partition_col="product_id"
)

write_to_s3(
    reviews_df,
    f"s3a://ecommerce-bronze90/bronze_product_reviews/",
    partition_col="product_id"
)

write_to_s3(
    users_clean,
    f"s3a://ecommerce-bronze90/bronze_users/",
    partition_col="user_id"
)

write_to_s3(
    users_company,
    f"s3a://ecommerce-bronze90/bronze_user_companies",
    partition_col="user_id`"
)

write_to_s3(
    users_location,
    f"s3a://ecommerce-bronze90/bronze_user_locations/",
    partition_col="user_id"
)







