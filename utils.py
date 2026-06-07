import boto3
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

load_dotenv()

# Get credentials safely using getenv
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")


# -------------------
# S3 CLIENT
# -------------------
s3_client = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# -------------------
# GLUE CLIENT
# -------------------
glue_client = boto3.client(
    "glue",
    region_name="us-east-1",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# -------------------
# ATHENA CLIENT
# -------------------
athena_client = boto3.client(
    "athena",
    region_name="us-east-1",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)


# -------------------
# SPARK SESSION
# -------------------
def get_spark():
    return SparkSession.builder \
        .appName("ecommerce_pipeline") \
        .master("local[2]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()