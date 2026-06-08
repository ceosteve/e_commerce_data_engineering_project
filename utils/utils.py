import boto3
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

load_dotenv()
#create an s3 client
s3_client = boto3.client(
        's3',
        region_name='eu-north-1',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )

#create an glue client
glue_client = boto3.client(
    'glue',
    region_name='eu-north-1',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)


#create an athena client
athena_client = boto3.client(
    'athena',
    region_name='eu-north-1',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

## initiliaze a spark session
def get_spark():
    spark = (
        SparkSession.builder.appName("EcommercePipeline")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ).getOrCreate())

    hadoop_conf = spark._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    hadoop_conf.set(
        "fs.s3a.access.key",
        os.environ["AWS_ACCESS_KEY_ID"]
    )

    hadoop_conf.set(
        "fs.s3a.secret.key",
        os.environ["AWS_SECRET_ACCESS_KEY"]
    )

    hadoop_conf.set(
        "fs.s3a.endpoint",
        "s3.amazonaws.com"
    )

def create_glue_table(database, table_name, s3_path, columns, partition_keys=None):
    try:
        glue_client.create_table(
            DatabaseName=database,
            TableInput={
                "Name": table_name,
                "TableType": "EXTERNAL_TABLE",
                "PartitionKeys": partition_keys or [],
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": s3_path,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                },
            },
        )
        print(f"Created Glue table: {table_name}")

    except glue_client.exceptions.AlreadyExistsException:
        print(f"Glue table already exists: {table_name}")