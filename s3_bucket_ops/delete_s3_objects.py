import os
import boto3
from dotenv import load_dotenv
import os

load_dotenv()


s3_client = boto3.client('s3',
                          region_name='eu-north-1',
                           aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
                           aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
                             )


# delete versioned objects in a bucket
def delete_versioned_object():
  
  """
   This function deletes a versioned object by retrieving and dropping every specific 
   version ID and delete marker associated with that object
   """
  

  bucket_name = 'ecommerce-glue-scripts90'
  paginator = s3_client.get_paginator("list_object_versions")
  pages = paginator.paginate(Bucket=bucket_name)

  try:
     
    for page in pages:
      delete_targets = []

  # gather historical object versions
      if 'Versions' in page:
        for version in page['Versions']:
            delete_targets.append({"Key": version['Key'], 'VersionId': version['VersionId']})

    # gather delete markers
      if 'DeleteMarkers' in page:
        for marker in page['DeleteMarkers']:
            delete_targets.append({"Key":marker['Key'], 'VersionId':marker['VersionId']})
        

    # execute deletion if both are found
      if delete_targets:
         response = s3_client.delete_objects(
              Bucket = bucket_name,
              Delete = {'Objects': delete_targets}
          )
         
         if "Errors" in response:
            print("Errors", response["Errors"])
         
    print(f"deleted objects from bucket: {bucket_name}")

  except Exception as e:
     print(e)

  return


def delete_bucket(bucket_name):
    """
    This function deletes an S3 bucket after ensuring that all objects (including versions) are removed.
    """
    delete_versioned_object()  # Ensure all versions and delete markers are removed

    try:
        s3_client.delete_bucket(Bucket=bucket_name)
        print(f"Deleted bucket: {bucket_name}")
    except Exception as e:
        print(f"Error deleting bucket {bucket_name}: {e}")

    return

if __name__ == "__main__":
    delete_bucket("ecommerce-glue-scripts90")