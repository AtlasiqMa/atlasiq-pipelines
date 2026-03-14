import os
import json
import boto3
from datetime import datetime


S3_BUCKET = os.getenv("S3_BUCKET", "atlasiq-data-lake")


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_DEFAULT_REGION", "eu-west-2"),
    )


def upload_raw_to_s3(filepath: str, source: str) -> str:
    """
    Upload a raw file to S3 after it's been saved locally.
    Returns the S3 path.
    """
    filename = os.path.basename(filepath)
    s3_key   = f"raw/{source}/{filename}"

    try:
        s3 = get_s3_client()
        with open(filepath, "rb") as f:
            s3.put_object(
                Bucket      = S3_BUCKET,
                Key         = s3_key,
                Body        = f,
                ContentType = "application/json",
            )
        s3_path = f"s3://{S3_BUCKET}/{s3_key}"
        print(f"Uploaded raw file to {s3_path}")
        return s3_path
    except Exception as e:
        print(f"S3 upload failed for {filepath}: {e}")
        return filepath