import os
import json
import glob
import boto3
from datetime import datetime


S3_BUCKET = os.getenv("S3_BUCKET", "atlasiq-data-lake")


def get_s3_client():
    """
    Returns a boto3 S3 client using env credentials.
    """
    return boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_DEFAULT_REGION", "eu-west-2"),
    )


def load_raw_files(source: str) -> list:
    """
    Load all raw JSON files for a given source from local data lake.
    """
    raw_path = f"/opt/airflow/data/raw/{source}"
    pattern  = os.path.join(raw_path, "*.json")
    files    = glob.glob(pattern)

    if not files:
        print(f"No files found for source: {source}")
        return []

    records = []
    for filepath in files:
        with open(filepath, "r") as f:
            data = json.load(f)
            records.append({
                "filepath": filepath,
                "data":     data
            })

    print(f"Loaded {len(files)} files from {source}")
    return records


def save_processed(data: list, table_name: str) -> str:
    today    = datetime.now().strftime("%Y-%m-%d")
    filename = f"{table_name}_{today}.json"

    # NDJSON format — one record per line for Athena compatibility
    content  = "\n".join(json.dumps(record) for record in data)

    # Save locally
    local_path = f"/opt/airflow/data/processed/{table_name}"
    os.makedirs(local_path, exist_ok=True)
    local_filepath = os.path.join(local_path, filename)
    with open(local_filepath, "w") as f:
        f.write(content)
    print(f"Saved {len(data)} records locally to {local_filepath}")

    # Save to S3
    s3_key = f"processed/{table_name}/{filename}"
    try:
        s3 = get_s3_client()
        s3.put_object(
            Bucket      = S3_BUCKET,
            Key         = s3_key,
            Body        = content.encode("utf-8"),
            ContentType = "application/x-ndjson",
        )
        s3_path = f"s3://{S3_BUCKET}/{s3_key}"
        print(f"Uploaded to {s3_path}")
        return s3_path
    except Exception as e:
        print(f"S3 upload failed: {e}")
        return local_filepath
    
    
def add_metadata(record: dict, source: str) -> dict:
    record["source"]       = source
    record["processed_at"] = datetime.now().isoformat()
    return record