"""S3 data reader utility."""
import csv
import os
from io import StringIO

try:
    import boto3
except ImportError:
    boto3 = None

# ==================== Configuration ====================
BUCKET_NAME = "amzn-s3-bucket-777997078677-ap-southeast-2-an"
FILE_KEY = "Comp3041J MiniProject 2 Dataset.csv"
REGION = "ap-southeast-2"
LOCAL_FILE = "Comp3041J MiniProject 2 Dataset.csv"

# AWS credentials are read from environment variables.
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "")


def get_s3_client():
    """Get S3 client using configured AWS credentials."""
    if boto3 is None:
        raise RuntimeError("boto3 is not installed. Run: pip install -r requirements.txt")

    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        raise RuntimeError(
            "AWS credentials are missing. Set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY environment variables."
        )

    kwargs = {
        "region_name": REGION,
        "aws_access_key_id": AWS_ACCESS_KEY,
        "aws_secret_access_key": AWS_SECRET_KEY,
    }
    if AWS_SESSION_TOKEN:
        kwargs["aws_session_token"] = AWS_SESSION_TOKEN

    return boto3.client("s3", **kwargs)


def _read_text_from_s3():
    """Read the dataset object from S3."""
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    return obj["Body"].read().decode("utf-8")


def _read_text_from_local():
    """Read the dataset from the local project folder."""
    with open(LOCAL_FILE, "r", encoding="utf-8") as f:
        return f.read()


def read_dataset_text():
    """Read data from S3, falling back to a local CSV copy if needed."""
    try:
        return _read_text_from_s3()
    except Exception as exc:
        print(f"Warning: could not read from S3 ({exc}). Using local CSV instead.")
        return _read_text_from_local()


def read_csv_from_s3():
    """Read CSV file from S3 or local fallback and return list of dicts."""
    content = read_dataset_text()
    return list(csv.DictReader(StringIO(content)))


def read_csv_lines_from_s3():
    """Read raw CSV lines from S3 or local fallback for sharding."""
    content = read_dataset_text()
    lines = content.splitlines()
    return lines[0], lines[1:]

