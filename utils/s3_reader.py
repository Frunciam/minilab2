"""S3 data reader utility"""
import csv
from io import StringIO
import boto3

# ==================== Configuration ====================
BUCKET_NAME = "amzn-s3-bucket-777997078677-ap-southeast-2-an"
FILE_KEY = "Comp3041J MiniProject 2 Dataset.csv"
REGION = "ap-southeast-2"

"change this to own keys"
AWS_ACCESS_KEY = "AWS_ACCESS_KEY"
AWS_SECRET_KEY = "AWS_SECRET_KEY"


def get_s3_client():
    """Get S3 client"""
    return boto3.client(
        's3',
        region_name=REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )


def read_csv_from_s3():
    """Read CSV file from S3, return list of dicts"""
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    content = obj['Body'].read().decode('utf-8')
    return list(csv.DictReader(StringIO(content)))


def read_csv_lines_from_s3():
    """Read raw CSV lines from S3 for sharding"""
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    content = obj['Body'].read().decode('utf-8')
    lines = content.splitlines()
    return lines[0], lines[1:]