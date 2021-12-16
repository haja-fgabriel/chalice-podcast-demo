import boto3
from botocore.config import Config

BUCKET = "aws-lambda-juniors"


def s3_client():
    return boto3.Session().client("s3", config=Config(signature_version="s3v4"), region_name="us-east-2")


def list_subdirectories(prefix):
    def get_subdirectory(content):
        path = content["Prefix"].rstrip("/")
        try:
            return path[path.rindex("/") + 1 :]
        except ValueError:
            return path

    object_result = s3_client().list_objects(Prefix=prefix, Bucket=BUCKET, Delimiter="/")
    return [*map(get_subdirectory, object_result.get("CommonPrefixes") or [])]
