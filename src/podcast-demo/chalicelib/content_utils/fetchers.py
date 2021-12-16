from . import *
from .parsers import AudioContentParser

from smart_open import open

from chalicelib.aws.s3 import BUCKET


def get_metadata(prefix: str) -> AudioContent:
    s3_path = f"s3://{BUCKET}/{prefix}"
    parser = AudioContentParser()
    with open(f"{s3_path}/metadata.json", "r") as f:
        return parser.parse(f.read())
