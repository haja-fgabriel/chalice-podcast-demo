from chalicelib.dates import DateWithTimezoneEncoder
from . import *
from dataclasses import asdict
import json

from smart_open import open

from chalicelib.aws.s3 import BUCKET


class ContentS3Writer:
    path_suffix = ""

    def write_content(self, content: Content, s3_path: str = ""):
        if content.content:
            with open(f"{s3_path}/{content.content_hash}/content", "wb") as g:
                g.write(content.content)

    def write(self, content: Content, prefix: str = ""):
        prefix = prefix.rstrip("/")
        s3_path = f"s3://{BUCKET}/{prefix}/{self.path_suffix}"
        self.write_content(content, s3_path)

        tmp_content = content.content
        content.content = None
        dict_content = asdict(content)
        del dict_content["content"]
        print(dict_content)
        with open(f"{s3_path}/{content.content_hash}/metadata.json", "w") as g:
            g.write(json.dumps(dict_content))
        content.content = tmp_content


class AudioS3Writer(ContentS3Writer):
    path_suffix = "assets/audio"

    def write_content(self, content: AudioContent, s3_path: str = ""):
        if content.content:
            content.content.seek(0)
            with open(f"{s3_path}/{content.content_hash}/content", "wb") as g:
                while c := content.content.read(2000000):
                    g.write(c)
            content.content.seek(0)


class HTMLS3Writer(ContentS3Writer):
    path_suffix = "assets/html"


class ImageS3Writer(ContentS3Writer):
    path_suffix = "assets/img"


class CSSS3Writer(ContentS3Writer):
    path_suffix = "assets/css"


class JSS3Writer(ContentS3Writer):
    path_suffix = "assets/js"
