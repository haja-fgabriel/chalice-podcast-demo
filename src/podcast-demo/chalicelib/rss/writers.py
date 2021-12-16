from dataclasses import asdict
import itertools
from concurrent.futures import ThreadPoolExecutor
import json

from . import *
from .parsers import *
from .fetchers import *
from chalicelib.dates import DateWithTimezoneEncoder
from chalicelib.content_utils.writers import AudioS3Writer
from chalicelib.content_utils.extractors import extract_content

from smart_open import open


class RSSItemS3Writer:
    def write(self, itemext: RSSItem, prefix=""):
        prefix = prefix.rstrip("/")
        content_prefix = f"{prefix}/{itemext.encoded_link}"
        s3_path = f"s3://{BUCKET}/{content_prefix}"
        print(s3_path)
        AudioS3Writer().write(itemext.content, content_prefix)

        tmp_content = None
        if itemext.content:
            tmp_content = itemext.content.content
            itemext.content.content = None
        dict_itemext = asdict(itemext)
        del dict_itemext["content"]["content"]
        with open(f"{s3_path}/metadata.json", "w") as g:
            json.dump(dict_itemext, g, cls=DateWithTimezoneEncoder)

        if itemext.content:
            itemext.content.content = tmp_content


class RSSFeedS3Writer:
    def write(self, feed: RSSFeed, prefix=""):
        prefix = prefix.rstrip("/")
        s3_path = f"s3://{BUCKET}/{prefix}"
        dict_feed = asdict(feed)
        dict_feed["channel"]["items"] = [
            *map(
                lambda item: {
                    k: v
                    for k, v in item.items()
                    if k in ("videoId", "link", "encoded_link", "published", "updated", "description", "title", "views")
                },
                dict_feed["channel"]["items"],
            )
        ]
        with open(f"{s3_path}/metadata.json", "w") as g:
            json.dump(dict_feed, g, cls=DateWithTimezoneEncoder)


def write_item(item, prefix):
    item.content = extract_content(item.link)
    RSSItemS3Writer().write(item, prefix)
