import hashlib
from concurrent.futures import ThreadPoolExecutor
from typing import List, Iterable

import requests
from smart_open import open

from . import *
from .parsers import RSSFeedParserJSON, RSSFeedParserXML
from chalicelib.aws.s3 import list_subdirectories, BUCKET


def get_feed_from_s3(prefix):
    s3_path_prefix = f"s3://{BUCKET}/{prefix}"
    parser = RSSFeedParserJSON()
    try:
        with open(f"{s3_path_prefix}/metadata.json", "r") as f:
            return parser.parse(f.read())
    except OSError:
        return None


def get_feed_from_url(feed_url):
    rss = requests.get(feed_url).content.decode("utf-8")

    parsed_rss = RSSFeedParserXML().parse(rss)
    return parsed_rss


def list_html_hashes_for_item(prefix):
    return list_subdirectories(f"{prefix}/assets/audio/")


def get_html_hashes_for_items(prefix: str, items: List[RSSItem]) -> List[str]:
    return get_html_hashes_for_item_guids(prefix, map(lambda i: i.encoded_link, items))


def get_html_hashes_for_item_guids(prefix: str, guids: Iterable[str]):
    def extract_hash(results):
        return results[0] if results else None

    with ThreadPoolExecutor(max_workers=30) as executor:
        return [
            *executor.map(
                lambda i: extract_hash(list_html_hashes_for_item(f"{prefix}/{i}")),
                guids,
            )
        ]


def get_full_feed_from_s3(prefix):
    feed = get_feed_from_s3(prefix)
    if not feed:
        return None
    for item, hash in zip(feed.channel.items, get_html_hashes_for_items(prefix, feed.channel.items)):
        # TODO maybe HTMLs are going to have different charsets; you should look into the metadata.json from the assets/html folder
        item.content = AudioContent(base_url=item.link, content_hash=hash)

    return feed


def extract_latest_items(feeds):
    items = {}
    for feed in feeds:
        for item in feed.channel.items:
            duplicate_item = items.get(item.encoded_guid)
            if duplicate_item and duplicate_item.content.content_hash != item.content.content_hash:
                duplicate_item.content.content_hash = item.content.content_hash
            items[item.encoded_guid] = item
    return sorted(items.values(), key=lambda i: i.pubDate)


def get_feed_from_s3_verification_mode(prefix):
    for subdirectory in list_subdirectories(prefix + "/"):
        print(subdirectory)
    with ThreadPoolExecutor(30) as executor:
        feeds = [*map(lambda dir: get_full_feed_from_s3(f"{prefix}/{dir}"), list_subdirectories(prefix + "/"))]
        print("ok")
        if not feeds:
            return None
        feeds[0].channel.items = extract_latest_items(feeds)
        return feeds[0]
    pass
