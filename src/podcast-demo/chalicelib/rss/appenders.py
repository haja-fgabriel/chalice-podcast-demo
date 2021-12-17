from chalicelib.aws.awslambda import invoke_lambda
from chalicelib.dates import DateWithTimezoneEncoder
from memory_profiler import profile

from . import *

from threading import Thread
from datetime import timedelta
import itertools
import json
from typing import List

from .fetchers import *
from .writers import RSSFeedS3Writer, extract_content_and_write_item


@profile
def run_trust_mode(feed_url, previous_date, current_date, prefix):
    parsed_rss = get_feed_from_url(feed_url)
    if not [*parsed_rss.channel.filter_items_by_date(previous_date, current_date)]:
        return

    s3_path = f"{prefix}/{parsed_rss.channel.encoded_feed_link}"
    parsed_old_rss = get_feed_from_s3(s3_path)
    if parsed_old_rss:
        latest_s3_item_date = parsed_old_rss.channel.items[0].published
        filtered_items = [
            *parsed_rss.channel.filter_items_by_date(
                max(previous_date, latest_s3_item_date + timedelta(milliseconds=1)), current_date
            )
        ]
        parsed_old_rss.append_items(filtered_items)
    else:
        filtered_items = [*parsed_rss.channel.filter_items_by_date(previous_date, current_date)]
        parsed_old_rss = parsed_rss
        parsed_old_rss.channel.items = filtered_items

    dict_filtered_items = [*map(lambda i: i.keys_as_dict(), filtered_items)]

    print(f"Number of articles: {len(filtered_items)}")
    print(filtered_items)

    if filtered_items:
        write_thread = Thread(target=RSSFeedS3Writer().write, args=(parsed_old_rss, s3_path))
        write_thread.start()
        for item in dict_filtered_items:
            invoke_lambda(
                "PodcastDemoFlorin-dev-fetch_item",
                json.dumps({"item": item, "prefix": s3_path}, cls=DateWithTimezoneEncoder),
                invoke_async=True,
            )
        write_thread.join()
