from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from chalicelib.content_utils import AudioContent


@dataclass
class RSSItem:
    videoId: str
    link: str
    encoded_link: str
    published: datetime
    title: str = field(default=None)
    updated: datetime = field(default=None)
    description: str = field(default=None)
    views: int = field(default=None)
    starRating_count: int = field(default=None)
    starRating_min: int = field(default=None)
    starRating_max: int = field(default=None)
    starRating_avg: float = field(default=None)
    content: AudioContent = field(default=None)

    def keys_as_dict(self):
        return {
            "videoId": self.videoId,
            "encoded_link": self.encoded_link,
            "link": self.link,
            "published": self.published,
        }


@dataclass
class RSSChannel:
    channelId: str
    title: str
    author_name: str
    author_uri: str
    feed_link: str
    channel_link: str
    published_date: datetime
    encoded_channel_link: str
    encoded_feed_link: str
    items: list[RSSItem]

    def filter_items_by_date(self, date_min, date_max):
        def is_date_in_range(item: RSSItem):
            return date_min <= item.published <= date_max

        return filter(is_date_in_range, self.items)


@dataclass
class RSSFeed:
    channel: RSSChannel

    def append_items(self, items):
        self.channel.items = items + self.channel.items
