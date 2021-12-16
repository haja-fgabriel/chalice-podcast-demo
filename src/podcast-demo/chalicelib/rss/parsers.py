from . import *
import base64
from datetime import date, datetime
import hashlib
import json
from xml.etree.ElementTree import XML, Element

from chalicelib.dates import parse_date_with_timezone

xml_namespaces = {
    "yt": "http://www.youtube.com/xml/schemas/2015",
    "media": "http://search.yahoo.com/mrss/",
    "": "http://www.w3.org/2005/Atom",
}


class RSSItemParser:
    def parse(self, item: Element) -> RSSItem:
        media_group_element = item.find("media:group", xml_namespaces)
        media_description_element = media_group_element.find("media:description", xml_namespaces)
        media_community_element = media_group_element.find("media:community", xml_namespaces)
        media_starRating_element = media_community_element.find("media:starRating", xml_namespaces)
        media_statistics_element = media_community_element.find("media:statistics", xml_namespaces)

        videoId = item.find("yt:videoId", xml_namespaces).text
        title = item.find("title", xml_namespaces).text
        link = item.find("link", xml_namespaces).get("href")
        encoded_link = hashlib.sha256(link.encode("utf-8")).hexdigest()
        published = datetime.fromisoformat(item.find("published", xml_namespaces).text)
        updated = datetime.fromisoformat(item.find("updated", xml_namespaces).text)
        description = media_description_element.text
        views = int(media_statistics_element.get("views"))
        starRating_count = int(media_starRating_element.get("count"))
        starRating_min = int(media_starRating_element.get("min"))
        starRating_max = int(media_starRating_element.get("max"))
        starRating_avg = float(media_starRating_element.get("average"))

        return RSSItem(
            videoId=videoId,
            title=title,
            link=link,
            encoded_link=encoded_link,
            published=published,
            updated=updated,
            description=description,
            views=views,
            starRating_count=starRating_count,
            starRating_min=starRating_min,
            starRating_max=starRating_max,
            starRating_avg=starRating_avg,
        )


class RSSChannelParser:
    def __init__(self):
        self.__parsed_xml = None

    def __parse_items(self):
        items = self.__parsed_xml.iterfind("entry", xml_namespaces)
        parser = RSSItemParser()
        return [*map(lambda item: parser.parse(item), items)]

    def parse(self, element: Element) -> RSSChannel:
        self.__parsed_xml = element
        author_element = element.find("author", xml_namespaces)
        author_name_element = author_element.find("name", xml_namespaces)
        author_uri_element = author_element.find("uri", xml_namespaces)
        feed_link_element = element.find('link[@rel="self"]', xml_namespaces)
        channel_link_element = element.find('link[@rel="alternate"]', xml_namespaces)
        title_element = element.find("title", xml_namespaces)
        channelId_element = element.find("yt:channelId", xml_namespaces)
        published_date_element = element.find("published", xml_namespaces)

        author_name = author_name_element.text
        author_uri = author_uri_element.text
        title = title_element.text if title_element is not None else ""
        feed_link = feed_link_element.get("href") if feed_link_element is not None else ""
        channel_link = channel_link_element.get("href") if channel_link_element is not None else ""
        channelId = channelId_element.text if channelId_element is not None else ""
        encoded_feed_link = hashlib.sha256(feed_link.encode()).hexdigest()
        encoded_channel_link = hashlib.sha256(channel_link.encode()).hexdigest()
        published_date = (
            datetime.fromisoformat(published_date_element.text) if published_date_element is not None else None
        )
        items = self.__parse_items()
        return RSSChannel(
            author_name=author_name,
            author_uri=author_uri,
            channelId=channelId,
            title=title,
            published_date=published_date,
            feed_link=feed_link,
            channel_link=channel_link,
            encoded_feed_link=encoded_feed_link,
            encoded_channel_link=encoded_channel_link,
            items=items,
        )


class RSSFeedParserXML:
    def __init__(self):
        self.__parsed_xml = None

    def __parse_xml(self, xml: str):
        return XML(xml)

    def __parse_channel(self) -> RSSChannel:
        parser = RSSChannelParser()
        return parser.parse(self.__parsed_xml)

    def parse(self, xml) -> RSSFeed:
        self.__parsed_xml = self.__parse_xml(xml)
        return RSSFeed(self.__parse_channel())


class RSSItemParserJSON:
    def parse(self, item: dict) -> RSSItem:
        videoId = item.get("videoId")
        title = item.get("title")
        link = item.get("link")
        encoded_link = item.get("encoded_link") or hashlib.sha256(link.encode("utf-8")).hexdigest()
        published = datetime.fromisoformat(item.get("published"))
        updated = datetime.fromisoformat(item.get("updated"))
        description = item.get("description")
        views = item.get("views")
        starRating_count = item.get("starRating_count")
        starRating_min = item.get("starRating_min")
        starRating_max = item.get("starRating_max")
        starRating_avg = item.get("starRating_avg")
        return RSSItem(
            videoId=videoId,
            title=title,
            link=link,
            encoded_link=encoded_link,
            published=published,
            updated=updated,
            description=description,
            views=views,
            starRating_count=starRating_count,
            starRating_min=starRating_min,
            starRating_max=starRating_max,
            starRating_avg=starRating_avg,
        )


class RSSChannelParserJSON:
    def __init__(self):
        self.__parsed_json = None

    def __parse_items(self):
        items = self.__parsed_json.get("items")
        parser = RSSItemParserJSON()
        return [*map(lambda item: parser.parse(item), items)]

    def parse(self, feed_dict: dict) -> RSSChannel:
        self.__parsed_json = feed_dict
        title = feed_dict.get("title")
        feed_link = feed_dict.get("feed_link")
        channel_link = feed_dict.get("channel_link")
        published_date = datetime.fromisoformat(feed_dict.get("published_date"))
        encoded_channel_link = feed_dict.get("channel_link") or hashlib.sha256(channel_link.encode("utf-8")).hexdigest()
        encoded_feed_link = feed_dict.get("feed_link") or hashlib.sha256(feed_link.encode("utf-8")).hexdigest()
        author_name = feed_dict.get("author_name")
        author_uri = feed_dict.get("author_uri")
        channelId = feed_dict.get("channelId")

        items = self.__parse_items()
        return RSSChannel(
            author_name=author_name,
            author_uri=author_uri,
            channelId=channelId,
            title=title,
            published_date=published_date,
            feed_link=feed_link,
            channel_link=channel_link,
            encoded_feed_link=encoded_feed_link,
            encoded_channel_link=encoded_channel_link,
            items=items,
        )


class RSSFeedParserJSON:
    def __init__(self):
        self.__parsed_json = None

    def __parse_json(self, feed_json: str):
        return json.loads(feed_json)

    def __parse_channel(self) -> RSSChannel:
        channel = self.__parsed_json.get("channel")
        parser = RSSChannelParserJSON()
        return parser.parse(channel)

    def parse(self, feed_json: str) -> RSSFeed:
        self.__parsed_json = self.__parse_json(feed_json)
        return RSSFeed(self.__parse_channel())
