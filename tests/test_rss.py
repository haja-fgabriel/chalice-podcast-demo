from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, time, timedelta, tzinfo
import hashlib
import re
import time
from zoneinfo import ZoneInfo
from chalicelib.content_utils.fetchers import get_metadata

import pytest

from chalicelib.aws.s3 import list_subdirectories
from chalicelib.dates import DateWithTimezoneEncoder
from chalicelib.content_utils import *
from chalicelib.rss.writers import *
from chalicelib.rss.parsers import *
from chalicelib.rss.fetchers import *
from chalicelib.rss.builders import *
from chalicelib.rss import *
from chalicelib.rss.appenders import *
from chalicelib.rss.comparison import *

# from dates import


@pytest.fixture(scope="function")
def sample_feed_xml():
    with open("tests/sample.rss", "r") as f:
        return f.read()


@pytest.fixture(scope="session")
def sample_html():
    with open("tests/sample.html", "r") as f:
        return f.read()


@pytest.fixture(scope="function")
def parsed_feed(sample_feed_xml):
    parser = RSSFeedParserXML()
    assert parser
    return parser.parse(sample_feed_xml)


@pytest.fixture(scope="function")
def sample_feed_json():
    with open("tests/sample_feed.json", "r") as f:
        return f.read()


@pytest.fixture(scope="session")
def encoded_feed_url():
    return "6e0526dcab962005d9e9ef8cf6bb98974329ba921fe9036629793fb2c0a7523d"


@pytest.fixture(scope="session")
def publisher_url():
    return "http://www.youtube.com/feeds/videos.xml?channel_id=UCL6JmiMXKoXS6bpP1D3bk8g"


@pytest.fixture(scope="session")
def sample_item_as_dict():
    return {
        "videoId": "iHyuKSMp8s0",
        "link": "https://www.youtube.com/watch?v=iHyuKSMp8s0",
        "encoded_link": "da9b37b2dcc79b33f0f16ed5243095910424e904c99c1d1da864d01ddb17fa28",
        "published": datetime(2021, 12, 10, 20, 2, 0),
    }


@pytest.fixture
def sample_item(sample_item_as_dict):
    return RSSItem(**sample_item_as_dict)


@pytest.fixture(scope="function")
def parsed_feed_json(sample_feed_json):
    return RSSFeedParserJSON().parse(sample_feed_json)


@pytest.fixture(scope="session")
def sample_content(sample_html):
    return AudioContent(
        base_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        content_type="m4a",
        content_hash="27723570ae4d5752b703c744a458fafec7b4e9eaa3bf9c8a649737eab27e85fe",
    )


@pytest.fixture(scope="function")
def sample_itemext(parsed_feed, sample_content):
    item = parsed_feed.channel.items[0]
    item.content = sample_content
    return item


@pytest.fixture(scope="session")
def prefix():
    return f"Florin/podcasts_test"


@pytest.fixture(scope="session")
def feed_url():
    return "https://feeds.macrumors.com/MacRumors-All"


@pytest.fixture(scope="session")
def feed_prefix(encoded_feed_url, prefix):
    return f"{prefix}/{encoded_feed_url}"


@pytest.fixture(scope="session")
def feed_prefix_verification(encoded_feed_url, prefix_verification):
    return f"{prefix_verification}/{encoded_feed_url}"


def test_rss_feed():
    feed = RSSFeed(RSSChannel("", "", "", "", "", "", None, "", "", []))
    assert feed
    assert feed.channel


def test_rss_parser(parsed_feed):
    assert isinstance(parsed_feed, RSSFeed)
    assert isinstance(parsed_feed.channel, RSSChannel)
    assert isinstance(parsed_feed.channel.items, list)
    assert parsed_feed.channel.items
    for item in parsed_feed.channel.items:
        assert isinstance(item, RSSItem)


def test_filter_items_by_date(parsed_feed):
    date_max = datetime(2021, 12, 10, 1, 00).astimezone()
    date_min = date_max - timedelta(days=1)
    items = [*parsed_feed.channel.filter_items_by_date(date_min, date_max)]
    assert items
    for item in items:
        assert item.published >= date_min and item.published <= date_max


# @pytest.mark.skip
def test_rss_itemext_writer(sample_itemext, feed_prefix):
    writer = RSSItemS3Writer()
    writer.write(sample_itemext, feed_prefix)


# @pytest.mark.skip
def test_rss_feed_writer(parsed_feed, feed_prefix):
    writer = RSSFeedS3Writer()
    writer.write(parsed_feed, prefix=feed_prefix)


def test_date_encoder():
    encoder = DateWithTimezoneEncoder()
    sample_date = datetime(2021, 11, 15, 11, 23, 00, 00, ZoneInfo("US/Pacific"))
    encoded_date = encoder.encode(sample_date)
    assert encoded_date == '"2021-11-15T11:23:00-08:00"'


def test_append_to_feed(parsed_feed):
    published = datetime.now().astimezone()
    videoId = "asdadsdsadasd"
    link = "https://www.example.com"
    encoded_link = hashlib.sha256(link.encode("utf-8")).hexdigest()
    item = RSSItem(videoId, link, encoded_link, published)
    parsed_feed.append_items([item])
    assert parsed_feed.channel.items[0] == item
    pass


def test_parse_json_feed(sample_feed_json):
    parser = RSSFeedParserJSON()
    parsed_feed = parser.parse(sample_feed_json)
    assert isinstance(parsed_feed, RSSFeed)
    assert isinstance(parsed_feed.channel, RSSChannel)
    for item in parsed_feed.channel.items:
        assert isinstance(item, RSSItem)
        assert isinstance(item.published, datetime)
        assert isinstance(item.updated, datetime)
        assert item.link
        assert item.encoded_link
        assert item.videoId
        assert item.title


@pytest.mark.skip
def test_fetch_feed_trust_mode(prefix, encoded_feed_url, publisher_url):
    s3_path = f"{prefix}/{encoded_feed_url}"
    old_feed = get_feed_from_s3(s3_path)

    date_max = datetime.now().astimezone()
    date_min = date_max - timedelta(hours=24)
    run_trust_mode(publisher_url, date_min, date_max, prefix)

    feed = get_feed_from_s3(s3_path)
    items = feed.channel.items
    if old_feed:
        old_items = old_feed.channel.items
        assert items_in_feed(old_feed, feed)
        if items[0].published > old_items[0].published:
            assert len(old_items) < len(items)
            assert items[-1].published == old_items[-1].published

    online_feed = get_feed_from_url(publisher_url)
    online_feed.channel.items = [*online_feed.channel.filter_items_by_date(date_min, date_max)]
    time.sleep(5)
    assert items_in_feed(online_feed, feed)


def test_get_feed_from_s3(feed_prefix):
    s3_path = feed_prefix
    feed = get_feed_from_s3(s3_path)
    if feed is not None:
        assert isinstance(feed, RSSFeed)
        assert feed.channel
        for item in feed.channel.items:
            assert isinstance(item, RSSItem)
            assert item.published
            assert item.updated
            assert item.videoId
            assert item.link
            assert item.encoded_link


# @pytest.mark.skip
def test_list_hashes_for_item(feed_prefix):
    encoded_item_url = "da9b37b2dcc79b33f0f16ed5243095910424e904c99c1d1da864d01ddb17fa28"
    path = f"{feed_prefix}/{encoded_item_url}"
    hashes = list_html_hashes_for_item(path)
    assert hashes
    for hash in hashes:
        assert re.search("^[0-9A-Fa-f]{64}$", hash)
    pass


def items_in_feed(old_feed, new_feed):
    for item in old_feed.channel.items:
        in_new_feed = False
        for new_item in new_feed.channel.items:
            if item.encoded_link == new_item.encoded_link:
                in_new_feed = True
                break
        if not in_new_feed:
            return False
    return True


@pytest.mark.skip
def test_get_html_hashes_for_item_guids(sample_item, feed_prefix):
    hashes = get_html_hashes_for_item_guids(feed_prefix, [sample_item.encoded_guid])
    assert len(hashes) == 1
    assert re.match(r"^[a-fA-F0-9]{64}", hashes[0])


def assert_fetched(prefix, encoded_feed_url, date_max):
    is_fetched = False
    for directory in list_subdirectories(f"{prefix}/{encoded_feed_url}/"):
        if date_max.strftime("%Y%m%d_%H%M%S") in directory:
            is_fetched = True
            break
    assert is_fetched


def assert_has_items(prefix, encoded_feed_url, date_max):
    root_path = f"{prefix}/{encoded_feed_url}/run_date={date_max.strftime('%Y%m%d_%H%M%S')}"

    def has_assets_on_s3(guid):
        subdirectories = list_subdirectories(f"{root_path}/{guid}")
        return len(subdirectories) > 0 and subdirectories[0] == guid

    feed = get_feed_from_s3(root_path)
    with ThreadPoolExecutor(30) as executor:
        all_item_guids = map(lambda i: i.encoded_guid, feed.channel.items)
        assert any(executor.map(has_assets_on_s3, all_item_guids))


def assert_items_in_date_range(date_min, date_max, prefix_verification, encoded_feed_url):
    feed = get_feed_from_s3(
        "https://www.macrumors.com",
        f"{prefix_verification}/{encoded_feed_url}/run_date={date_max.strftime('%Y%m%d_%H%M%S')}",
    )
    assert feed
    for item in feed.channel.items:
        assert date_min <= item.pubDate <= date_max


def test_get_feed_with_hashes(feed_prefix):
    feed = get_full_feed_from_s3(feed_prefix)
    assert isinstance(feed, RSSFeed)
    assert feed.channel.items
    for item in feed.channel.items:
        assert isinstance(item.content, AudioContent)
        assert item.content
        assert item.content.base_url
        # TODO download the assets for the items first
        # assert item.content.content_hash
    pass


def get_feed_and_assert_all_items(big_feed, path_to_new_feed):
    new_feed = get_full_feed_from_s3(path_to_new_feed)
    return items_in_feed(new_feed, big_feed)


def feed_contains_all_items(feed, feed_prefix_verification):
    with ThreadPoolExecutor(30) as executor:
        return all(
            executor.map(
                lambda p: get_feed_and_assert_all_items(feed, f"{feed_prefix_verification}/{p}"),
                list_subdirectories(f"{feed_prefix_verification}/"),
            )
        )


def test_write_item(sample_item, prefix):
    s3_path = f"{prefix}_items"
    write_item(sample_item, s3_path)
    print(f"{s3_path}/{sample_item.encoded_link}/assets/audio/{sample_item.content.content_hash}/content")
    assert sample_item.content
    metadata = get_metadata(f"{s3_path}/{sample_item.encoded_link}")
    with open(
        f"s3://aws-lambda-juniors/{s3_path}/{sample_item.encoded_link}/assets/audio/{sample_item.content.content_hash}/content",
        "rb",
    ) as f:
        assert f.read(10)
    pass
