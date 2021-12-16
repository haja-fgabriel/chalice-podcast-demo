from importlib import import_module
from re import sub
from chalicelib.aws.s3 import list_subdirectories

import pytest

from app import app
from chalice.test import Client


@pytest.fixture
def client():
    with Client(app) as client:
        yield client


@pytest.fixture(scope="session")
def video():
    return {
        "videoId": "AY5d3QlulqM",
        "link": "https://www.youtube.com/watch?v=AY5d3QlulqM",
        "encoded_link": "b08f8e8d79f7eb0316754a462f124b1de44bc3839e88e50f15815950538670eb",
        "published": "2021-12-14T00:00:00",
    }


@pytest.fixture(scope="session")
def feed_prefix():
    return f"Florin/podcasts_test/Music"


def test_fetch_items(client, video, feed_prefix):
    response = client.lambda_.invoke("fetch_item", {"item": video, "prefix": feed_prefix})
    subdirs = list_subdirectories(feed_prefix + "/")
    assert subdirs
    assert video["encoded_link"] in subdirs
