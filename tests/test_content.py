import hashlib
import re
from urllib.parse import urlparse

import pytest

from chalicelib.content_utils import *
from chalicelib.content_utils.extractors import *
from chalicelib.content_utils.parsers import *
from chalicelib.content_utils.writers import *
from chalicelib.content_utils.fetchers import *


@pytest.fixture(scope="session")
def sample_htmlcontent_json():
    with open("tests/sample_content.json", "r") as f:
        return f.read()


@pytest.fixture(scope="session")
def sample_audiocontent(sample_content_url):
    return AudioContent(sample_content_url)


@pytest.fixture(scope="session")
def prefix():
    return "Florin/podcasts_content_test"


@pytest.fixture(scope="session")
def sample_content_url():
    return "https://www.youtube.com/watch?v=AY5d3QlulqM"


@pytest.fixture(scope="session")
def sample_content(sample_content_url):
    return extract_content(sample_content_url)


def test_extract_content(sample_content):
    assert isinstance(sample_content, AudioContent)
    assert sample_content.content_type in ydl_options["format"].split("/")
    hash = hashlib.sha256()
    with open(f"/tmp/content.{sample_content.content_type}", "rb") as f:
        while c := f.read(1000000):
            hash.update(c)
    assert sample_content.content_hash == hash.hexdigest()


def test_fetch_content(prefix):
    html_hash = "27723570ae4d5752b703c744a458fafec7b4e9eaa3bf9c8a649737eab27e85fe"
    prefix = f"{prefix}/assets/audio/{html_hash}"
    content = get_metadata(prefix)
    assert isinstance(content, AudioContent)


def test_parse_metadata(sample_htmlcontent_json):
    parser = AudioContentParser()
    metadata = parser.parse(sample_htmlcontent_json)
    assert isinstance(metadata, AudioContent)
    assert metadata.base_url
    assert metadata.content_hash
    assert metadata.content_type


def test_write_metadata(prefix, sample_content):
    writer = AudioS3Writer()
    writer.write(sample_content, prefix)
    with open(f"s3://aws-lambda-juniors/{prefix}/assets/audio/{sample_content.content_hash}/content", "rb") as f:
        while c := f.read(2000000):
            assert c == sample_content.content.read(2000000)
