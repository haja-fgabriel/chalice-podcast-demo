import hashlib
from urllib.parse import urlparse, urlunparse


def convert_to_absolute_url(base_url, other_url):
    parsed_other_url = urlparse(other_url)
    parsed_base_url = urlparse(base_url)
    if not parsed_other_url.netloc:
        if parsed_other_url.path[:1] != "/":
            parsed_other_url = parsed_other_url._replace(path=f"{parsed_base_url.path}/{parsed_other_url.path}")
        parsed_other_url = parsed_other_url._replace(netloc=parsed_base_url.netloc, scheme=parsed_base_url.scheme)
    if not parsed_other_url.scheme:
        parsed_other_url = parsed_other_url._replace(scheme=parsed_base_url.scheme)
    return urlunparse(parsed_other_url)


def map_to_canonical_urls(base_url, urls):
    return [*map(lambda u: convert_to_absolute_url(base_url, u), urls)]


def encode_with_sha256(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def map_to_sha256_encoded(urls):
    return [*map(encode_with_sha256, urls)]
