from . import *

from html.parser import HTMLParser

from yt_dlp import YoutubeDL

from chalicelib.urls import *


ydl_options = {
    "format": "m4a/webm",
    "overwrites": True,
}


def get_file_hash(filename):
    with open(filename, "rb") as f:
        hash = hashlib.sha256()
        while content := f.read(4000000):
            hash.update(content)
        return hash.hexdigest()


def download_from_youtube(base_url, path_template):
    opts = ydl_options.copy()
    opts["outtmpl"] = path_template
    with YoutubeDL(opts) as ydl:
        ydl.download([base_url])


def extract_content(base_url):
    download_from_youtube(base_url, "/tmp/content.%(ext)s")

    for extension in ydl_options["format"].split("/"):
        try:
            return AudioContent(
                base_url,
                content=open(f"/tmp/content.{extension}", "rb"),
                content_type=extension,
                content_hash=get_file_hash(f"/tmp/content.{extension}"),
            )
        except OSError:
            continue
