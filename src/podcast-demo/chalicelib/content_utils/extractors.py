from . import AudioContent

import os.path

from yt_dlp import YoutubeDL
from memory_profiler import profile


ydl_options = {
    "format": "m4a/webm",
    "overwrites": True,
}


def get_file_hash(filename):
    with open(filename, "rb") as f:
        hash = hashlib.sha256()
        while content := f.read(1000000):
            hash.update(content)
        return hash.hexdigest()


@profile
def download_from_youtube(base_url, path_template):
    opts = ydl_options.copy()
    opts["outtmpl"] = path_template
    with YoutubeDL(opts) as ydl:
        ydl.download([base_url])


@profile
def extract_content(base_url):
    download_from_youtube(base_url, "/tmp/content.%(ext)s")

    for extension in ydl_options["format"].split("/"):
        path_to_content = f"/tmp/content.{extension}"
        if os.path.exists(path_to_content):
            return AudioContent(
                base_url,
                content=open(path_to_content, "rb"),
                content_type=extension,
                content_hash=get_file_hash(f"/tmp/content.{extension}"),
            )
