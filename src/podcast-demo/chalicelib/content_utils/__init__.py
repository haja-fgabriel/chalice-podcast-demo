from dataclasses import dataclass, field
import hashlib
from io import IOBase
from typing import Generator

import requests
from yt_dlp import YoutubeDL

ydl_options = {
    "format": "m4a/webm",
}


@dataclass
class Content:
    base_url: str
    content_type: str = field(default=None)
    content: bytes = field(default=None)
    content_hash: str = field(default=None)

    def keys_as_dict(self):
        return {"base_url": self.base_url, "content_type": self.content_type, "content_hash": self.content_hash}

    def fetch(self):
        response = requests.get(self.base_url)
        self.content_type = response.headers["Content-Type"]
        self.content = response.content
        self.content_hash = hashlib.sha256(response.content).hexdigest()


@dataclass
class AudioContent(Content):
    content: IOBase = field(default=None)

    def fetch(self):
        pass
