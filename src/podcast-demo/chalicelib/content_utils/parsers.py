from . import *

import json
from typing import List


class AudioContentParser:
    def __parse_json(self, content: str) -> dict:
        return json.loads(content)

    def parse(self, content: str) -> AudioContent:
        self.__parsed_content = self.__parse_json(content)
        return AudioContent(
            base_url=self.__parsed_content.get("base_url"),
            content_type=self.__parsed_content.get("content_type"),
            content_hash=self.__parsed_content.get("content_hash"),
        )
