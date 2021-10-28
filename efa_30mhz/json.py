from typing import List
import json

from efa_30mhz.sync import Source


class JSONSource(Source):
    @staticmethod
    def to_thirty_mhz(**kwargs):
        pass

    def __init__(self, filename, **kwargs):
        super(JSONSource, self).__init__(**kwargs)
        self.filename = filename

    def read_all(self) -> List:
        with open(self.filename, "r") as f:
            return json.load(f)
