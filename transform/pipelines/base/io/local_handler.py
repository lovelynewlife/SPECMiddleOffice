import os.path
from typing import Union, IO, List, Dict

from pipelines.base.io.handler import IOHandler


class LocalFileHandler(IOHandler):

    def __init__(self):
        self._local_file: Union[str, None] = None

    def open(self, *args):
        file_path, = args
        self._local_file = file_path

    def read(self):
        return self._local_file

    def write(self, contents: List[Dict] = None, **kwargs):

        def write_file(drop_collection=False):
            mode = "a+"
            if drop_collection:
                mode = "w"

            with open(self._local_file, mode) as file:
                file.write(str(contents))

        if os.path.isfile(self._local_file):
            write_file(**kwargs)

    def close(self):
        self._local_file = None
