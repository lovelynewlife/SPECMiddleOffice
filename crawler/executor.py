import os

from storage import DataStorage
from download import ResultsDownloader


class Executor:
    __ROOT_NAME = "SPEC"

    def __init__(self, downloader: ResultsDownloader):
        self.storage = None
        self.root_dir = None
        self.downloader = downloader

    @property
    def data_dir(self):
        return os.path.join(self.root_dir, self.__ROOT_NAME)

    def execute_init_storage(self, root_dir):
        self.root_dir = os.path.abspath(root_dir)
        if not os.path.exists(self.root_dir):
            raise FileNotFoundError(f"Can't open data root directory, {self.root_dir} is not exists.")
        elif not os.path.isdir(self.root_dir):
            raise NotADirectoryError(f"Can't open data root directory, {self.root_dir} is not a directory")
        else:
            if os.path.exists(self.data_dir):
                # try load storage from exists
                pass
