import os

from runner import ScrapyRunner
from storage import DataStorage
from downloader import ResultsDownloader


class Executor:

    def __init__(self, root_dir: str, storage: DataStorage, downloader: ResultsDownloader, crawler: ScrapyRunner):
        self.root_dir = root_dir
        self.storage = storage
        self.downloader = downloader
        self.crawler = crawler

    def execute_init_storage(self, root_dir):
        self.root_dir = os.path.abspath(root_dir)
        if not os.path.exists(self.root_dir):
            raise FileNotFoundError(f"Can't open data root directory, {self.root_dir} is not exists.")
        elif not os.path.isdir(self.root_dir):
            raise NotADirectoryError(f"Can't open data root directory, {self.root_dir} is not a directory")
        else:
            pass

