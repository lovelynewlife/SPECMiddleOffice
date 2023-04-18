import os

from runner import ScrapyRunner
from storage import DataStorage
from downloader import ResultsDownloader


class Executor:

    def __init__(self, storage: DataStorage, downloader: ResultsDownloader, crawler: ScrapyRunner):
        self.storage = storage
        self.downloader = downloader
        self.crawler = crawler

    def execute_init_storage(self, root_dir):
        pass
