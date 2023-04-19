from downloader import ResultsDownloader
from runner import ScrapyRunner
from storage import BenchmarkGroup


class BenchmarkExecutor:

    def __init__(self, downloader: ResultsDownloader, crawler: ScrapyRunner):
        self.downloader = downloader
        self.crawler = crawler

    def execute_xx(self, group: BenchmarkGroup):
        pass
