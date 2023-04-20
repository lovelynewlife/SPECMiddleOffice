from downloader import ResultsDownloader
from runner import ScrapyRunner
from storage import BenchmarkGroup


class BenchmarkExecutor:

    def __init__(self, *args):
        self.downloader = ResultsDownloader()
        self.crawler = ScrapyRunner("fetch_catalog")

    def execute_xx(self, group: BenchmarkGroup):
        pass
