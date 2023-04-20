from urllib.parse import urljoin

from downloader import ResultsDownloader
from runner import ScrapyRunner
from storage import BenchmarkGroup, GroupType

GroupTypeExeMaps = {
    GroupType.OSG: "OSG",
    GroupType.HPC: "HPC",
    GroupType.ISG: "ISG",
    GroupType.GWP: "GWP",
    GroupType.RG: "RG"
}


class BenchmarkExecutor:
    __BASIC_DOMAIN = "https://spec.org/"

    def __init__(self, *args):
        self.downloader = ResultsDownloader()
        self.crawler = ScrapyRunner("fetch_catalog")

    def execute_fetch_benchmarks(self, group: BenchmarkGroup):
        group_exe_name = GroupTypeExeMaps[group.group_type]
        spider_name = f"{group_exe_name}Benchmarks"
        self.crawler.run_one_crawl(spider_name, group.data_root_path,
                                   f"BENCHMARKS_FILE_NAME={group.benchmarks_filename}")
        try:
            group.verify_catalogs()
            group.verify_results()
        except FileNotFoundError as fn:
            raise RuntimeError(fn)
        except NotADirectoryError as nd:
            raise RuntimeError(nd)
        group.read_benchmarks()

        return group.get_benchmarks()

    def execute_fetch_catalogs(self, group: BenchmarkGroup, benchmarks):
        available = group.get_benchmarks()
        not_available = []
        status = []

        for b in benchmarks:
            if b not in available:
                not_available.append(b)
        if len(not_available) > 0:
            raise RuntimeError(f"Benchmarks: {','.join(not_available)} are not available.")
        group_exe_name = GroupTypeExeMaps[group.group_type]
        spider_name = f"{group_exe_name}Catalog"
        btf = ",".join(benchmarks)
        self.crawler.run_one_crawl(spider_name, group.catalog_path,
                                   f"BENCHMARKS_TO_FETCH={btf}",
                                   f"RESULTS_DOWNLOAD_MARK={group.results_download_mark}",
                                   f"CATALOG_ID_FIELD={group.results_id_field}")

        for b in benchmarks:
            status.append((b, group.get_catalog_location(b)))

        for b, bs in status:
            if bs is not None:
                try:
                    group.verify_results_files(b)
                except NotADirectoryError as nd:
                    raise RuntimeError(nd)
                except FileNotFoundError as fn:
                    raise RuntimeError(fn)

        return status

    @staticmethod
    def execute_show_supported_types(group: BenchmarkGroup, benchmark):
        available = group.get_benchmarks()
        if benchmark not in available:
            raise RuntimeError(f"Unsupported benchmark: {benchmark}")
        try:
            res = group.get_supported_file_types(benchmark)
            return res
        except FileNotFoundError:
            raise RuntimeError(f"Benchmark {benchmark} catalog file not found.")

    @staticmethod
    def validate_available(group:BenchmarkGroup, benchmark, file_type):
        available = group.get_benchmarks()
        if benchmark not in available:
            raise RuntimeError(f"Unsupported benchmark: {benchmark}")
        catalog_location = group.get_catalog_location(benchmark)
        if catalog_location is None:
            raise RuntimeError(f"Benchmark {benchmark} catalog file not found.")
        available_file_types = group.get_supported_file_types(benchmark)

        if file_type not in available_file_types:
            raise RuntimeError(f"Unsupported file type {file_type} of benchmark: {benchmark}.")

    def execute_download_results(self, group: BenchmarkGroup, benchmark, file_type):
        self.validate_available(group, benchmark, file_type)
        target_dir = group.get_results_file_dir(benchmark, file_type)
        results_location = group.get_all_results_location(benchmark, file_type)
        id_urls = dict()
        for rid, url in results_location:
            id_urls[rid] = urljoin(self.__BASIC_DOMAIN, url)
        self.downloader.download_results(target_dir, id_urls, file_type)
        group.verify_results_with_file_type(benchmark, file_type)

    def execute_download_lost_results(self, group: BenchmarkGroup, benchmark, file_type):
        self.validate_available(group, benchmark, file_type)
        target_dir = group.get_results_file_dir(benchmark, file_type)
        try:
            results_location = group.check_lost_results_files(benchmark, file_type)
        except FileNotFoundError as fn:
            raise RuntimeError(fn)
        id_urls = dict()
        for _, rid, url in results_location:
            id_urls[rid] = urljoin(self.__BASIC_DOMAIN, url)
        self.downloader.download_results(target_dir, id_urls, file_type)
        group.verify_results_with_file_type(benchmark, file_type)

    def execute_get_result_locations(self, group: BenchmarkGroup, benchmark, file_type, indices):
        self.validate_available(group, benchmark, file_type)
        res = []
        try:
            locations = group.get_results_location(benchmark, file_type, indices)
            for index, path, rurl in locations:
                res.append((index, path, urljoin(self.__BASIC_DOMAIN, rurl)))
        except FileNotFoundError as fn:
            raise RuntimeError(fn)
        except NotADirectoryError as nd:
            raise RuntimeError(nd)
        finally:
            return res
