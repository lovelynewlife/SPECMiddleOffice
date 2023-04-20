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
        for b in benchmarks:
            if b not in available:
                not_available.append(b)
        if len(not_available) > 0:
            raise RuntimeError(f"Benchmarks: {','.join(not_available)} are not available.")
        group_exe_name = GroupTypeExeMaps[group.group_type]
        spider_name = f"{group_exe_name}Catalog"
        btf = ",".join(benchmarks)
        self.crawler.run_one_crawl(spider_name, group.catalog_path,
                                   f"BENCHMARKS_TO_FETCH={btf}"
                                   f"RESULTS_DOWNLOAD_MARK={group.results_download_mark}"
                                   f"CATALOG_ID_FIELD={group.results_id_field}")

        for b in benchmarks:
            try:
                group.verify_results_files(b)
            except NotADirectoryError as nd:
                raise RuntimeError(nd)
        status = []
        for b in benchmarks:
            status.append((b, group.get_catalog_location(b)))
        return status
