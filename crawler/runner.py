import os
import subprocess


class ScrapyRunner:

    def __init__(self, project):
        self.project = project

    @property
    def working_dir(self):
        root_dir = os.path.dirname(__file__)
        return os.path.join(root_dir, self.project)

    def run_one_crawl(self, spider_name, result_path, *args):
        remain_args = ' '.join([f"-s {arg}" for arg in args])
        cmds = f"scrapy crawl {spider_name} -s DATA_ROOT_PATH={result_path} {remain_args}"
        subprocess.run(cmds.split(), cwd=self.working_dir, stderr=subprocess.STDOUT)


def main():
    runner = ScrapyRunner("fetch_catalog")
    runner.run_one_crawl("OSGCatalog", "/home/uw1/data/SPEC/OSG", "BENCHMARKS_TO_FETCH=cfp95", "CATALOG_DIR_NAME=catalog", "RESULTS_DOWNLOAD_MARK=[D]", "CATALOG_ID_FIELD=id")
    print("Crawler done.")


if __name__ == '__main__':
    main()
