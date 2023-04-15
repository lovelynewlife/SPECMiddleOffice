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
        print(remain_args)
        cmds = f"scrapy crawl {spider_name} -s DATA_ROOT_PATH={result_path} {remain_args}"
        print(cmds)
        subprocess.run(cmds.split(), cwd=self.working_dir)


def main():
    runner = ScrapyRunner("fetch_catalog")
    runner.run_one_crawl("OSGBenchmarks", "/home/uw2/data/SPEC/OSG", "BENCHMARKS_FILE_NAME=benchmarks.txt")


if __name__ == '__main__':
    main()
