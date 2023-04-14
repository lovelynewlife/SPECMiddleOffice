import concurrent.futures
import csv
import logging
import os.path
import queue
import time
from urllib.parse import urljoin

import requests


class BoundThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, max_workers=None):
        super().__init__(max_workers)
        self._work_queue = queue.Queue(self._max_workers * 32)


class ResultsDownloader:

    def __init__(self):
        self.executor = BoundThreadPoolExecutor()

    def download_results(self, result_place_dir, id_urls: dict, file_type: str):
        assert os.path.isdir(result_place_dir)

        def do_download(file_url):
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/91.0.4472.114 Safari/537.36'
            }
            res = requests.get(file_url, headers)
            if res.status_code == requests.codes.ok:
                return res.content
            else:
                logging.warning(f"request url:{file_url} not ok")
                return None

        def do_save(file_id):
            def save(content):
                res = content.result()
                if res:
                    save_path = os.path.join(result_place_dir, f"{file_id}.{file_type.lower()}")
                    with open(save_path, "wb") as sf:
                        sf.write(res)
                    print(f"save {save_path}.")
                else:
                    logging.error(f"unable to download {id_urls[file_id]}")

            return save

        tasks = list()
        max_tasks = 4096
        for k, v in id_urls.items():
            task = self.executor.submit(do_download, v)
            task.add_done_callback(do_save(k))
            tasks.append(task)
            if len(tasks) >= max_tasks:
                # wait for all download tasks done
                for task in tasks:
                    task.result()
            tasks.clear()

        # wait for all download tasks done
        for task in tasks:
            task.result()

    def download_all_types_results(self, result_place_dir, ft_id_urls):
        for ft, id_urls in ft_id_urls.items():
            self.download_results(result_place_dir, id_urls, ft)

    def __del__(self):
        self.executor.shutdown(wait=True)


def main():
    catalog_dir = "/home/uw1/data/SPEC/OSG/catalog"
    result_dir = "/home/uw1/data/SPEC/OSG/results"
    basic_domain = "https://spec.org/"
    catalogs = ["cpu2017.csv"]
    for elem in catalogs:
        cf = os.path.join(catalog_dir, elem)
        id_urls = dict()
        with open(cf, "r") as csvfile:
            catalog_dict = csv.DictReader(csvfile)
            for row in catalog_dict:
                html_url = row["[D]HTML"]
                id_urls[row["id"]] = urljoin(basic_domain, html_url)
        target_dir = os.path.join(result_dir, elem.rstrip(".csv"), "HTML")
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

        d = ResultsDownloader()
        d.download_results(target_dir, id_urls, "HTML")


if __name__ == '__main__':
    main()
