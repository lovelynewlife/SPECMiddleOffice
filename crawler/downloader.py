import concurrent.futures
import csv
import logging
import os.path
import queue
from urllib.parse import urljoin
from tqdm import trange

import requests
from requests.adapters import HTTPAdapter


class BoundThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, max_workers=None):
        super().__init__(max_workers)
        self._work_queue = queue.Queue(self._max_workers * 32)


class ResultsDownloader:
    __USER_AGENT = 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                   'Chrome/91.0.4472.114 Safari/537.36'

    def __init__(self):
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=5))
        s.mount('https://', HTTPAdapter(max_retries=5))
        self.session = s

    @staticmethod
    def do_save(file_id, result_place_dir, file_type, bar):
        def save(content):
            res = content.result()
            file_name = f"{file_id}.{file_type.lower()}"
            if res:
                save_path = os.path.join(result_place_dir, file_name)
                with open(save_path, "wb") as sf:
                    sf.write(res)
                # print(f"save {save_path}.")
                bar.set_postfix(msg=f"save {file_name}.")
                bar.update()
            else:
                logging.error(f"unable to save {file_name}")

        return save

    def download_results(self, result_place_dir, id_urls: dict, file_type: str):
        assert os.path.isdir(result_place_dir)

        def do_download(file_url):
            headers = {
                'User-Agent': self.__USER_AGENT
            }
            try:
                res = self.session.get(file_url, headers=headers)
                if res.status_code == requests.codes.ok:
                    return res.content
                else:
                    logging.warning(f"request url:{file_url} not ok")
                    return None
            except requests.exceptions.RequestException:
                logging.warning(f"request url:{file_url} request exception")

        task_num = len(id_urls.items())
        max_workers = min(task_num, min(32, (os.cpu_count() or 1) + 4))
        with BoundThreadPoolExecutor(max_workers=max_workers) as executor:
            with trange(task_num, desc=f"Downloading {task_num} {file_type} files") as bar:
                tasks = list()
                max_tasks = 4096
                for k, v in id_urls.items():
                    task = executor.submit(do_download, v)
                    task.add_done_callback(self.do_save(k, result_place_dir, file_type, bar))
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


def main():
    catalog_dir = "/home/uw2/data/SPEC/OSG/catalog"
    result_dir = "/home/uw2/data/SPEC/OSG/results"
    basic_domain = "https://spec.org/"
    catalogs = ["cint95.csv"]
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
