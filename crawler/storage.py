import csv
import logging
import os.path
import shutil


def remove_catalog_suffix(file_name):
    return file_name.rstrip(".csv")


class GroupDirectoryStruct:
    __CATALOG = "catalog"
    __RESULTS = "results"
    __BENCHMARKS = "benchmarks.txt"
    __DOWNLOAD_PREFIX = "[D]"

    def __init__(self, data_dir, group="OSG"):
        assert os.path.exists(os.path.exists(data_dir))
        data_dir = os.path.abspath(data_dir)
        self.group = group
        self.data_root_path = os.path.join(data_dir, group)
        self.benchmarks = []

    @property
    def results_path(self):
        return os.path.join(self.data_root_path, self.__RESULTS)

    @property
    def catalog_path(self):
        return os.path.join(self.data_root_path, self.__CATALOG)

    @property
    def benchmarks_file_path(self):
        return os.path.join(self.data_root_path, self.__BENCHMARKS)

    def get_catalog_file_path(self, benchmark):
        return os.path.join(self.catalog_path, f"{benchmark}.csv")

    def get_results_path(self, benchmark):
        return os.path.join(self.results_path, benchmark)

    @staticmethod
    def try_load_existed(data_dir, group="OSG"):
        new_ds = GroupDirectoryStruct(data_dir, group)
        if not os.path.exists(new_ds.data_root_path):
            raise FileNotFoundError("No struct dir exists")
        benchmarks_path = new_ds.benchmarks_file_path
        if not os.path.isfile(benchmarks_path):
            raise FileNotFoundError("No benchmarks.txt file exists")
        new_ds.read_benchmarks()
        catalog_path = new_ds.catalog_path
        results_path = new_ds.results_path
        if not os.path.exists(results_path) or not os.path.exists(catalog_path):
            raise FileNotFoundError("Broken structure.")
        try:
            new_ds.verify_catalogs()
            new_ds.verify_results()
        except FileNotFoundError as e:
            raise e
        except NotADirectoryError as e:
            raise e

        return new_ds

    def build_init(self):
        if not os.path.exists(self.data_root_path):
            os.makedirs(self.data_root_path)
            os.mkdir(os.path.join(self.data_root_path, self.__CATALOG))
            os.mkdir(os.path.join(self.data_root_path, self.__RESULTS))
        else:
            raise FileExistsError(f"Init Building in {self.data_root_path} error, already exists.")

    def read_benchmarks(self):
        assert os.path.isfile(self.benchmarks_file_path)
        self.benchmarks.clear()
        with open(self.benchmarks_file_path, "r") as bf:
            rows = bf.readlines()
            for row in rows:
                b = row.strip().split("|")[0]
                self.benchmarks.append(b)

    def rebuild_results(self, benchmark):
        catalog_file_path = self.get_catalog_file_path(benchmark)
        if os.path.exists(catalog_file_path):
            assert os.path.isdir(catalog_file_path)
            shutil.rmtree(catalog_file_path)
        os.mkdir(catalog_file_path)

        with open(catalog_file_path, "r") as cf:
            csv_file = csv.DictReader(cf)
            field_names = csv_file.fieldnames
            download_fields = list(filter(lambda x: str(x).startswith(self.__DOWNLOAD_PREFIX), field_names))
            for field in download_fields:
                file_type_path = os.path.join(catalog_file_path, field)
                os.mkdir(file_type_path)

    def verify_catalogs(self):
        assert os.path.isdir(self.catalog_path)
        catalogs = os.listdir(self.catalog_path)
        for elem in catalogs:
            benchmark = remove_catalog_suffix(elem)
            if benchmark not in self.benchmarks:
                file_removing = self.get_catalog_file_path(benchmark)
                if not os.path.isfile(file_removing):
                    raise FileNotFoundError(f"{file_removing} is not a file.")
                os.remove(file_removing)

    def verify_results(self):
        assert os.path.isdir(self.catalog_path)
        assert os.path.isdir(self.results_path)
        catalogs = os.listdir(self.catalog_path)
        catalogs = list(map(remove_catalog_suffix, catalogs))
        results = os.listdir(self.results_path)

        for elem in results:
            if elem not in catalogs:
                path_removing = self.get_results_path(elem)
                if not os.path.isdir(path_removing):
                    raise NotADirectoryError(f"{path_removing} is not a dir.")
                shutil.rmtree(path_removing)

    def __repr__(self):
        return f"<data_root_path: {self.data_root_path}, benchmarks: {self.benchmarks}>"


class DataStorage:
    __GROUP_MAP_FILE = "group.map"

    def __init__(self, data_dir):
        self.data_dir = data_dir

    @staticmethod
    def try_load_existed(data_dir):
        pass
