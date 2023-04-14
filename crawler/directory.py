import csv
import logging
import os.path
import shutil


class SPECDirectoryStruct:
    __SPEC = "SPEC"
    __CATALOG = "catalog"
    __RESULTS = "results"
    __BENCHMARKS = "benchmarks.txt"
    __DOWNLOAD_PREFIX = "[D]"

    def __init__(self, data_dir, group="OSG"):
        assert os.path.exists(os.path.exists(data_dir))
        data_dir = os.path.abspath(data_dir)
        self.group = group
        self.data_root_path = os.path.join(data_dir, self.__SPEC, group)
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

    def remove_catalog_suffix(self, file_name):
        return file_name.rstrip(".csv")

    @staticmethod
    def load_exists(data_dir, group="OSG"):
        new_ds = SPECDirectoryStruct(data_dir, group)
        if not os.path.exists(new_ds.data_root_path):
            raise FileNotFoundError("No struct dir exists")
        benchmarks_path = new_ds.benchmarks_file_path
        if not os.path.isfile(benchmarks_path):
            raise FileNotFoundError("No benchmarks.txt file exists")
        new_ds.read_benchmarks()
        catalog_path = new_ds.catalog_path
        results_path = new_ds.results_path
        if not os.path.exists(results_path) or not os.path.exists(catalog_path):
            raise FileNotFoundError("Cannot launch exists, broken structure.")
        new_ds.verify_benchmarks()
        new_ds.verify_catalogs()

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

    def build_results(self, benchmark):
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

    def verify_benchmarks(self):
        assert os.path.isdir(self.catalog_path)
        catalogs = os.listdir(self.catalog_path)
        for elem in catalogs:
            benchmark = self.remove_catalog_suffix(elem)
            if benchmark not in self.benchmarks:
                file_removing = self.get_catalog_file_path(benchmark)
                assert os.path.isfile(file_removing)
                os.remove(file_removing)

    def verify_catalogs(self):
        assert os.path.isdir(self.catalog_path)
        assert os.path.isdir(self.results_path)
        catalogs = os.listdir(self.catalog_path)
        catalogs = list(map(self.remove_catalog_suffix, catalogs))
        results = os.listdir(self.results_path)

        for elem in results:
            if elem not in catalogs:
                path_removing = self.get_results_path(elem)
                assert os.path.isdir(path_removing)
                shutil.rmtree(path_removing)

    def __repr__(self):
        return f"<data_root_path: {self.data_root_path}, benchmarks: {self.benchmarks}>"


if __name__ == '__main__':
    ds = SPECDirectoryStruct("/home/uw2/data")
    ds.build_init()
