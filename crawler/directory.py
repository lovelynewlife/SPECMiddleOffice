import logging
import os.path


class SPECDirectoryStruct:
    __SPEC = "SPEC"
    __CATALOG = "catalog"
    __RESULTS = "results"
    __BENCHMARKS = "benchmarks.txt"

    def __init__(self, data_dir, group="OSG"):
        data_dir = os.path.abspath(data_dir)
        assert os.path.exists(os.path.exists(data_dir))
        self.group = group
        self.data_root_path = os.path.join(data_dir, self.__SPEC, group)
        self.benchmarks = []

    @staticmethod
    def load_exists(data_dir, group="OSG"):
        new_ds = SPECDirectoryStruct(data_dir, group)
        if not os.path.exists(new_ds.data_root_path):
            raise FileNotFoundError("No struct dir exists")
        benchmarks_path = os.path.join(new_ds.data_root_path, new_ds.__BENCHMARKS)
        if not os.path.isfile(benchmarks_path):
            raise FileNotFoundError("No benchmarks.txt file exists")
        catalog_path = os.path.join(new_ds.data_root_path, new_ds.__CATALOG)
        results_path = os.path.join(new_ds.data_root_path, new_ds.__RESULTS)
        if not os.path.exists(results_path) or not os.path.exists(catalog_path):
            raise FileNotFoundError("Cannot launch exists, broken init structure.")
        new_ds.read_benchmarks(benchmarks_path)

        return new_ds

    def build_init(self):
        if not os.path.exists(self.data_root_path):
            os.makedirs(self.data_root_path)
            os.mkdir(os.path.join(self.data_root_path, self.__CATALOG))
            os.mkdir(os.path.join(self.data_root_path, self.__RESULTS))
        else:
            raise FileExistsError(f"Init Building in {self.data_root_path} error, already exists.")

    def read_benchmarks(self, benchmarks_path):
        self.benchmarks.clear()
        with open(benchmarks_path, "r") as bf:
            rows = bf.readlines()
            for row in rows:
                b = row.strip().split("|")[0]
                self.benchmarks.append(b)

    def build_benchmarks(self):
        assert os.path.isdir(self.data_root_path)
        benchmarks_path = os.path.join(self.data_root_path, self.__BENCHMARKS)
        results_path = os.path.join(self.data_root_path, self.__RESULTS)
        assert os.path.isdir(results_path)
        self.read_benchmarks(benchmarks_path)
        results = os.listdir(results_path)
        if len(results) > 0:
            raise EnvironmentError(f"Benchmarks Building in {results_path} error, not empty.")
        for elem in self.benchmarks:
            os.mkdir(os.path.join(results_path, elem))

    def __repr__(self):
        return f"<data_root_path: {self.data_root_path}, benchmarks: {self.benchmarks}>"


if __name__ == '__main__':
    ds = SPECDirectoryStruct("/home/uw2/data")
    ds.build_init()
