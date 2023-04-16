import csv
import os.path
import shutil


class LocalGroupDirectory:
    __CATALOG = "catalog"
    __RESULTS = "results"
    __BENCHMARKS = "benchmarks.txt"
    __DOWNLOAD_PREFIX = "[D]"
    __ID = "id"
    __INDEX = "index"

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

    def get_results_file_dir(self, benchmark, filetype):
        return os.path.join(self.get_results_path(benchmark), filetype)

    @staticmethod
    def remove_catalog_suffix(file_name):
        return file_name.rstrip(".csv")

    @staticmethod
    def try_load_existed(data_dir, group="OSG"):
        new_ds = LocalGroupDirectory(data_dir, group)
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

    def get_supported_file_types(self, benchmark):
        pass

    def rebuild_results(self, benchmark):
        catalog_file_path = self.get_catalog_file_path(benchmark)
        results_dir = self.get_results_path(benchmark)
        if os.path.exists(catalog_file_path):
            assert os.path.isdir(results_dir)
            shutil.rmtree(results_dir)
        os.mkdir(results_dir)

        with open(catalog_file_path, "r") as cf:
            csv_file = csv.DictReader(cf)
            field_names = csv_file.fieldnames
            download_fields = filter(lambda x: str(x).startswith(self.__DOWNLOAD_PREFIX), field_names)
            download_fields = map(lambda x: str(x).lstrip(self.__DOWNLOAD_PREFIX), download_fields)
            for field in download_fields:
                file_type_path = os.path.join(results_dir, field)
                os.mkdir(file_type_path)

    def verify_catalogs(self):
        assert os.path.isdir(self.catalog_path)
        catalogs = os.listdir(self.catalog_path)
        files_removing = list()
        for elem in catalogs:
            benchmark = self.remove_catalog_suffix(elem)
            if benchmark not in self.benchmarks:
                file_removing = self.get_catalog_file_path(benchmark)
                if not os.path.isfile(file_removing):
                    raise FileNotFoundError(f"{file_removing} is not a file.")
                files_removing.append(file_removing)

        for elem in files_removing:
            os.remove(elem)

    def verify_results(self):
        assert os.path.isdir(self.catalog_path)
        assert os.path.isdir(self.results_path)
        catalogs = os.listdir(self.catalog_path)
        catalogs = list(map(self.remove_catalog_suffix, catalogs))
        results = os.listdir(self.results_path)

        removing = list()
        for elem in results:
            if elem not in catalogs:
                path_removing = self.get_results_path(elem)
                if not os.path.isdir(path_removing):
                    raise NotADirectoryError(f"{path_removing} is not a dir.")
                removing.append(path_removing)
        for elem in removing:
            shutil.rmtree(elem)

    def verify_results_files(self, benchmark):
        assert os.path.isdir(self.catalog_path)
        assert os.path.isdir(self.results_path)
        catalog_file_path = self.get_catalog_file_path(benchmark)
        result_ids = set()
        with open(catalog_file_path, "r") as cf:
            csv_file = csv.DictReader(cf)
            field_names = csv_file.fieldnames
            file_types = filter(lambda x: str(x).startswith(self.__DOWNLOAD_PREFIX), field_names)
            file_types = map(lambda x: str(x).lstrip(self.__DOWNLOAD_PREFIX), file_types)
            for row in csv_file:
                result_ids.add(row[self.__ID])

        results_files_dirs = map(lambda x: (f".{str(x).lower()}", self.get_results_file_dir(benchmark, x)), file_types)
        dirs_creating = list()
        for suffix, rd in results_files_dirs:
            if os.path.exists(rd):
                if not os.path.isdir(rd):
                    raise NotADirectoryError(f"{rd} is not a dir.")
            else:
                dirs_creating.append(rd)
        for elem in dirs_creating:
            os.mkdir(elem)

        files_removing = list()
        for suffix, rd in results_files_dirs:
            results_files = os.listdir(rd)
            for f in results_files:
                if f.rstrip(suffix) not in result_ids:
                    file_removing = os.path.join(rd, f)
                    if os.path.isfile(file_removing):
                        files_removing.append(file_removing)

        for elem in files_removing:
            os.remove(elem)

    def check_lost_results_files(self, benchmark, file_type):
        lost_files = []
        results_files_dir = self.get_results_file_dir(benchmark, file_type)
        catalog_file_path = self.get_catalog_file_path(benchmark)
        assert os.path.isdir(results_files_dir)
        suffix = f".{str(file_type).lower()}"

        results_files = set(os.listdir(results_files_dir))
        results = set()
        with open(catalog_file_path, "r") as cf:
            csv_file = csv.DictReader(cf)
            field_names = csv_file.fieldnames
            file_types = filter(lambda x: str(x).startswith(self.__DOWNLOAD_PREFIX), field_names)
            file_types = map(lambda x: str(x).lstrip(self.__DOWNLOAD_PREFIX), file_types)
            if str(file_type).upper() not in file_types:
                raise FileNotFoundError("No such file type supported.")
            for row in csv_file:
                results.add(row[self.__INDEX], row[self.__ID])

        for index, rid in results:
            file_lost = f"{rid}{suffix}"
            if file_lost not in results_files:
                lost_files.append((index, file_lost))

        return lost_files

    def __repr__(self):
        return f"<data_root_path: {self.data_root_path}, benchmarks: {self.benchmarks}>"


class DataStorage:
    __ROOT_NAME = "SPEC"
    __GROUP_MAP_FILE = "group.map"

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.group = None

    @staticmethod
    def try_load_existed(data_dir):
        pass
