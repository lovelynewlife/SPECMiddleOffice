import csv
import os.path
import shutil


class GroupType:
    OSG = 1  # Open System Group
    HPC = 2  # High Performance Group
    ISG = 3  # International Standards Group
    GWP = 4  # Graphics and Workstation Performance Group
    RG = 5  # Research Group


GroupTypeMaps = {
    GroupType.OSG: "OSG: Open System Group",
    GroupType.HPC: "HPC: High Performance Group",
    GroupType.ISG: "ISG: International Standards Group",
    GroupType.GWP: "GWP: Graphics and Workstation Performance Group",
    GroupType.RG: "RG: Research Group"
}
# BenchmarkGroups = [GroupType.OSG, GroupType.HPC, GroupType.ISG, GroupType.GWP]
BenchmarkGroups = [GroupType.OSG]


class Group:

    @property
    def group_type(self):
        raise NotImplementedError

    @property
    def group_name(self):
        raise NotImplementedError


class BenchmarkGroup(Group):

    @classmethod
    def try_load_existed(cls, data_dir, group_name, group_type):
        raise NotImplementedError

    def build_init(self):
        raise NotImplementedError

    def read_benchmarks(self):
        raise NotImplementedError

    def get_supported_file_types(self, benchmark):
        raise NotImplementedError

    def get_benchmarks(self):
        raise NotImplementedError

    def get_catalog_location(self, benchmark):
        raise NotImplementedError

    def get_results_location(self, benchmark, indices, file_type):
        raise NotImplementedError

    def rebuild_results(self, benchmark):
        raise NotImplementedError

    def verify_catalogs(self):
        raise NotImplementedError

    def verify_results(self):
        raise NotImplementedError

    def verify_results_with_file_type(self, benchmark, file_type):
        raise NotImplementedError

    def verify_results_files(self, benchmark):
        raise NotImplementedError

    def check_lost_results_files(self, benchmark, file_type):
        raise NotImplementedError


class LocalBenchmarkGroup:
    __CATALOG = "catalog"
    __RESULTS = "results"
    __BENCHMARKS = "benchmarks.txt"
    __DOWNLOAD_PREFIX = "[D]"
    __ID = "id"
    __INDEX = "index"

    def __init__(self, data_dir, group_name, group_type=GroupType.OSG):
        assert os.path.exists(os.path.exists(data_dir))
        data_dir = os.path.abspath(data_dir)
        self.__group_name = group_name
        self.__group_type = group_type
        self.__data_root_path = os.path.join(data_dir, group_name)
        self.__benchmarks = []

    @property
    def group_type(self):
        return self.__group_type

    @property
    def group_name(self):
        return self.__group_name

    @property
    def results_path(self):
        return os.path.join(self.__data_root_path, self.__RESULTS)

    @property
    def catalog_path(self):
        return os.path.join(self.__data_root_path, self.__CATALOG)

    @property
    def benchmarks_file_path(self):
        return os.path.join(self.__data_root_path, self.__BENCHMARKS)

    def get_catalog_file_path(self, benchmark):
        return os.path.join(self.catalog_path, f"{benchmark}.csv")

    def get_results_path(self, benchmark):
        return os.path.join(self.results_path, benchmark)

    def get_results_file_dir(self, benchmark, filetype):
        return os.path.join(self.get_results_path(benchmark), filetype)

    @staticmethod
    def remove_catalog_suffix(file_name):
        if file_name.endswith(".csv"):
            return file_name[:-len(".csv")]

    @classmethod
    def try_load_existed(cls, data_dir, group_name, group_type):
        new_ds = cls(data_dir, group_name, group_type)
        if not os.path.exists(new_ds.__data_root_path):
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
        if not os.path.exists(self.__data_root_path):
            os.makedirs(self.__data_root_path)
            bf = open(self.benchmarks_file_path, "w")
            bf.close()
            os.mkdir(self.catalog_path)
            os.mkdir(self.results_path)
        else:
            raise FileExistsError(f"Init Building in {self.__data_root_path} error, already exists.")

    def read_benchmarks(self):
        assert os.path.isfile(self.benchmarks_file_path)
        self.__benchmarks.clear()
        with open(self.benchmarks_file_path, "r") as bf:
            rows = bf.readlines()
            for row in rows:
                b = row.strip().split("|")[0]
                self.__benchmarks.append(b)

    def get_supported_file_types(self, benchmark):
        assert os.path.isdir(self.catalog_path)
        assert os.path.isdir(self.results_path)
        catalog_file_path = self.get_catalog_file_path(benchmark)
        with open(catalog_file_path, "r") as cf:
            csv_file = csv.DictReader(cf)
            field_names = csv_file.fieldnames
            file_types = filter(lambda x: str(x).startswith(self.__DOWNLOAD_PREFIX), field_names)
            file_types = map(lambda x: str(x)[len(self.__DOWNLOAD_PREFIX):],, file_types)

        return list(file_types)

    def get_benchmarks(self):
        return self.__benchmarks

    def get_catalog_location(self, benchmark):
        return self.get_catalog_file_path(benchmark)

    def get_results_location(self, benchmark, indices, file_type):
        catalog_file_path = self.get_catalog_file_path(benchmark)
        res = []
        find_indices = set([str(elem) for elem in indices])
        suffix = f".{str(file_type).lower()}"

        with open(catalog_file_path, "r") as cf:
            csv_file = csv.DictReader(cf)
            field_names = csv_file.fieldnames
            file_types = filter(lambda x: str(x).startswith(self.__DOWNLOAD_PREFIX), field_names)
            file_types = map(lambda x: str(x)[len(self.__DOWNLOAD_PREFIX):], file_types)
            if str(file_type).upper() not in file_types:
                raise FileNotFoundError("No such file type supported.")
            download_field = f"{self.__DOWNLOAD_PREFIX}{str(file_type).upper()}"
            for elem in csv_file:
                rindex = elem[self.__INDEX]
                rid = elem[self.__ID]
                rurl = elem[download_field]
                if rindex in find_indices:
                    filename = f"{rid}{suffix}"
                    full_path = os.path.join(self.get_results_file_dir(benchmark, file_type), file_name)
                    res.append((rindex, f"{full_path}", rurl))

        return res

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
            download_fields = map(lambda x: str(x)[len(self.__DOWNLOAD_PREFIX):], download_fields)
            for field in download_fields:
                file_type_path = os.path.join(results_dir, field)
                os.mkdir(file_type_path)

    def verify_catalogs(self):
        assert os.path.isdir(self.catalog_path)
        catalogs = os.listdir(self.catalog_path)
        files_removing = list()
        for elem in catalogs:
            benchmark = self.remove_catalog_suffix(elem)
            if benchmark not in self.__benchmarks:
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

    def verify_results_with_file_type(self, benchmark, file_type):
        assert os.path.isdir(self.catalog_path)
        assert os.path.isdir(self.results_path)
        catalog_file_path = self.get_catalog_file_path(benchmark)
        result_ids = set()
        with open(catalog_file_path, "r") as cf:
            csv_file = csv.DictReader(cf)
            for row in csv_file:
                result_ids.add(row[self.__ID])

        rd = self.get_results_file_dir(benchmark, file_type)
        suffix = f".{str(file_type).lower()}"
        files_removing = list()

        results_files = os.listdir(rd)
        for f in results_files:
            if f[:-len(suffix)] not in result_ids:
                file_removing = os.path.join(rd, f)
                if os.path.isfile(file_removing):
                    files_removing.append(file_removing)

        for elem in files_removing:
            os.remove(elem)
        return result_ids

    def verify_results_files(self, benchmark):
        assert os.path.isdir(self.catalog_path)
        assert os.path.isdir(self.results_path)
        catalog_file_path = self.get_catalog_file_path(benchmark)
        result_ids = set()
        with open(catalog_file_path, "r") as cf:
            csv_file = csv.DictReader(cf)
            field_names = csv_file.fieldnames
            file_types = filter(lambda x: str(x).startswith(self.__DOWNLOAD_PREFIX), field_names)
            file_types = map(lambda x: str(x)[len(self.__DOWNLOAD_PREFIX):], file_types)
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
                if f[:-len(suffix)] not in result_ids:
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

        results_files = set(os.listdir(results_files_dir))
        results = set()
        with open(catalog_file_path, "r") as cf:
            csv_file = csv.DictReader(cf)
            field_names = csv_file.fieldnames
            file_types = filter(lambda x: str(x).startswith(self.__DOWNLOAD_PREFIX), field_names)
            file_types = map(lambda x: str(x)[len(self.__DOWNLOAD_PREFIX):], file_types)
            if str(file_type).upper() not in file_types:
                raise FileNotFoundError("No such file type supported.")
            for row in csv_file:
                results.add((int(row[self.__INDEX]), row[self.__ID], row[f"{self.__DOWNLOAD_PREFIX}{str(file_type).upper()}"]))

        for index, rid, rurl in results:
            if file_lost not in results_files:
                lost_files.append((index, rid, rurl))

        return lost_files

    def __repr__(self):
        return f"<data_root_path: {self.__data_root_path}, benchmarks: {self.__benchmarks}>"


class DataStorage:

    def __init__(self, store_path):
        self._store_path = store_path

    @classmethod
    def open_storage(cls, store_path):
        raise NotImplementedError

    def load_group(self, group_name):
        raise NotImplementedError

    def get_groups(self):
        raise NotImplementedError

    @property
    def current_group(self):
        raise NotImplementedError

    def create_group(self, group_name, group_type):
        raise NotImplementedError

    def delete_group(self, group_name):
        raise NotImplementedError

    def dump_garbage(self):
        raise NotImplementedError

    def rename_group(self, group_name, new_name):
        raise NotImplementedError


class LocalDataStorage(DataStorage):
    _ROOT_NAME = "SPEC"
    _GROUP_MAP_FILE = "group.map"
    _GARBAGE_MARK = ".del"

    def __init__(self, store_path):
        super().__init__(store_path)
        self.__data_path = os.path.join(os.path.abspath(store_path), self._ROOT_NAME)
        self.__group = None
        self.__metadata = dict()

    @classmethod
    def open_storage(cls, store_path):
        if not os.path.exists(store_path):
            raise RuntimeError(f"store path: {store_path} not found.")
        if not os.path.isdir(store_path):
            raise RuntimeError(f"store path: {store_path} is not a dir.")
        ns = cls(store_path)
        ns.try_load_storage()
        return ns

    def try_load_storage(self):
        if not os.path.exists(self.__data_path):
            raise RuntimeError(f"data root path: {self.__data_path} not found.")
        if not os.path.isdir(self.__data_path):
            raise RuntimeError(f"data root path: {self.__data_path} is not a dir.")

        metadata = os.path.join(self.__data_path, self._GROUP_MAP_FILE)
        if os.path.exists(metadata):
            if not os.path.isfile(metadata):
                raise RuntimeError(f"metadata file: {metadata} not found.")
        else:
            mf = open(self.metadata_file, "w")
            mf.close()

        self.read_metadata()

    @property
    def metadata_file(self):
        return os.path.join(self.__data_path, self._GROUP_MAP_FILE)

    def read_metadata(self):
        self.__metadata.clear()
        with open(self.metadata_file, "r") as mf:
            gmap = mf.readlines()
            for elem in gmap:
                gname, gtype = elem.strip().split(":")
                self.__metadata[gname] = int(gtype)

    def write_metadata(self):
        with open(self.metadata_file, "w") as mf:
            for gname, gtype in self.__metadata.items():
                mf.write(f"{gname}:{gtype}\n")

    # In storage, this method itself is a factory method.
    def load_group(self, group_name):
        load_group_type = None
        for gname, gtype in self.__metadata.items():
            if gname == group_name:
                load_group_type = gtype
        if load_group_type is None:
            raise RuntimeError(f"{group_name} not found.")
        else:
            if load_group_type in BenchmarkGroups:
                try:
                    self.__group = LocalBenchmarkGroup.try_load_existed(self.__data_path, group_name, load_group_type)
                except FileNotFoundError as fn:
                    raise RuntimeError(fn)
                except NotADirectoryError as nd:
                    raise RuntimeError(nd)
            else:
                raise RuntimeError(f"Broken: not supported group type {GroupTypeMaps[load_group_type]}.")
        return self.current_group

    def get_groups(self):
        group_info = dict()
        for gname, gtype in self.__metadata.items():
            group_info[gname] = GroupTypeMaps[gtype]
        return group_info

    @property
    def current_group(self):
        if self.__group:
            return self.__group
        else:
            return None

    def create_group(self, group_name, group_type):
        for gname, gtype in self.__metadata.items():
            if gname == group_name:
                raise RuntimeError(f"{group_name} already existed.")
        if group_type in BenchmarkGroups:
            g = LocalBenchmarkGroup(self.__data_path, group_name, group_type)
            try:
                g.build_init()
            except FileExistsError as fe:
                raise RuntimeError(fe)

        else:
            raise RuntimeError(f"Unsupported group type: {GroupTypeMaps[group_type]}.")
        self.__metadata[group_name] = group_type
        self.write_metadata()

    def delete_group(self, group_name):
        del_group = None
        for gname, gtype in self.__metadata.items():
            if gname == group_name:
                del_group = group_name
                break
        if del_group is None:
            raise RuntimeError(f"{group_name} not found.")
        if self.current_group and self.current_group.group_name == group_name:
            self.__group = None
        group_path = os.path.join(self.__data_path, del_group)
        new_group_path = os.path.join(self.__data_path, f"{del_group}{self._GARBAGE_MARK}")
        self.__metadata.pop(del_group)
        self.write_metadata()
        os.rename(group_path, new_group_path)

    def dump_garbage(self):
        dirs_deleted = []
        candidates = os.listdir(self.__data_path)
        for elem in candidates:
            if elem.endswith(self._GARBAGE_MARK):
                cpath = os.path.join(self.__data_path, elem)
                if os.path.isdir(cpath):
                    dirs_deleted.append(cpath)
        for elem in dirs_deleted:
            shutil.rmtree(elem)

    def rename_group(self, group_name, new_name):
        re_group = None
        re_group_type = None
        for gname, gtype in self.__metadata.items():
            if gname == group_name:
                re_group = group_name
                re_group_type = gtype
                break
        if re_group is None:
            raise RuntimeError(f"{group_name} not found.")

        for gname, gtype in self.__metadata.items():
            if gname == new_name:
                raise RuntimeError(f"{new_name} already existed.")
        group_path = os.path.join(self.__data_path, re_group)
        assert os.path.isdir(group_path)
        new_group_path = os.path.join(self.__data_path, new_name)
        if os.path.exists(new_group_path):
            raise RuntimeError(f"storage in {new_group_path} already existed.")

        self.__metadata[new_name] = re_group_type
        self.__metadata.pop(re_group)
        self.write_metadata()
        if self.current_group:
            self.current_group.group_name = new_name
        os.rename(group_path, new_group_path)
