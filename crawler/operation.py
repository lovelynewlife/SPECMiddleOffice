from executor import BenchmarkExecutor
from storage import DataStorage, BenchmarkGroup, BenchmarkGroups


class OperationsHelper:

    def __init__(self):
        self.__group = None
        self.__executor = None

    @property
    def group(self):
        return self.__group

    @group.setter
    def group(self, group):
        self.__group = group

    @property
    def executor(self):
        return self.__executor

    @executor.setter
    def executor(self, executor):
        self.__executor = executor


class BenchmarkGroupOperation:
    def __init__(self, helper: OperationsHelper):
        self.__helper = helper

    @property
    def __group(self) -> BenchmarkGroup:
        return self.__helper.group

    @property
    def __executor(self) -> BenchmarkExecutor:
        return self.__helper.executor

    # Operations in console.
    def fetch_benchmarks(self):
        try:
            res = self.__executor.execute_fetch_benchmarks(self.__group)
            print(f"{len(res)} Benchmarks available.")
        except RuntimeError as err:
            print(err)

    def fetch_one_catalog(self, benchmark: str):
        self.fetch_catalogs([benchmark])

    def fetch_catalogs(self, benchmarks: list):
        try:
            res = self.__executor.execute_fetch_catalogs(self.__group, benchmarks)
            print("Fetching Status:")
            for b, location in res:
                print(f"{b}: {location}")
        except RuntimeError as err:
            print(err)

    def show_supported_results_types(self, benchmark: str):
        try:
            res = self.__executor.execute_show_supported_types(self.__group, benchmark)
            print(f"{benchmark} Supported filetypes:")
            for elem in res:
                print(elem)
        except RuntimeError as err:
            print(err)

    def show_available_benchmarks(self):
        benchmarks = self.__group.get_benchmarks()
        print(f"{len(benchmarks)} benchmarks available:")
        for b in benchmarks:
            print(b)

    def show_downloadable_benchmark(self):
        benchmarks = self.__group.get_benchmarks()
        bs = []
        for b in benchmarks:
            cl = self.__group.get_catalog_location(b)
            if cl is not None:
                bs.append(b)
        print(f"{len(bs)} benchmarks downloadable:")
        for b in bs:
            print(b)

    def download_all_results(self, benchmark: str, filetype: str):
        try:
            self.__executor.execute_download_results(self.__group, benchmark, filetype)
        except RuntimeError as err:
            print(err)

    def download_lost_results(self, benchmark: str, filetype: str):
        try:
            self.__executor.execute_download_lost_results(self.__group, benchmark, filetype)
        except RuntimeError as err:
            print(err)

    # Programming APIs with return value.
    def get_catalog_location(self, benchmark: str):
        return self.__group.get_catalog_location(benchmark)

    def get_one_result_location(self, benchmark: str, file_type: str, index: int):
        locations = self.get_result_locations(benchmark, file_type, [index])
        if locations is not None:
            if len(locations) >= 1:
                return locations[0]
        return None

    def get_result_locations(self, benchmark: str, file_type: str, indices: list):
        locations = []
        try:
            locations = self.__executor.execute_get_result_locations(self.__group, benchmark, file_type, indices)
        except RuntimeError as err:
            print(err)
        finally:
            return locations


class BenchmarkGroupOperationWrapper:

    def __init__(self, helper: OperationsHelper, operation: BenchmarkGroupOperation):
        self.helper = helper
        self.operation = operation


class Operations:
    def __init__(self, storage: DataStorage):
        self.__op_helper = OperationsHelper()
        self.__group_op = None
        self.__storage = storage

    @property
    def group(self):
        if self.__group_op:
            return self.__group_op.operation
        else:
            return None

    @group.setter
    def group(self, value):
        raise PermissionError("Unsupported operation.")

    def use_group(self, group_name: str):
        try:
            loaded = self.__storage.load_group(group_name)
            self.__op_helper.group = loaded
        except RuntimeError as err:
            print(err)
            return

        # May need a factory class to build executor.
        if self.__op_helper.group.group_type in BenchmarkGroups:
            self.__op_helper.executor = BenchmarkExecutor()
            self.__group_op = BenchmarkGroupOperationWrapper(self.__op_helper,
                                                             BenchmarkGroupOperation(self.__op_helper))
            print(f"Load group {group_name} ok.")
        else:
            # on purpose, for debugging. 
            raise RuntimeError("Group type is not supported.")

    def drop_group(self, group_name: str):
        try:
            cur_group_name = self.__storage.current_group.group_name
            self.__storage.delete_group(group_name)
            if cur_group_name == group_name:
                self.__group_op = None
            print(f"Drop {group_name} ok.")
        except RuntimeError as err:
            print(err)

    def rename_group(self, group_name: str, new_name: str):
        try:
            self.__storage.rename_group(group_name, new_name)
            print(f"Rename {group_name} ok.")
        except RuntimeError as err:
            print(err)

    def show_group(self, group_name: str):
        group_info = self.__storage.get_groups()
        if group_info.has_key(group_name):
            print(f"{group_name}: {group_info[group_name]}")
        else:
            print(f"{group_name} not exists.")

    def list_groups(self):
        group_info = self.__storage.get_groups()
        for k, v in group_info.items():
            print(f"{k}: {v}")

    def create_group(self, group_name: str, group_type: int):
        try:
            self.__storage.create_group(group_name, group_type)
            print(f"Create {group_name} ok.")
        except RuntimeError as err:
            print(err)

    def dump_garbage(self, confirm: bool = False):
        if not confirm:
            print("Delete permanently need set confirm to True.")
            return
        self.__storage.dump_garbage()
        print("Dump garbage ok.")

    @staticmethod
    def help():
        print("help")
