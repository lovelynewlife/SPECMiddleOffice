from executor import BenchmarkExecutor
from storage import DataStorage, BenchmarkGroup, GroupType, BenchmarkGroups


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
    def group(self) -> BenchmarkGroup:
        return self.__helper.group

    @property
    def executor(self) -> BenchmarkExecutor:
        return self.__helper.executor

    def fetch_benchmarks(self):
        pass


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

    def use_group(self, group_name):
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

    def drop_group(self, group_name):
        try:
            self.__storage.delete_group(group_name)
        except RuntimeError as err:
            print(err)

    def rename_group(self, group_name, new_name):
        try:
            self.__storage.rename_group(group_name, new_name)
        except RuntimeError as err:
            print(err)

    def show_group(self, group_name):
        pass

    def list_groups(self):
        pass

    def create_group(self, group_name, group_type: GroupType):
        pass

    @staticmethod
    def help():
        print("help")
