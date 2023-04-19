from executor import BenchmarkExecutor
from storage import DataStorage, BenchmarkGroup


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
        self.__op_helper.group = self.__storage.load_group("OSG")
        self.__group_op = BenchmarkGroupOperationWrapper(self.__op_helper, BenchmarkGroupOperation(self.__op_helper))
        pass

    def drop_group(self):
        pass

    def rename_group(self):
        pass

    def show_group(self, group_name):
        pass

    def list_groups(self):
        pass

    def create_group(self, group_name, group_type):
        pass

    @staticmethod
    def help():
        print("help")
