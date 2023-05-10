import os

from SPECrawling.operation import Operations
from SPECrawling.storage import LocalDataStorage


class Singleton(object):
    _instance = None

    def __new__(cls, *args, **kw):
        if cls._instance is None:
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls)
        return cls._instance


class Launcher(Singleton):

    def __init__(self):
        self.__OP = None
        self.__storage = None

    @property
    def op(self):
        return self.__OP

    @op.setter
    def op(self, value):
        raise PermissionError("Unsupported operation.")

    def open(self, data_path, mode="local"):
        if mode == "local":
            print(f"Opening: mode: {mode}, data_path: {data_path}.")
            # init Storage
            try:
                storage_load = LocalDataStorage.open_storage(data_path)
                self.__storage = storage_load
            except RuntimeError as err:
                print(err)
                return

            # launch Operations
            self.__OP = Operations(self.__storage)
            print(f"Storage in {data_path} has Launched.")
        else:
            print(f"Unsupported mode:{mode}.")
            return

    @staticmethod
    def help():
        helper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "helper")
        helper_file_path = os.path.join(helper_dir, "main_helper.txt")

        with open(helper_file_path) as file:
            helper = file.read()

        print(helper)


__SPEC = Launcher()
SPEC = __SPEC
