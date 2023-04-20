from operation import Operations
from storage import LocalDataStorage


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


__SPEC = Launcher()
SPEC = __SPEC
