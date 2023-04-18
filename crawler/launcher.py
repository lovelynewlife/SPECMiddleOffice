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

    @property
    def op(self):
        return self.__OP

    @op.setter
    def op(self, value):
        raise PermissionError("Unsupported operation.")

    def set_op(self):
        self.__OP = dict()


__SPEC = Launcher()
SPEC = __SPEC
