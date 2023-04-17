class SPEC:

    def __init__(self):
        self.__OP = None

    @property
    def op(self):
        return self.__OP

    @op.setter
    def op(self, value):
        raise PermissionError("op cannot be set.")
