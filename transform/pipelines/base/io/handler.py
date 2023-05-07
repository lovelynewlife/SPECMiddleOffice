class IOHandler:
    def open(self, *args):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def read(self):
        raise NotImplementedError

    def write(self, contents=None, **kwargs):
        raise NotImplementedError

