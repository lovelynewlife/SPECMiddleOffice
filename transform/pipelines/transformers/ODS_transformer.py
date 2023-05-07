from typing import Callable
from urllib.parse import urlparse

from pipelines.base.pipeline import Pipeline
from pipelines.base.io import IOHandlerType, IOHandlerFactory

from loguru import logger


ODS_IO_MAP = {
    "": IOHandlerType.LOCAL,
    "pymongo": IOHandlerType.PYMONGO
}


class ODSTransformer(Pipeline):
    NAME = "ODSTransformer"

    def __init__(self, input_path: str, db_config: dict, transform_func: Callable):
        super().__init__()
        self._input_path = input_path
        self._db_config = db_config
        self._transform_func = transform_func
        self._input_handler = None
        self._output_handler = None

    def init(self):
        path_parse = urlparse(self._input_path)
        input_scheme = path_parse.scheme
        self._input_handler = IOHandlerFactory.create(ODS_IO_MAP[input_scheme])
        self._input_handler.open(self._input_path)

        try:
            output_scheme = str(self._db_config["scheme"])
        except KeyError:
            err_msg = "Output Scheme Not Specified."
            logger.error(err_msg)
            raise RuntimeError(err_msg)

        self._output_handler = IOHandlerFactory.create(ODS_IO_MAP[output_scheme])
        self._output_handler.open(self._db_config)

    def close(self):
        if self._input_handler is not None:
            self._input_handler.close()

        if self._output_handler is not None:
            self._output_handler.close()

    def run(self):
        self._transform_func(self._input_handler, self._output_handler)
