from enum import Enum, auto

from pipelines.base.io.handler import IOHandler
from pipelines.base.io.local_handler import LocalFileHandler
from pipelines.base.io.pymongo_handler import PyMongoHandler


class IOHandlerType(Enum):
    LOCAL = auto()
    PYMONGO = auto()


class IOHandlerFactory:
    IOHandlerMap = {
        IOHandlerType.LOCAL: LocalFileHandler,
        IOHandlerType.PYMONGO: PyMongoHandler
    }

    @staticmethod
    def create(handler_type: IOHandlerType) -> IOHandler:
        constructor = IOHandlerFactory.IOHandlerMap[handler_type]
        return constructor()
