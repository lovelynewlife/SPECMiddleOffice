from typing import List, Dict

from pipelines.base.io.handler import IOHandler

import pymongo


class PyMongoHandler(IOHandler):
    def __init__(self):
        self._db_config = dict()

    def open(self, *args):
        db_config_param, = args
        self._db_config.update(db_config_param)

        self._overwrite()

    def _overwrite(self):
        connection_string = self._get_connection_string()

        db_name = self._db_config["db_name"]
        table_name = self._db_config["table_name"]
        db_connection = pymongo.MongoClient(connection_string)

        mdb = db_connection[db_name]

        mdb.drop_collection(table_name)

        db_connection.close()

    def _get_connection_string(self):
        host = self._db_config["host"]
        port = self._db_config["port"]
        username = self._db_config["username"]
        password = self._db_config["password"]
        db_name = self._db_config["db_name"]
        return f"mongodb://{username}:{password}@{host}:{port}/?authMechanism=DEFAULT&authSource={db_name}"

    def read(self):
        return self._get_connection_string()

    def write(self, contents: List[Dict] = None, **kwargs):
        connection_string = self._get_connection_string()

        db_name = self._db_config["db_name"]
        table_name = self._db_config["table_name"]
        db_connection = pymongo.MongoClient(connection_string)

        def insert_docs(**others):
            mdb = db_connection[db_name]

            collection = mdb[table_name]
            if contents is not None:
                collection.insert_many(contents)

        try:
            insert_docs(**kwargs)
        except Exception as e:
            raise e
        finally:
            db_connection.close()

    def close(self):
        self._db_config = dict()
