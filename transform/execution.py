from pipelines.ODS_transformer import ODSTransformer
from SPECrawling import SPEC

from pipelines.functions.cpu2017.catalog_clean_function import catalog_clean_pandas_func


def execute_cpu2017_catalog_clean():
    SPEC.open("/home/uw1/data")
    SPEC.op.use_group("OSG")
    catalog_path = SPEC.op.group.get_catalog_location("cpu2017")
    db_config = {
        "scheme": "pymongo",
        "host": "127.0.0.1",
        "port": "27017",
        "username": "ODS_SPEC_OSG_CPU2017_Root",
        "password": "SPECOSGCPU2017Root123456",
        "db_name": "ODS_SPEC_OSG_CPU2017",
        "table_name": "Catalog"
    }

    transformer = ODSTransformer(catalog_path, db_config, catalog_clean_pandas_func)

    transformer.init()
    transformer.run()
    transformer.close()


if __name__ == '__main__':
    execute_cpu2017_catalog_clean()
