from pipelines.base.pipeline import PipelineChain
from pipelines.functions.cpu2017.html_result_extract_clean_function import html_result_extract_clean_app
from pipelines.transformers.ODS_transformer import ODSTransformer
from SPECrawling import SPEC, FileType

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


def execute_cpu2017_html_results_extract():
    SPEC.open("/home/uw1/data")
    SPEC.op.use_group("OSG")
    html_result_dir = SPEC.op.group.get_result_dir("cpu2017", FileType.HTML)
    db_config = {
        "scheme": "pymongo",
        "host": "127.0.0.1",
        "port": "27017",
        "username": "ODS_SPEC_OSG_CPU2017_Root",
        "password": "SPECOSGCPU2017Root123456",
        "db_name": "ODS_SPEC_OSG_CPU2017",
        "table_name": "HTML_Results"
    }

    transformer = ODSTransformer(html_result_dir, db_config, html_result_extract_clean_app)

    transformer.init()
    transformer.run()
    transformer.close()


def execute_cpu2017ODS():
    SPEC.open("/home/uw1/data")
    SPEC.op.use_group("OSG")
    catalog_path = SPEC.op.group.get_catalog_location("cpu2017")
    html_result_dir = SPEC.op.group.get_result_dir("cpu2017", FileType.HTML)
    db_basic_config = {
        "scheme": "pymongo",
        "host": "127.0.0.1",
        "port": "27017",
        "username": "ODS_SPEC_OSG_CPU2017_Root",
        "password": "SPECOSGCPU2017Root123456",
        "db_name": "ODS_SPEC_OSG_CPU2017",
    }

    catalog_db_config = {
        "table_name": "Catalog"
    }
    catalog_db_config.update(db_basic_config)

    chain = PipelineChain()

    chain.add_step(ODSTransformer(catalog_path, catalog_db_config, catalog_clean_pandas_func))

    html_results_db_config = {
        "table_name": "HTML_Results"
    }
    html_results_db_config.update(db_basic_config)

    chain.add_step(ODSTransformer(html_result_dir, html_results_db_config,  html_result_extract_clean_app))

    chain.init()
    chain.run()
    chain.close()


if __name__ == '__main__':
    execute_cpu2017ODS()
