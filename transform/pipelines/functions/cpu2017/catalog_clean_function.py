from pipelines.base.io.handler import IOHandler

import pandas as pd

from urllib.parse import urljoin

from loguru import logger

NULL_STRING = "<null>"
SPEC_base_url = "https://spec.org/"


def catalog_clean_pandas_func(catalog_file: IOHandler, sink: IOHandler):
    logger.info("Cleaning catalog using pandas..")
    df = pd.read_csv(catalog_file.read(), dtype={"Other Cache": "object"})
    df = df.set_index("index")

    new_columns = ['result_id', 'benchmark', 'hardware_vendor', 'system', 'peak_result', 'base_result',
                   'energy_peak_result', 'energy_base_result', 'number_of_cores', 'number_of_chips',
                   'number_of_enabled_threads_per_core', 'processor', 'processor_MHz', 'CPUs_orderable', 'parallel',
                   'base_pointer_size', 'peak_pointer_size', 'cache_1st_level', 'cache_2nd_level', 'cache_3rd_level',
                   'other_cache', 'memory', 'storage', 'operating_system', 'file_system', 'compiler',
                   'hardware_avail_date', 'software_avail_date', 'license', 'tested_by', 'test_sponsor', 'test_date',
                   'published_date', 'updated_date', 'disclosure', 'disclosures', 'csv_link', 'ps_link', 'text_link',
                   'config_link', 'pdf_link', 'html_link']
    df.columns = new_columns

    clean_df = df[
        ['result_id', 'benchmark', 'hardware_vendor', 'system', 'peak_result', 'base_result', 'energy_peak_result',
         'energy_base_result', 'number_of_cores', 'number_of_chips', 'number_of_enabled_threads_per_core', 'processor',
         'processor_MHz', 'CPUs_orderable', 'parallel', 'base_pointer_size', 'peak_pointer_size', 'cache_1st_level',
         'cache_2nd_level', 'cache_3rd_level', 'other_cache', 'memory', 'storage', 'operating_system', 'file_system',
         'compiler', 'hardware_avail_date', 'software_avail_date', 'license', 'tested_by', 'test_sponsor', 'test_date',
         'published_date', 'updated_date', 'csv_link', 'ps_link', 'text_link', 'config_link', 'pdf_link',
         'html_link']].copy()

    clean_df.published_date = pd.to_datetime(clean_df.published_date, format="mixed")
    clean_df.updated_date = pd.to_datetime(clean_df.updated_date, format="mixed")
    clean_df.test_date = pd.to_datetime(clean_df.test_date, format="mixed")
    clean_df.hardware_avail_date = pd.to_datetime(clean_df.hardware_avail_date, format="mixed")
    clean_df.software_avail_date = pd.to_datetime(clean_df.software_avail_date, format="mixed")

    clean_df.parallel = clean_df.parallel.map({
        'Yes': True,
        'No': False
    })
    clean_df.parallel = clean_df.parallel.astype("bool")

    clean_df.peak_pointer_size = clean_df.peak_pointer_size.str.replace("Not Applicable", NULL_STRING)

    clean_df.cache_1st_level = clean_df.cache_1st_level.str.replace("redacted", NULL_STRING)

    clean_df.cache_2nd_level = clean_df.cache_2nd_level.str.replace("redacted", NULL_STRING)

    clean_df.cache_3rd_level = clean_df.cache_3rd_level.str.replace("redacted", NULL_STRING)

    clean_df.CPUs_orderable = clean_df.CPUs_orderable.fillna(NULL_STRING)
    cpus_orderable_trim = clean_df.CPUs_orderable.str.replace(" ", "").str.lower()
    clean_df.CPUs_orderable = cpus_orderable_trim

    clean_df.test_sponsor = clean_df.test_sponsor.str.replace("Dell Inc.", "Dell Inc").str.replace("Dell Inc",
                                                                                                   "Dell Inc.")

    clean_df.text_link = clean_df.text_link.apply(lambda x: urljoin(SPEC_base_url, x))
    clean_df.csv_link = clean_df.csv_link.apply(lambda x: urljoin(SPEC_base_url, x))
    clean_df.ps_link = clean_df.ps_link.apply(lambda x: urljoin(SPEC_base_url, x))
    clean_df.config_link = clean_df.config_link.apply(lambda x: urljoin(SPEC_base_url, x))
    clean_df.pdf_link = clean_df.pdf_link.apply(lambda x: urljoin(SPEC_base_url, x))
    clean_df.html_link = clean_df.html_link.apply(lambda x: urljoin(SPEC_base_url, x))

    date_df = clean_df[['hardware_avail_date', 'software_avail_date', 'test_date', 'published_date', 'updated_date']]
    clean_df[['hardware_avail_date', 'software_avail_date', 'test_date', 'published_date',
              'updated_date']] = date_df.applymap(lambda x: x.strftime("%Y-%m"))

    clean_df.system.fillna(NULL_STRING, inplace=True)
    clean_df.other_cache.fillna(NULL_STRING, inplace=True)

    output_dict = clean_df.to_dict('records')

    logger.info("Exporting clean catalog file to mongodb..")
    sink.write(output_dict, drop_collection=True)

    logger.info("Done.")

