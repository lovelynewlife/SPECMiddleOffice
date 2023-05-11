from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as pyspark_funcs
import pyspark.sql.types as pyspark_types

from loguru import logger

NULL_STRING = "<null>"
pg_url = "jdbc:postgresql://localhost:5432/spec_dwd"
pg_user = "spec_dwd_admin"
pg_password = "SPEC_DWD_Admin123456"
pg_jdbc_jar = "./resources/postgresql-42.6.0.jar"

mongo_uri = "mongodb://ODS_SPEC_OSG_CPU2017_Reader:SPECOSGCPU2017Reader123456@192.168.137.128:27017" \
            "/ODS_SPEC_OSG_CPU2017"
html_results_sample_size = 10000


def write_df2db(df2write, table_suffix):
    table_name = f"dwd_benchmark_cpu_{table_suffix}"
    options = {
        "url": pg_url,
        "dbtable": table_name,
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver",
        "numPartitions": 20,

    }
    df2write.write.format("jdbc").options(**options).mode("append").save()

    logger.info(f"Write to table: {table_name}")


def trim_null(row):
    new_row = dict()
    for k, v in row.asDict().items():
        nv = row[k]
        if nv is None or str(nv).strip() in ["<null>", "None", "Null", "", "null", "none", "redacted",
                                             "Not Applicable"]:
            nv = "<null>"
        new_row[k] = nv
    return Row(**new_row)


@pyspark_funcs.udf(returnType=pyspark_types.IntegerType())
def extract_year_from_date(date_col):
    year, _ = date_col.split("-")
    return int(year)


@pyspark_funcs.udf(returnType=pyspark_types.IntegerType())
def extract_month_from_date(date_col):
    _, month = date_col.split("-")
    return int(month)


@pyspark_funcs.udf(returnType=pyspark_types.StringType())
def null_standardize(data_col):
    ret_col = str(data_col)
    if ret_col.strip() in ["<null>", "None", "Null", "", "null", "none", "redacted", "Not Applicable"]:
        ret_col = "<null>"
    return ret_col


@pyspark_funcs.udf(returnType=pyspark_types.StringType())
def trim_chips(data_col):
    import re
    ret_col = str(data_col)
    ret_col = re.sub(r"\s+", "", ret_col)
    ret_col = re.sub(r"chip.*", "chips", ret_col)
    return ret_col


def table_projections(table, cols):
    cols_names = map(lambda x: f'{table}.{x}', cols)
    projs = ",".join(cols_names)
    return projs


def flatten_results(benchmark_type="base"):
    def flatten_results_func(row):
        null_string = "<null>"
        new_rows = []

        result_id = row["result_id"]
        mb = row["main_benchmark"]
        benchmark_results = row[benchmark_type]
        for r in benchmark_results:
            r_dict = r.asDict()
            r_dict_trim_null = {}
            for k, v in r_dict.items():
                if v is None:
                    r_dict_trim_null[k] = [null_string]
                else:
                    r_dict_trim_null[k] = v

            b = r_dict_trim_null["benchmark"]
            copies = r_dict_trim_null["copies"]
            threads = r_dict_trim_null["threads"]
            average_power = r_dict_trim_null["averagepower"]
            energy_kj = r_dict_trim_null["energy(kj)"]
            energy_ratio = r_dict_trim_null["energyratio"]
            maximum_power = r_dict_trim_null["maximumpower"]
            ratio = r_dict_trim_null["ratio"]
            seconds = r_dict_trim_null["seconds"]
            if len(b) == 0 or len(copies) == 0 or len(threads) == 0:
                continue

            b = b[0]
            copies = copies[0]
            threads = threads[0]

            flatten_lens = max([
                len(average_power),
                len(energy_kj),
                len(energy_ratio),
                len(maximum_power),
                len(ratio),
                len(seconds)
            ])

            def flatten_func(v_list, index, field_name):
                if len(v_list) >= index + 1:
                    return {
                        field_name: v_list[index] if v_list[index] != null_string else "0.0"
                    }
                else:
                    return {
                        field_name: "0.0"
                    }

            flatten_row_dict = {}
            for i in range(flatten_lens):
                flatten_row_dict.update(**flatten_func(average_power, i, "average_power"))
                flatten_row_dict.update(**flatten_func(energy_kj, i, "energy_kj"))
                flatten_row_dict.update(**flatten_func(energy_ratio, i, "energy_ratio"))
                flatten_row_dict.update(**flatten_func(maximum_power, i, "maximum_power"))
                flatten_row_dict.update(**flatten_func(ratio, i, "ratio"))
                flatten_row_dict.update(**flatten_func(seconds, i, "seconds"))
                flatten_row_dict.update(benchmark=b)
                flatten_row_dict.update(copies=copies if copies != null_string else "0")
                flatten_row_dict.update(threads=threads if threads != null_string else "0")
                flatten_row_dict.update(result_id=result_id)
                flatten_row_dict.update(main_benchmark=mb)
                new_rows.append(Row(**flatten_row_dict))

        return new_rows

    return flatten_results_func


def run_cpu2017_dwd():
    conf = SparkConf()
    conf.setAppName("cpu2017_dwd")
    conf.setMaster("local[*]")
    conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")
    conf.set("spark.mongodb.read.connection.uri", mongo_uri)
    conf.set("spark.driver.extraClassPath", pg_jdbc_jar)

    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    catalog_df = spark.read.format("mongodb").option("collection", "Catalog").load()
    catalog_df.createOrReplaceTempView("Catalog")

    html_results_df = spark.read.format("mongodb").option("collection", "HTML_Results")\
        .option("sampleSize", html_results_sample_size).load()
    html_results_df.createOrReplaceTempView("HTML_Results")

    benchmark_summary_facts_from_catalog_columns = [
        "result_id", "benchmark", "system", "hardware_vendor",
        "test_sponsor", "tested_by", "test_date", "published_date", "updated_date",
        "license", "base_result", "peak_result", "energy_base_result", "energy_peak_result"
    ]
    projections = table_projections("Catalog", benchmark_summary_facts_from_catalog_columns)
    benchmark_summary_facts_df = spark.sql(f"SELECT {projections} FROM Catalog;")

    benchmark_summary_facts_df = benchmark_summary_facts_df.withColumns(
        {
            "updated_date_year": extract_year_from_date(pyspark_funcs.col("updated_date")),
            "updated_date_month": extract_month_from_date(pyspark_funcs.col("updated_date")),
            "test_date_year": extract_year_from_date(pyspark_funcs.col("test_date")),
            "test_date_month": extract_month_from_date(pyspark_funcs.col("test_date")),
            "published_date_year": extract_year_from_date(pyspark_funcs.col("published_date")),
            "published_date_month": extract_month_from_date(pyspark_funcs.col("published_date")),
        }
    )
    benchmark_summary_facts_df.createOrReplaceTempView("Benchmarks_Summary")

    write_df2db(benchmark_summary_facts_df, "benchmark_facts")

    system_hardware_info_from_catalog_columns = [
        "result_id", "system", "hardware_vendor", "hardware_avail_date",
        "processor", "memory", "storage", "cache_1st_level", "cache_2nd_level",
        "cache_3rd_level", "other_cache", "processor_MHZ", "number_of_chips", "number_of_cores",
        "number_of_enabled_threads_per_core", "CPUs_orderable"
    ]
    system_hardware_info_from_html_columns = [
        "cpu_name", "enabled", "max_mhz", "nominal"
    ]

    system_hardware_info_df = spark.sql(
        f"SELECT {table_projections('Catalog', system_hardware_info_from_catalog_columns)},{table_projections('HTML_Results.system_info.hardware', system_hardware_info_from_html_columns)}  "
        f"From Catalog, HTML_Results WHERE  "
        f"Catalog.result_id=HTML_Results.result_id and  "
        f"HTML_Results.system_info.hardware is not NULL")

    system_hardware_info_df = system_hardware_info_df.withColumnsRenamed(
        {
            "CPUs_orderable": "cpus_orderable",
            "nominal": "nominal_mhz",
            "processor_MHZ": "processor_mhz"
        }
    )
    system_hardware_info_df = system_hardware_info_df.withColumns({
        "nominal_mhz": pyspark_funcs.col("nominal_mhz").cast("long"),
        "processor_mhz": pyspark_funcs.col("processor_mhz").cast("long"),
        "hardware_avail_date_year": extract_year_from_date(pyspark_funcs.col("hardware_avail_date")),
        "hardware_avail_date_month": extract_month_from_date(pyspark_funcs.col("hardware_avail_date")),
    })

    system_hardware_info_df = system_hardware_info_df.withColumns({
        "cpu_name": null_standardize(pyspark_funcs.col("cpu_name")),
        "enabled": null_standardize(pyspark_funcs.col("enabled")),
    })

    system_hardware_info_df = system_hardware_info_df.withColumns(
        {
            "cpus_orderable": trim_chips(pyspark_funcs.col("cpus_orderable"))
        }
    )

    write_df2db(system_hardware_info_df, "system_hardware_info")

    system_software_info_from_catalog_columns = [
        "result_id", "system", "file_system", "parallel",
        "software_avail_date", "base_pointer_size", "peak_pointer_size",
        "compiler", "operating_system"
    ]

    system_software_info_from_html_columns = [
        "firmware", "other", "power_management",
        "system_state"
    ]

    system_software_info_df = spark.sql(
        f"SELECT {table_projections('Catalog', system_software_info_from_catalog_columns)},{table_projections('HTML_Results.system_info.software', system_software_info_from_html_columns)}  "
        f"From Catalog, HTML_Results WHERE  "
        f"Catalog.result_id=HTML_Results.result_id and  HTML_Results.system_info.software is not NULL")

    system_software_info_df = system_software_info_df.withColumns(
        {
            "firmware": null_standardize(pyspark_funcs.col("firmware")),
            "other": null_standardize(pyspark_funcs.col("other")),
            "power_management": null_standardize(pyspark_funcs.col("power_management")),
            "system_state": null_standardize(pyspark_funcs.col("system_state")),
            "software_avail_date_year": extract_year_from_date(pyspark_funcs.col("software_avail_date")),
            "software_avail_date_month": extract_month_from_date(pyspark_funcs.col("software_avail_date"))
        }
    )

    write_df2db(system_software_info_df, "system_software_info")

    system_power_analyzer_info_df = spark.sql(
        "SELECT result_id, HTML_Results.system_info.power_analyzer.* "
        "FROM HTML_Results where HTML_Results.system_info.power_analyzer is not null ;")
    system_power_analyzer_info_df = system_power_analyzer_info_df.withColumnsRenamed({
        "ptdaemon®_version": "ptdaemon_r_version",
        "ptdaemon™_version": "ptdaemon_tm_version"
    })

    system_power_analyzer_info_df = system_power_analyzer_info_df.rdd.map(lambda x: trim_null(x)).toDF()

    write_df2db(system_power_analyzer_info_df, "system_power_analyzer_info")

    system_power_info_df = spark.sql(
        "SELECT result_id, HTML_Results.system_info.power.* FROM HTML_Results "
        "where HTML_Results.system_info.power is not null ;")
    system_power_info_df = system_power_info_df.withColumnsRenamed(
        {
            "max._power_(w)": "max_power_w",
            "idle_power_(w)": "idle_power_w",
            "min._temperature_(c)": "min_temperature_c",
            "elevation_(m)": "elevation_m",
        }
    )
    system_power_info_df = system_power_info_df.rdd.map(lambda x: trim_null(x)).toDF()

    write_df2db(system_power_info_df, "system_power_info")

    base_results_df = spark.sql("SELECT Catalog.result_id, Catalog.benchmark main_benchmark, "
                                "HTML_Results.result_tables.base "
                                "FROM Catalog,HTML_Results "
                                "WHERE Catalog.result_id=HTML_Results.result_id and "
                                "HTML_Results.result_tables.base is not null;")

    base_results_flatten_df = base_results_df.rdd.flatMap(lambda x: flatten_results()(x)).toDF()

    base_results_flatten_df = base_results_flatten_df.withColumns({
        "average_power": pyspark_funcs.col("average_power").cast("double"),
        "energy_kj": pyspark_funcs.col("energy_kj").cast("double"),
        "energy_ratio": pyspark_funcs.col("energy_ratio").cast("double"),
        "maximum_power": pyspark_funcs.col("maximum_power").cast("double"),
        "ratio": pyspark_funcs.col("ratio").cast("double"),
        "seconds": pyspark_funcs.col("seconds").cast("double"),
        "copies": pyspark_funcs.col("copies").cast("int"),
        "threads": pyspark_funcs.col("threads").cast("int"),
    }
    )
    base_results_flatten_df.fillna(0)

    base_results_flatten_df.createOrReplaceTempView("Base_Results")

    benchmark_base_details_facts_from_catalog_columns = [
        "system", "hardware_vendor",
        "test_sponsor", "tested_by", "test_date", "published_date", "updated_date",
        "license", "base_result", "peak_result", "energy_base_result", "energy_peak_result",
        "updated_date_year", "updated_date_month", "test_date_year", "test_date_month", "published_date_year",
        "published_date_month"
    ]

    benchmark_base_details_facts = spark.sql(
        f"SELECT {table_projections('Benchmarks_Summary', benchmark_base_details_facts_from_catalog_columns)},"
        f"Base_Results.* FROM Benchmarks_Summary, Base_Results "
        f"WHERE Benchmarks_Summary.result_id=Base_Results.result_id "
        f"and Benchmarks_Summary.benchmark=Base_Results.main_benchmark")

    write_df2db(benchmark_base_details_facts, "benchmark_base_details_facts")

    peak_results_df = spark.sql(
        "SELECT Catalog.result_id, Catalog.benchmark main_benchmark, HTML_Results.result_tables.peak "
        "FROM Catalog,HTML_Results WHERE "
        "Catalog.result_id=HTML_Results.result_id and HTML_Results.result_tables.peak is not null;")
    peak_results_flatten_df = peak_results_df.rdd.flatMap(lambda x: flatten_results("peak")(x)).toDF()

    peak_results_flatten_df = peak_results_flatten_df.withColumns({
        "average_power": pyspark_funcs.col("average_power").cast("double"),
        "energy_kj": pyspark_funcs.col("energy_kj").cast("double"),
        "energy_ratio": pyspark_funcs.col("energy_ratio").cast("double"),
        "maximum_power": pyspark_funcs.col("maximum_power").cast("double"),
        "ratio": pyspark_funcs.col("ratio").cast("double"),
        "seconds": pyspark_funcs.col("seconds").cast("double"),
        "copies": pyspark_funcs.col("copies").cast("int"),
        "threads": pyspark_funcs.col("threads").cast("int"),
    }
    )
    peak_results_flatten_df.fillna(0)

    peak_results_flatten_df.createOrReplaceTempView("Peak_Results")

    benchmark_peak_details_facts_from_catalog_columns = [
        "system", "hardware_vendor",
        "test_sponsor", "tested_by", "test_date", "published_date", "updated_date",
        "license", "base_result", "peak_result", "energy_base_result", "energy_peak_result",
        "updated_date_year", "updated_date_month", "test_date_year", "test_date_month", "published_date_year",
        "published_date_month"
    ]
    benchmark_peak_details_facts = spark.sql(
        f"SELECT {table_projections('Benchmarks_Summary', benchmark_peak_details_facts_from_catalog_columns)}, "
        f"Peak_Results.* FROM Benchmarks_Summary,Peak_Results "
        f"WHERE Benchmarks_Summary.result_id=Peak_Results.result_id and "
        f"Benchmarks_Summary.benchmark=Peak_Results.main_benchmark")

    write_df2db(benchmark_peak_details_facts, "benchmark_peak_details_facts")

    ordinary_notes_df = spark.sql(
        "SELECT result_id, ordinary_notes.* FROM HTML_Results where ordinary_notes is not null;")

    ordinary_notes_df = ordinary_notes_df.rdd.map(lambda x: trim_null(x)).toDF()

    write_df2db(ordinary_notes_df, "ordinary_notes")

    flag_notes_df = spark.sql("SELECT result_id, flag_notes.* "
                              "FROM HTML_Results "
                              "where flag_notes is not null;")
    flag_notes_df = flag_notes_df.rdd.map(lambda x: trim_null(x)).toDF()

    write_df2db(flag_notes_df, "flag_notes")

    logger.info("done.")


if __name__ == '__main__':
    run_cpu2017_dwd()
