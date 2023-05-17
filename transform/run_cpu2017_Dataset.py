from loguru import logger
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

pg_url_in = "jdbc:postgresql://localhost:5432/spec_dwd"
pg_user_in = "spec_dwd_admin"
pg_password_in = "SPEC_DWD_Admin123456"

pg_url_out = "jdbc:postgresql://localhost:5432/spec_dataset"
pg_user_out = "spec_dataset_admin"
pg_password_out = "SPEC_DATA_Admin123456"

pg_jdbc_jar = "./resources/postgresql-42.6.0.jar"


def write_df2db(df2write, table_suffix):
    table_name = f"dataset_{table_suffix}"
    options = {
        "url": pg_url_out,
        "dbtable": table_name,
        "user": pg_user_out,
        "password": pg_password_out,
        "driver": "org.postgresql.Driver",
        "numPartitions": 20,

    }
    df2write.write.format("jdbc").options(**options).mode("overwrite").save()

    logger.info(f"Write to table: {table_name}")


def run_cpu2017_summary_dataset():
    conf = SparkConf()
    conf.setAppName("cpu2017_summary_dataset")
    conf.setMaster("local[*]")
    conf.set("spark.driver.extraClassPath", pg_jdbc_jar)

    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    read_prop = {
        "user": pg_user_in,
        "password": pg_password_in,
    }
    facts_table = "dwd_benchmark_cpu_benchmark_facts"
    facts_df = spark.read.jdbc(pg_url_in, facts_table, properties=read_prop)
    facts_df.createOrReplaceTempView("dwd_benchmark_cpu_benchmark_facts")

    hardware_info_table = "dwd_benchmark_cpu_system_hardware_info"
    hardware_info_df = spark.read.jdbc(pg_url_in, hardware_info_table, properties=read_prop)

    hardware_info_df.createOrReplaceTempView("dwd_benchmark_cpu_system_hardware_info")

    software_info_table = "dwd_benchmark_cpu_system_software_info"
    software_info_df = spark.read.jdbc(pg_url_in, software_info_table, properties=read_prop)

    software_info_df.createOrReplaceTempView("dwd_benchmark_cpu_system_software_info")

    dataset_df = spark.sql('''
    select dwd_benchmark_cpu_benchmark_facts.result_id, benchmark, dwd_benchmark_cpu_benchmark_facts.system,
       dwd_benchmark_cpu_benchmark_facts.hardware_vendor, dwd_benchmark_cpu_system_hardware_info.hardware_avail_date,
       processor, memory, storage, cache_1st_level, cache_2nd_level, cache_3rd_level, other_cache, processor_mhz,
       number_of_chips, number_of_cores, number_of_enabled_threads_per_core, cpus_orderable, cpu_name, enabled, max_mhz,
       nominal_mhz, hardware_avail_date_year, hardware_avail_date_month,
       dwd_benchmark_cpu_system_software_info.file_system, parallel, software_avail_date, base_pointer_size,
       peak_pointer_size, compiler, operating_system, firmware, other, power_management, system_state,
       software_avail_date_year, software_avail_date_month, test_sponsor, tested_by, test_date, published_date,
       updated_date, license, base_result, peak_result, energy_base_result, energy_peak_result, updated_date_year,
       updated_date_month, test_date_year, test_date_month, published_date_year, published_date_month
from dwd_benchmark_cpu_benchmark_facts, dwd_benchmark_cpu_system_hardware_info, dwd_benchmark_cpu_system_software_info
where dwd_benchmark_cpu_benchmark_facts.result_id = dwd_benchmark_cpu_system_hardware_info.result_id and
      dwd_benchmark_cpu_benchmark_facts.result_id =dwd_benchmark_cpu_system_software_info.result_id;
    ''')

    write_df2db(dataset_df, "cpu2017_summary")


if __name__ == '__main__':
    run_cpu2017_summary_dataset()
