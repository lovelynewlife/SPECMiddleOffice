from loguru import logger
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as pyspark_funcs
import pyspark.sql.types as pyspark_types

dwd_url_in = "jdbc:postgresql://localhost:5432/spec_dwd"
dwd_user_in = "spec_dwd_admin"
dwd_password_in = "SPEC_DWD_Admin123456"

pg_url_out = "jdbc:postgresql://localhost:5432/spec_tdm"
pg_user_out = "spec_tdm_admin"
pg_password_out = "SPEC_TDM_Admin123456"

pg_jdbc_jar = "./resources/postgresql-42.6.0.jar"


def write_df2db(df2write, table_suffix):
    table_name = f"tdm_{table_suffix}"
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


@pyspark_funcs.udf(returnType=pyspark_types.StringType())
def micro_benchmark_tag(date_col):
    app_area_tags = {
        "500.perlbench_r": "Perl interpreter",
        "600.perlbench_s": "Perl interpreter",
        "502.gcc_r":  "GNU C compiler",
        "602.gcc_s":  "GNU C compiler",
        "505.mcf_r": "Route planning",
        "605.mcf_s": "Route planning",
        "520.omnetpp_r": "Discrete Event simulation-computer network",
        "620.omnetpp_s": "Discrete Event simulation-computer network",
        "523.xalancbmk_r": "XML to HTML conversion via XSLT",
        "623.xalancbmk_s": "XML to HTML conversion via XSLT",
        "525.x264_r": "Video compression",
        "625.x264_s": "Video compression",
        "531.deepsjeng_r": "Artificial Intelligence: alpha-beta tree search (Chess)",
        "631.deepsjeng_s": "Artificial Intelligence: alpha-beta tree search (Chess)",
        "541.leela_r": "Artificial Intelligence: Monte Carlo tree search (Go)",
        "641.leela_s": "Artificial Intelligence: Monte Carlo tree search (Go)",
        "548.exchange2_r": "Artificial Intelligence: recursive solution generator (Sudoku)",
        "648.exchange2_s": "Artificial Intelligence: recursive solution generator (Sudoku)",
        "557.xz_r": "General data compression",
        "657.xz_s": "General data compression",
        "503.bwaves_r": "Explosion modeling",
        "603.bwaves_s": "Explosion modeling",
        "507.cactuBSSN_r": "Physics: relativity",
        "607.cactuBSSN_s": "Physics: relativity",
        "508.namd_r": "Molecular dynamics",
        "510.parest_r": "Biomedical imaging: optical tomography with finite elements",
        "511.povray_r": "Ray tracing",
        "519.lbm_r": "Fluid dynamics",
        "619.lbm_s": "Fluid dynamics",
        "521.wrf_r": "Weather forecasting",
        "621.wrf_s": "Weather forecasting",
        "526.blender_r": "3D rendering and animation",
        "527.cam4_r": "Atmosphere modeling",
        "627.cam4_s": "Atmosphere modeling",
        "628.pop2_s": "Wide-scale ocean modeling (climate level)",
        "538.imagick_r": "Image manipulation",
        "638.imagick_s": "Image manipulation",
        "544.nab_r": "Molecular dynamics",
        "644.nab_s": "Molecular dynamics",
        "549.fotonik3d_r": "Computational Electromagnetics",
        "649.fotonik3d_s": "Computational Electromagnetics",
        "554.roms_r": "Regional ocean modeling",
        "654.roms_s": "Regional ocean modeling",
    }

    return app_area_tags[date_col]

def run_cpu2017_micro_benchmarks_dtm():
    conf = SparkConf()
    conf.setAppName("cpu2017_micro_benchmarks_dtm")
    conf.setMaster("local[*]")
    conf.set("spark.driver.extraClassPath", pg_jdbc_jar)

    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    read_prop = {
        "user": dwd_user_in,
        "password": dwd_password_in,
    }
    facts_table = "dwd_benchmark_cpu_benchmark_base_details_facts"
    facts_df = spark.read.jdbc(dwd_url_in, facts_table, properties=read_prop)
    facts_df.createOrReplaceTempView("dwd_benchmark_cpu_benchmark_base_details_facts")

    benchmarks_df = spark.sql('''
    select DISTINCT main_benchmark, benchmark
from dwd_benchmark_cpu_benchmark_base_details_facts
    ''')

    benchmarks_df = benchmarks_df.withColumns({
        "application_area": micro_benchmark_tag(pyspark_funcs.col("benchmark")),
    })

    write_df2db(benchmarks_df, "cpu2017_micro_benchmarks")


if __name__ == '__main__':
    run_cpu2017_micro_benchmarks_dtm()
