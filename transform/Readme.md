# SPEC Data Transform

## About
Transforms on SPEC Raw Data using PySpark.
1. Do exploratory Data Analysis(notebook) to determine0 transforming pipeline.
2. The Transformed data are export to MongoDB as the data warehouse storage.
3. Only SPEC CPU2007 Benchmark results are transformed.

## Requirements
* MongoDB server
* Spark (Distribution) (Optional, Local is enough.)
* SPECrawling in the [project]/crawler dir. 
```shell
pip install jupyter pyspark pandas
```