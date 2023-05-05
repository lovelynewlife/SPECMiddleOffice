# SPEC Data Transform

## About
Transforms on SPEC Raw Data using PySpark.
1. Do exploratory Data Analysis(notebook) to determine transforming pipeline.
2. The Transformed data are export to MongoDB as the data warehouse storage.
3. Only SPEC CPU2007 Benchmark results are transformed.

## Requirements
* MongoDB server
* Pyspark, Spark (Distribution) (Optional, Local is enough.)
* SPECrawling in the [project]/crawler dir. 

On Ubuntu 20.04
```shell
cd crawler
pip install .
```
```shell
sudo apt install default-jdk
pip install jupyter pyspark pandas requests matplotlib beautifulsoup4 lxml
```

## Structure
* notebooks: jupyter notebooks for Exploratory Data Analysis
* pipelines: data transform/data science pipelines
* data: intermediate output data