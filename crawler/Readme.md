# Crawler for spec.org
## About
v0.1 release

It's a crawler tool which supports:
* Crawl benchmarks, benchmark catalogs.
* Download benchmark results reports.
* Data Management for the raw data.
* Python console shell.
* Notebook Data Exploratory APIs. 

## Install Requirements
On ubuntu20.04
```shell
sudo apt-get install libssl-dev libcurl4-openssl-dev curl gcc python3-dev
pip install -r requirements.txt
```

## TODO List
* Add Unit tests.
* Better APIs.
* Add Rich colorful printer.
* Other storage engines support.

## How to use it
### Import
from SPECrawling import SPEC, FileType, GroupType
### APIs
#### Launcher
SPEC.open(self, data_path, mode="local")
#### Storage Operation
SPEC.op.use_group(self, group_name: str)

SPEC.op.drop_group(self, group_name: str)

SPEC.op.rename_group(self, group_name: str, new_name: str)

SPEC.op.show_group(self, group_name: str)

SPEC.op.list_groups(self)

SPEC.op.create_group(self, group_name: str, group_type: int)

SPEC.op.dump_garbage(self, confirm: bool = False)

#### Group Operation
SPEC.op.group.fetch_one_catalog(self, benchmark: str)

SPEC.op.group.fetch_catalogs(self, benchmarks: list)

SPEC.op.group.fetch_all_catalogs(self)

SPEC.op.group.show_supported_results_types(self, benchmark: str)

SPEC.op.group.show_available_benchmarks(self)

SPEC.op.group.show_downloadable_benchmark(self)

SPEC.op.group.download_all_results(self, benchmark: str, filetype: str)

SPEC.op.group.download_lost_results(self, benchmark: str, filetype: str)

SPEC.op.group.get_catalog_location(self, benchmark: str)

SPEC.op.group.get_one_result_location(self, benchmark: str, file_type: str, index: int)

SPEC.op.group.get_result_locations(self, benchmark: str, file_type: str, indices: list)

SPEC.op.group.get_result_dir(self, benchmark: str, file_type: str)


