# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import io
import logging
import os.path
import csv
import re

from lxml import etree

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class FetchBenchmarkPipeline:

    def __init__(self, root_path, filename):
        self.path = os.path.join(root_path, filename)
        self.temp_name = os.path.join(root_path, f"{filename}.tmp")
        self.file = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            root_path=crawler.settings.get('DATA_ROOT_PATH'),
            filename=crawler.settings.get('BENCHMARKS_FILE_NAME')
        )

    def open_spider(self, spider):
        self.file = open(self.temp_name, "w")
        if not self.file:
            logging.error(f"Unable to open file in \"{self.path}\"")
            exit(1)

    def process_item(self, item: ItemAdapter, spider):
        logging.info(f"writing benchmark: {item}")
        self.file.write(f"{item['name'].strip()}|{item['full_name'].strip()}\n")
        return item

    def close_spider(self, spider):
        self.file.close()
        if os.path.exists(self.path):
            os.remove(self.path)
        os.rename(self.temp_name, self.path)


class FetchCatalogPipeline:
    __DOWNLOAD_FIELD = "Disclosure"
    __INDEX_FIELD = "index"

    def __init__(self, catalog_path, download_mark, id_field):
        self.path = os.path.abspath(catalog_path)
        self.matcher = re.compile(r"/(.*)\..*")
        # download url fields all have the following prefix
        self.download_mark = download_mark
        self.id_field = id_field

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            catalog_path=crawler.settings.get('DATA_ROOT_PATH'),
            download_mark=crawler.settings.get('RESULTS_DOWNLOAD_MARK'),
            id_field=crawler.settings.get('CATALOG_ID_FIELD'),
        )

    def process_item(self, item: ItemAdapter, spider):
        title = item["title"].strip()
        contents = csv.DictReader(io.StringIO(item["content"]))
        field_names_delta = set()
        assert self.__DOWNLOAD_FIELD in contents.fieldnames

        write_rows = []
        counter = 0

        for elem in contents:
            # sometimes, "" make a tag's href empty.
            link_fields_clean = elem[self.__DOWNLOAD_FIELD].replace("\"", "")
            links = etree.HTML(link_fields_clean).xpath("//a")
            id_candidates = []
            for link in links:
                # add Download prefix
                field_name = self.download_mark + link.xpath("./text()")[0].strip()
                field_names_delta.add(field_name)
                download_path = link.xpath("./@href")[0]
                elem[field_name] = download_path
                # match results identifier from download path.
                id_candidates.extend(self.matcher.findall(download_path))

            assert len(id_candidates)

            # extract results identifier.
            elem[self.id_field] = id_candidates[-1].replace("/", "_")
            write_rows.append(elem)
            # add index field
            elem[self.__INDEX_FIELD] = counter
            counter += 1

        file_path = os.path.join(self.path, f"{title}.csv")
        field_names = [self.__INDEX_FIELD, self.id_field]
        field_names.extend(list(contents.fieldnames))
        field_names.extend(field_names_delta)

        temp_file = f"{file_path}.tmp"
        with open(temp_file, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for row in write_rows:
                writer.writerow(row)
        if os.path.exists(file_path):
            os.remove(file_path)
        os.rename(temp_file, file_path)
        logging.info(f"write {title} catalog to {file_path}")
