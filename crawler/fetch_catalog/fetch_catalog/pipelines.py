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
        self.file = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            root_path=crawler.settings.get('DATA_ROOT_PATH'),
            filename=crawler.settings.get('BENCHMARKS_FILE_NAME')
        )

    def open_spider(self, spider):
        self.file = open(self.path, "w")
        if not self.file:
            logging.error(f"Unable to open file in \"{self.path}\"")
            exit(1)

    def process_item(self, item: ItemAdapter, spider):
        logging.info(f"writing benchmark: {item}")
        self.file.write(f"{item['name'].strip()}|{item['full_name'].strip()}\n")
        return item

    def close_spider(self, spider):
        self.file.close()


class FetchCatalogPipeline:
    __DOWNLOAD_FIELD = "Disclosure"

    def __init__(self, root_path, dir_name, download_mark, id_field):
        self.path = os.path.join(root_path, dir_name)
        self.matcher = re.compile(r"/(.*)\..*")
        # download url fields all have the following prefix
        self.download_mark = download_mark
        self.id_field = id_field

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            root_path=crawler.settings.get('DATA_ROOT_PATH'),
            dir_name=crawler.settings.get('CATALOG_DIR_NAME'),
            download_mark=crawler.settings.get('RESULTS_DOWNLOAD_MARK'),
            id_field=crawler.settings.get('CATALOG_ID_FIELD'),
        )

    def process_item(self, item: ItemAdapter, spider):
        title = item["title"].strip()
        contents = csv.DictReader(io.StringIO(item["content"]))
        field_names_delta = set()
        assert self.__DOWNLOAD_FIELD in contents.fieldnames

        write_rows = []

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

        file_path = os.path.join(self.path, f"{title}.csv")
        field_names = [self.id_field]
        field_names.extend(list(contents.fieldnames))
        field_names.extend(field_names_delta)

        with open(file_path, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for row in write_rows:
                writer.writerow(row)
        logging.info(f"write {title} catalog to {file_path}")
