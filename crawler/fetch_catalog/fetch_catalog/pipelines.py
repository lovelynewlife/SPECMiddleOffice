# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import io
import logging
import os.path
import csv
from lxml import etree

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class FetchBenchmarkPipeline:

    def __init__(self, root_path, group):
        self.path = os.path.join(root_path, group, "benchmarks.txt")
        self.file = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            root_path=crawler.settings.get('DATA_ROOT_PATH'),
            group=crawler.settings.get('GROUP'),
        )

    def open_spider(self, spider):
        self.file = open(self.path, "w")
        if not self.file:
            logging.error(f"Unable to open file in \"{self.path}\"")
            exit(1)

    def process_item(self, item, spider):
        logging.info(f"writing benchmark: {item}")
        self.file.write(f"{item['name'].strip()}|{item['full_name'].strip()}\n")
        return item

    def close_spider(self, spider):
        self.file.close()


class FetchCatalogPipeline:
    link_field = "Disclosure"
    CATALOG = "catalog"

    # Download url fields all have the following prefix
    DOWNLOAD = "[D]"
    ID = "id"

    def __init__(self, root_path, group):
        self.path = os.path.join(root_path, group, self.CATALOG)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            root_path=crawler.settings.get('DATA_ROOT_PATH'),
            group=crawler.settings.get('GROUP'),
        )

    def process_item(self, item, spider):
        title = item["title"].strip()
        contents = csv.DictReader(io.StringIO(item["content"]))
        field_names_delta = set()
        assert self.link_field in contents.fieldnames

        write_rows = []
        counter = 0

        for elem in contents:
            # Sometimes, "" make a tag's href empty.
            link_fields_clean = elem[self.link_field].replace("\"", "")
            links = etree.HTML(link_fields_clean).xpath("//a")
            for link in links:
                # add Download prefix
                field_name = self.DOWNLOAD + link.xpath("./text()")[0].strip()
                field_names_delta.add(field_name)
                elem[field_name] = link.xpath("./@href")[0]

            elem[self.ID] = counter
            counter += 1
            write_rows.append(elem)

        file_path = os.path.join(self.path, f"{title}.csv")
        field_names = [self.ID]
        field_names.extend(list(contents.fieldnames))
        field_names.extend(field_names_delta)

        with open(file_path, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for row in write_rows:
                writer.writerow(row)
        logging.info(f"write {title} catalog to {file_path}")
