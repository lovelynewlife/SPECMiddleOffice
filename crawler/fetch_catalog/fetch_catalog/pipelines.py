# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import os.path

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class FetchBenchmarkPipeline:
    def __init__(self, root_path):
        self.path = os.path.join(root_path, "benchmarks.txt")
        self.file = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            root_path=crawler.settings.get('DATA_ROOT_PATH'),
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
