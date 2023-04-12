import logging
import os.path

import scrapy


class CatalogSpider(scrapy.Spider):
    name = "Catalog"
    allowed_domains = ["spec.org"]
    BASE_URL = "https://spec.org/cgi-bin/osgresults"
    start_urls = [BASE_URL]

    def parse(self, response):
        data_root_path = self.crawler.settings.get("DATA_ROOT_PATH")
        with open(os.path.join(data_root_path, "benchmarks.txt"), "r") as bf:
            benchmarks = bf.readlines()
            for elem in benchmarks:
                conf = elem.strip().split("|")[0]
                query_string = f"conf={conf};op=dump;format=pre"
                print(query_string)
                catalog_url = self.BASE_URL + '?' + query_string
                logging.info(f"start crawling catalog {elem.strip()}:{catalog_url}")
                yield scrapy.Request(catalog_url, callback=self.parse_catalogs)

    def parse_catalogs(self, response):
        print(response)
        pass
