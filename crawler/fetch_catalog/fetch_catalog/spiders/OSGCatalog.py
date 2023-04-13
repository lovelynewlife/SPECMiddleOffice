import logging
import os.path

import scrapy
from ..items import CatalogItem


class OSGCatalogSpider(scrapy.Spider):
    name = "OSGCatalog"
    allowed_domains = ["spec.org"]
    BASE_URL = "https://spec.org/cgi-bin/osgresults"
    start_urls = [BASE_URL]
    custom_settings = {
        "GROUP": "OSG",
        "ITEM_PIPELINES": {
            "fetch_catalog.pipelines.FetchCatalogPipeline": 300
        }
    }

    def parse(self, response):
        data_root_path = self.crawler.settings.get("DATA_ROOT_PATH")
        group = self.crawler.settings.get("GROUP")
        with open(os.path.join(data_root_path, group, "benchmarks.txt"), "r") as bf:
            benchmarks = bf.readlines()
            for elem in benchmarks:
                conf = elem.strip().split("|")[0]
                query_string = f"conf={conf};op=dump;format=csvdump"
                print(query_string)
                catalog_url = self.BASE_URL + '?' + query_string
                logging.info(f"start crawling catalog {elem.strip()}:{catalog_url}")
                yield scrapy.Request(catalog_url, callback=self.conf_parse_func(conf))

    def conf_parse_func(self, conf):
        def download_catalog(response):
            yield CatalogItem(title=conf, content=response.text)

        return download_catalog
