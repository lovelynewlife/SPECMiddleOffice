import logging

import scrapy

from ..items import CatalogItem


def conf_parse_func(conf):
    def download_catalog(response):
        yield CatalogItem(title=conf, content=response.text)

    return download_catalog


class OSGCatalogSpider(scrapy.Spider):
    name = "OSGCatalog"
    allowed_domains = ["spec.org"]
    BASE_URL = "https://spec.org/cgi-bin/osgresults"
    start_urls = [BASE_URL]
    custom_settings = {
        "ITEM_PIPELINES": {
            "fetch_catalog.pipelines.FetchCatalogPipeline": 300
        }
    }

    def parse(self, response):
        benchmarks = self.crawler.settings.get("BENCHMARKS_TO_FETCH")
        benchmarks = benchmarks.split(",")
        for elem in benchmarks:
            conf = elem.strip().split("|")[0]
            query_string = f"conf={conf};op=dump;format=csvdump"
            catalog_url = self.BASE_URL + '?' + query_string
            logging.info(f"start crawling catalog {elem.strip()}:{catalog_url}")
            yield scrapy.Request(catalog_url, callback=conf_parse_func(conf))
