import scrapy

from ..items import BenchmarkItem


class BenchmarksSpider(scrapy.Spider):
    name = "Benchmarks"
    allowed_domains = ["spec.org"]
    start_urls = ["https://spec.org/cgi-bin/osgresults"]
    custom_settings = {
        "ITEM_PIPELINES": {
            "fetch_catalog.pipelines.FetchBenchmarkPipeline": 300
        }
    }

    def parse(self, response):
        benchmarks = response.xpath('//select[@name="URL"]//option/@value')
        for elem in benchmarks:
            yield BenchmarkItem(name=elem.extract())
