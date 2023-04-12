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
        options = response.xpath('//select[@name="URL"]//option')
        full_names = options.xpath('./text()').extract()
        names = options.xpath('./@value').extract()
        assert len(full_names) == len(names)
        for full_name, name in zip(full_names, names):
            yield BenchmarkItem(full_name=full_name, name=name)
