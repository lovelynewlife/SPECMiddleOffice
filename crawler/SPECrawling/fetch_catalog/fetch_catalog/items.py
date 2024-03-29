# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BenchmarkItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    full_name = scrapy.Field()
    name = scrapy.Field()


class CatalogItem(scrapy.Item):
    title = scrapy.Field()
    content = scrapy.Field()
