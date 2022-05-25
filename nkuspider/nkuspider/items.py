# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NkuspiderItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    newsUrl = scrapy.Field()
    newsUrlMd5 = scrapy.Field()
    newsFrom = scrapy.Field()
    newsPublishTime = scrapy.Field()
    newsContent = scrapy.Field()
    indexed = scrapy.Field()    # 索引构建过了没有的flag
    anchor_text =  scrapy.Field()
    out_degree = scrapy.Field()

