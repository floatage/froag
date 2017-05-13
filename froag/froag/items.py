# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class FroagItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class NewsItem(scrapy.Item): 
    msource = scrapy.Field()
    mtitle = scrapy.Field()
    mintro = scrapy.Field()
    mpic = scrapy.Field()
    mtags = scrapy.Field()
    mtime = scrapy.Field()
    mauthor = scrapy.Field()
    mcontent = scrapy.Field()