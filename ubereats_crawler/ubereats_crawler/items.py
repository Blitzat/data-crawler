# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class UbereatsCrawlerItem(scrapy.Item):
    # store uuid
    uuid = scrapy.Field()
    # store name
    name = scrapy.Field()
    # store location
    location = scrapy.Field()
    # store category labels
    categories = scrapy.Field()
    # store hours
    hours = scrapy.Field()
    # sections (lunch, breakfirst, dinner)
    sections = scrapy.Field()
    # store reviews
    reviews = scrapy.Field()
    # catalogSectionsMap includes all the menu informations
    catalogSectionsMap = scrapy.Field()
    # Store url
    storeURL = scrapy.Field()
