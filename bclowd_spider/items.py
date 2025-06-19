# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class ProductItem(scrapy.Item):
    date = scrapy.Field()
    domain = scrapy.Field()
    domain_url = scrapy.Field()
    collection_name = scrapy.Field()
    season = scrapy.Field()
    brand = scrapy.Field()
    product_badge = scrapy.Field()
    manufacturer = scrapy.Field()
    gender = scrapy.Field()  # New field for gender
    season = scrapy.Field()  # New field for season
    mpn = scrapy.Field()
    sku = scrapy.Field()
    gtin8 = scrapy.Field()
    gtin12 = scrapy.Field()
    gtin13 = scrapy.Field()
    gtin14 = scrapy.Field()
    main_material = scrapy.Field()
    secondary_material = scrapy.Field()
    sku_color = scrapy.Field()
    size_dimensions = scrapy.Field()
    image_url = scrapy.Field()
    specification = scrapy.Field()
    content = scrapy.Field()
    tags = scrapy.Field()         # New field for tags
    categories = scrapy.Field()   # New field for categories