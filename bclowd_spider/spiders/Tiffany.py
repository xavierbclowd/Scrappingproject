from datetime import datetime
import json
import re
from urllib.parse import urlencode, urljoin
import scrapy
from scrapy.utils.project import get_project_settings
import tldextract
from bclowd_spider.items import ProductItem
from bclowd_spider.settings import upload_images_to_azure_blob_storage

#Tiffany Spider
class Tiffiny(scrapy.Spider):
    name = "Tiffany"
    category_list = [
        "jewelry",
        "watches",
        "home_and_accessories_home",
        "home_and_accessories_accessories"
    ]
    spec_mapping = [
        {
            "countryCode": "us",
            "language": "en",
            "currencyCode": "USD",
            "codeUrl": "com",
        },
        {"countryCode": "es", "language": "es", "currencyCode": "USD", "codeUrl": "es"},
    ]
    base_url = "https://www.tiffany.com"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    custom_settings = {
        # "DOWNLOADER_MIDDLEWARES": {
        #     'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
        #     'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        #     'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
        # },
        # "RETRY_ENABLED": True,
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 633,
        },
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        "SPIDER_MIDDLEWARES": {
            "scrapy_zyte_api.ScrapyZyteAPISpiderMiddleware": 100,
            "scrapy_zyte_api.ScrapyZyteAPIRefererSpiderMiddleware": 1000,
        },
        "ZYTE_API_TRANSPARENT_MODE": True,
        "DOWNLOAD_DELAY": 1,
        "DOWNLOAD_TIMEOUT": 15,
        "ZYTE_API_ENABLED": True,
        "ZYTE_API_KEY": "717cc1db97b44a8b92e0df892e2e0c26",
        "ADDONS": {
            "scrapy_zyte_api.Addon": 500,
        },
        "CONCURRENT_REQUESTS": 24,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 12,
    }

    api_url = "https://u9oxz6rbmd-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.20.0)%3B%20Browser%20(lite)%3B%20Algolia%20Salesforce%20B2C%20(SFRA)%20(v24.2.0)%3B%20instantsearch.js%20(4.79.2)%3B%20JS%20Helper%20(3.26.0)&x-algolia-api-key=8bf85f80ef243bc2926af2731fca9f6d&x-algolia-application-id=U9OXZ6RBMD"

    post_headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,es;q=0.8,ja;q=0.7",
        "Connection": "keep-alive",
        "Origin": "https://www.tiffany.com",
        "Referer": "https://www.tiffany.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "content-type": "application/x-www-form-urlencoded",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    def extract_domain_domain_url(self, real_url):
        extracted = tldextract.extract(real_url)
        domain_without_tld = extracted.domain
        domain = domain_without_tld
        domain_url = extracted.registered_domain
        return domain, domain_url

    def start_requests(self):
        for link in self.category_list:
            payload = {
                "requests": [
                    {
                        "indexName": "ecommerce_us_products__en_US",
                        "params": f"clickAnalytics=true&facetFilters=%5B%5D&facets=%5B%22*%22%5D&filters=browseOnlineFrom.timestamp%20%3C%3D%201756130400%20AND%20browseOnlineTo.timestamp%20%3E%3D%201756130400%20AND%20categoryFilters%3A{link}&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&maxValuesPerFacet=20&page=0",
                    }
                ]
            }
            json_payload = json.dumps(payload)
            yield scrapy.FormRequest(
                url=self.api_url,
                method="POST",
                headers=self.post_headers,
                body=json_payload,
                callback=self.parse_category,
                cb_kwargs={"link": link},
            )

    def parse_category(self, response, link):
        try:
            product_data_us = json.loads(response.text)
            if product_data_us:
                total_page = int(product_data_us.get("results", [])[0].get("nbPages", 0))
                cur_page = int(product_data_us.get("results", [])[0].get("page", 0))
                if total_page > cur_page:
                    page = cur_page + 1
                    payload = {
                        "requests": [
                            {
                                "indexName": "ecommerce_us_products__en_US",
                                "params": f"clickAnalytics=true&facetFilters=%5B%5D&facets=%5B%22*%22%5D&filters=browseOnlineFrom.timestamp%20%3C%3D%201756130400%20AND%20browseOnlineTo.timestamp%20%3E%3D%201756130400%20AND%20categoryFilters%3A{link}&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&maxValuesPerFacet=20&page={page}",
                            }
                        ]
                    }
                    json_payload = json.dumps(payload)
                    yield scrapy.FormRequest(
                        url=self.api_url,
                        method="POST",
                        headers=self.post_headers,
                        body=json_payload,
                        callback=self.parse_category,
                        cb_kwargs={"link": link},
                    )

                products = product_data_us.get("results", [])[0].get("hits", [])
                for product in products:
                    product_url = product.get("productUrl", {}).get("canonicalUrl")
                    sku_id = product.get("itemMasterId")
                    if product_url:
                        yield scrapy.Request(
                            product_url,
                            callback=self.parse_product,
                            headers=self.headers,
                            dont_filter=True,
                            cb_kwargs={
                                "sku_id": sku_id,
                                "product_url": product_url.replace(self.base_url, ""),
                            },
                        )
        except Exception as e:
            print(e)
            pass

    def parse_product(self, response, sku_id, product_url):
        if response.status in [404]:
            return
        color = ""
        description = ""
        sale_price = ""
        material = None
        secondary_material = ""
        image_urls = []
        script_tag_content = response.css(
            'script[type="application/ld+json"]::text'
        ).getall()
        if script_tag_content:
            for json_content in script_tag_content:
                try:
                    json_data = json.loads(json_content)
                    description = json_data.get("description", '')
                    color = json_data.get("color", '')
                    material = json_data.get("material", None)
                    if "offers" in json_data:
                        offer = json_data["offers"]
                        sale_price = offer.get("price")
                    if "image" in json_data:
                        images = json_data.get("image", [])
                        if isinstance(images, list):
                            for image in images:
                                image_urls.append(image.get("contentUrl"))
                        else:
                            image_urls.append(images)
                except Exception as e:
                    continue
        if sale_price:
            size_dimensions = []
            detail = response.css(
                ".product-description__container_detail_list>li>span.product-description__container_list-content::text"
            ).getall()
            list_data = re.split(r"\.(?!\d)", description)
            description = list_data + detail
            lowercase_description = [item.lower() for item in description]
            if lowercase_description is not None:
                size_keywords = {
                    "wrists",
                    "wrist size",
                    "length",
                    "weight",
                    "box size",
                    "wide",
                    "ml",
                    "cm",
                    "mm",
                }
                size_dimensions.extend(
                    [
                        item
                        for item in lowercase_description
                        if any(keyword in item for keyword in size_keywords)
                    ]
                )
            pro_color = ""
            if color:
                pro_color = "".join(color)
            list_img = []
            for relative_url in image_urls:
                absolute_url = urljoin(self.base_url, relative_url)
                list_img.append(absolute_url)
            is_production = get_project_settings().get("IS_PRODUCTION")
            product_images_info = []
            if is_production:
                product_images_info = upload_images_to_azure_blob_storage(
                    self, list_img
                )
            else:
                product_images_info = list_img
            collection = ""
            domain, domain_url = self.extract_domain_domain_url(response.url)
            time_stamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            product_collection = response.css(
                ".product-description__content_eyebrow>span>span::text"
            ).get()
            if product_collection:
                collection = product_collection.strip()
            content = {}
            specification = {}

            country_code = self.spec_mapping[0].get("countryCode").lower()
            language_code = self.spec_mapping[0].get("language").lower()

            content[language_code], specification[country_code] = (
                self.get_content_and_specification(
                    response, country_code, language_code
                )
            )
            if len(self.spec_mapping) == 1:
                item = ProductItem()
                item["date"] = time_stamp
                item["domain"] = domain
                item["domain_url"] = domain_url
                item["collection_name"] = collection
                item["brand"] = "Tiffiny Co."
                item["manufacturer"] = "Tiffiny"
                item["product_badge"] = ""
                item["sku"] = sku_id
                item["sku_color"] = pro_color
                item["main_material"] = material
                item["secondary_material"] = secondary_material
                item["image_url"] = product_images_info
                item["size_dimensions"] = size_dimensions
                item["content"] = content
                item["specification"] = specification
                yield item
                return

            for i, item in enumerate(self.spec_mapping):
                if i > 0:
                    b_url = self.base_url.split("com")[0]
                    country_code = item.get("countryCode").lower()
                    language_code = item.get("language").lower()
                    url_country_code = item.get("codeUrl")
                    if url_country_code == "ca":
                        specific_url = (
                            f"https://fr.tiffany.{url_country_code}{product_url}"
                        )
                    else:
                        specific_url = f"{b_url}{url_country_code}{product_url}"
                    yield scrapy.Request(
                        specific_url,
                        callback=self.parse_another_country,
                        headers=self.headers,
                        dont_filter=True,
                        meta={
                            "handle_httpstatus_list": [
                                301,
                                302,
                                307,
                                403,
                                404,
                                410,
                                430,
                                500,
                                503,
                            ]
                        },
                        cb_kwargs={
                            "time_stamp": time_stamp,
                            "domain": domain,
                            "domain_url": domain_url,
                            "collection": collection,
                            "pro_color": pro_color,
                            "sku_id": sku_id,
                            "material": material,
                            "secondary_material": secondary_material,
                            "product_images_info": product_images_info,
                            "size_dimensions": size_dimensions,
                            "language_code": language_code,
                            "country_code": country_code,
                            "content": content,
                            "specification": specification
                        },
                    )

    def parse_another_country(
        self,
        response,
        time_stamp,
        domain,
        domain_url,
        collection,
        pro_color,
        sku_id,
        material,
        secondary_material,
        product_images_info,
        size_dimensions,
        language_code,
        country_code,
        content,
        specification
    ):
        if response.status != 200:
            item = ProductItem()
            item["date"] = time_stamp
            item["domain"] = domain
            item["domain_url"] = domain_url
            item["collection_name"] = collection
            item["brand"] = "Tiffiny Co."
            item["manufacturer"] = "Tiffiny"
            item["product_badge"] = ""
            item["sku"] = sku_id
            item["sku_color"] = pro_color
            item["main_material"] = material
            item["secondary_material"] = secondary_material
            item["image_url"] = product_images_info
            item["size_dimensions"] = size_dimensions
            item["content"] = content
            item["specification"] = specification
            yield item
            return

        content[language_code], specification[country_code] = (
            self.get_content_and_specification(response, country_code, language_code)
        )

        if len(specification) >= len(self.spec_mapping):
            item = ProductItem()
            item["date"] = time_stamp
            item["domain"] = domain
            item["domain_url"] = domain_url
            item["collection_name"] = collection
            item["brand"] = "Tiffiny Co."
            item["manufacturer"] = "Tiffiny"
            item["product_badge"] = ""
            item["sku"] = sku_id
            item["sku_color"] = pro_color
            item["main_material"] = material
            item["secondary_material"] = secondary_material
            item["image_url"] = product_images_info
            item["size_dimensions"] = size_dimensions
            item["content"] = content
            item["specification"] = specification
            yield item

    def get_content_and_specification(self, response, country_code, language_code):
        sku_title = ""
        sku_short_description = ""
        sku_long_description = ""
        script_tag_content = response.css(
            'script[type="application/ld+json"]::text'
        ).getall()
        if script_tag_content:
            for json_content in script_tag_content:
                json_data = json.loads(json_content)
                if "description" in json_data:
                    sku_short_description = json_data.get("description")
                    sku_title = json_data.get("name")
                    material = json_data.get("material")
                    if material:
                        sku_long_description = (
                            f"{sku_short_description} {material}"
                        )
                    else:
                        sku_long_description = f"{sku_short_description}"
        else:
            sku_title = response.css(
                ".product-description__content_title>span::text"
            ).get()
            sku_short_description = response.css(
                ".product-description__container_long-desc::text"
            ).get()
            list_desc = response.css(
                ".product-description__container_detail_list>li>span.product-description__container_list-content::text"
            ).getall()
            sku_long_description = f"{sku_short_description} {' '.join(list_desc)}"

        size_available = response.xpath('//*[@id="menu2"]/li[1]/div/a/span').extract()
        currency_codes = ""
        sale_price = ""
        script_tag_content = response.css(
            'script[type="application/ld+json"]::text'
        ).getall()
        if script_tag_content:
            for json_content in script_tag_content:
                json_data = json.loads(json_content)
                if "offers" in json_data:
                    offer = json_data["offers"]
                    sale_price = offer.get("price")
                    currency_codes = offer.get("priceCurrency")
                    break
        availability = response.css(
            "div.product-description__buttons tiffany-pdp-buttons"
        ).extract()
        if availability is not None:
            availability_status = "Yes"
            out_of_stock_text = "Available"
        else:
            availability_status = "No"
            out_of_stock_text = "Temporarily out of stock"

        return (
            {
                "sku_title": sku_title,
                "sku_short_description": sku_short_description,
                "sku_long_description": sku_long_description,
            },
            {
                "lang": language_code,
                "domain_country_code": country_code,
                "currency": currency_codes,
                "base_price": sale_price,
                "sales_price": sale_price,
                "active_price": sale_price,
                "stock_quantity": "",
                "availability": availability_status,
                "availability_message": out_of_stock_text,
                "shipping_lead_time": "",
                "shipping_expenses": "",
                "marketplace_retailer_name": "tiffiny",
                "condition": "NEW",
                "reviews_rating_value": "",
                "reviews_number": "",
                "size_available": size_available,
                "sku_link": response.url,
            },
        )
