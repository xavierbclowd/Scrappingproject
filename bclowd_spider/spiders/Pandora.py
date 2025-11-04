from datetime import datetime
import json
import os
from urllib.parse import urljoin
import scrapy
import tldextract
from bclowd_spider.items import ProductItem
from bclowd_spider.settings import upload_images_to_azure_blob_storage
from scrapy.utils.project import get_project_settings


class Pandorac(scrapy.Spider):
    name = "Pandora"
    base_url = ".pandora.net"
    sitemap_url = "https://us.pandora.net/sitemap_0-product.xml"

    custom_settings = {
        # "DOWNLOADER_MIDDLEWARES": {
        #     'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
        #     'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        # },
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
        "DOWNLOAD_DELAY": 2,
        "DOWNLOAD_TIMEOUT": 20,
        "ZYTE_API_ENABLED": True,
        "ZYTE_API_KEY": "67deeec4a8c641c488f63adb54fa512c",
        "ADDONS": {
            "scrapy_zyte_api.Addon": 500,
        },
        "CONCURRENT_REQUESTS": 24,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 12,
    }

    spec_mapping = [
        {
            "country": "United States",
            "countryCode": "US",
            "currencyCode": "USD",
            "codeUrl": "en",
            "locale": "en-US",
        },
        {
            "country": "Spain",
            "countryCode": "ES",
            "currencyCode": "EUR",
            "codeUrl": "es",
            "locale": "es-ES",
        },
    ]

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9,es;q=0.8",
        "priority": "u=0, i",
        "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-bitness": '"64"',
        "sec-ch-ua-full-version": '"137.0.7151.120"',
        "sec-ch-ua-full-version-list": '"Google Chrome";v="137.0.7151.120", "Chromium";v="137.0.7151.120", "Not/A)Brand";v="24.0.0.0"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua-platform-version": '"14.0.0"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    }

    def extract_domain_domain_url(self, real_url):
        extracted = tldextract.extract(real_url)
        domain_without_tld = extracted.domain
        domain = domain_without_tld
        domain_url = extracted.registered_domain
        return domain, domain_url

    def start_requests(self):
        yield scrapy.Request(
            url=self.sitemap_url,
            callback=self.parse_sitemap,
            headers=self.headers,
            dont_filter=True,
        )

    def parse_sitemap(self, response):
        # Extract all product URLs from sitemap
        product_urls = response.css("loc::text").getall()

        # Log the total number of product URLs found
        self.logger.info(f"Found {len(product_urls)} product URLs in sitemap")

        for product_url in product_urls:
            # Extract SKU from URL - typically at the end before .html
            sku_match = product_url.split("/")[-1].replace(".html", "")
            # Handle URL patterns like /products/charm-abc-123.html
            if "-" in sku_match:
                sku_parts = sku_match.split("-")
                # Try to find SKU-like pattern (numbers/codes)
                sku_id = sku_parts[-1] if sku_parts[-1].isalnum() else sku_match
            else:
                sku_id = sku_match

            yield scrapy.Request(
                url=product_url,
                callback=self.parse_product,
                headers=self.headers,
                dont_filter=True,
                cb_kwargs={"sku_id": sku_id},
            )

    def parse_product(self, response, sku_id):
        if response.status in [404]:
            return
        url_parts = response.url.split(".net/")[1]
        if url_parts.startswith("products"):
            url_without_lang = url_parts.split("/", 1)[1]
        else:
            url_without_lang = "/".join(url_parts.split("/")[1:])

        brand = ""
        mpn = ""
        script_tag_content = response.css(
            'script[type="application/ld+json"]::text'
        ).getall()
        for script_content in script_tag_content:
            json_data = json.loads(script_content)
            try:
                if "mpn" in json_data:
                    mpn = json_data.get("mpn")
                    brand = json_data["brand"].get("name")
                    break
            except json.JSONDecodeError as e:
                self.log(f"Exception decoding JSON Images: {e}")

        material = ""
        collection_value = ""
        color_value = ""
        size_dimensions = []
        main_material = ""
        raw_data = response.css(
            "span.datalayer-view-event::attr(data-tealium-view)"
        ).get()
        try:
            json_data = json.loads(raw_data)
            products = json_data[0].get("products", [])
            for product in products:
                main_material = product.get("metal", "").strip()
                collection_value = product.get("collection", "").strip()
                material = product.get("material", "").strip()
                if material.lower() in [
                    "no other material",
                    "sin ningún otro material",
                ]:
                    material = ""
                break
        except Exception as e:
            pass

        color_value = response.css(
            "a.color-variant-link.selected::attr(data-product-color-group)"
        ).get()
        if not color_value:
            color_value = response.css(
                "a.metal-swatch.selected::attr(data-product-metal-group)"
            ).get()

        details = response.css(
            "div.product-attributes-text > p.product-attributes-title::text"
        ).getall()
        for detail in details:
            if detail.strip() == "Dimensions" or detail.strip() == "Dimensiones":
                mm_values = response.css(
                    "div.product-attributes-text > p.product-attributes-description::text"
                ).getall()
                size_dimensions = [val.strip() for val in mm_values if "mm" in val]
                break

        domain, domain_url = self.extract_domain_domain_url(response.url)
        time_stamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        list_img = []
        image_sources = response.css("img.js-product-image::attr(data-img)").getall()
        for image in image_sources:
            json_data = json.loads(image)
            hires_url = json_data.get("hires")
            desired_part = urljoin(response.url, hires_url)
            list_img.append(desired_part)

        is_production = get_project_settings().get("IS_PRODUCTION")
        product_images_info = []
        if is_production:
            product_images_info = upload_images_to_azure_blob_storage(
                self, list_img, use_zyte=True
            )
        else:
            product_images_info = list_img

        review_api = f"https://api.bazaarvoice.com/data/display/0.2alpha/product/summary?PassKey=ua8wlktbp7dm9rbxu245ixjlt&productid={sku_id}&contentType=reviews,questions&reviewDistribution=primaryRating,recommended&rev=0&contentlocale=en_IE,en_GB,en_US,en_CA,fr_CA,fr_FR,it_IT,de_DE,nl_NL,da_DK,sv_SE,pl_PL,en_AU,en_NZ,es_ES&incentivizedStats=true"
        yield scrapy.Request(
            review_api,
            callback=self.parse_review,
            headers=self.headers,
            dont_filter=True,
            cb_kwargs={
                "time_stamp": time_stamp,
                "domain": domain,
                "domain_url": domain_url,
                "collection_value": collection_value,
                "brand": brand,
                "mpn": mpn,
                "sku_id": sku_id,
                "main_material": main_material,
                "material": material,
                "color_value": color_value,
                "product_images_info": product_images_info,
                "size_dimensions": size_dimensions,
                "url_without_lang": url_without_lang,
            },
        )

    def parse_review(
        self,
        response,
        time_stamp,
        domain,
        domain_url,
        collection_value,
        brand,
        mpn,
        sku_id,
        main_material,
        material,
        color_value,
        product_images_info,
        size_dimensions,
        url_without_lang,
    ):
        reviews_number = ""
        reviews_rating_value = ""
        content = {}
        specification = {}
        try:
            data = response.json()
            review_summary = data.get("reviewSummary")
            if review_summary:
                reviews_number = review_summary.get("numReviews")
                reviews_rating = review_summary.get("primaryRating", {}).get("average")
                reviews_rating_value = reviews_rating
        except Exception as e:
            pass
        # get content and specification
        json_data = self.spec_mapping
        for item in json_data:
            country_code = item.get("countryCode").lower()
            language_code = item.get("codeUrl").lower()
            url = f"https://{country_code}{self.base_url}/{language_code}/{url_without_lang}"
            yield scrapy.Request(
                url,
                headers=self.headers,
                callback=self.collect_content_and_specification,
                dont_filter=True,
                cb_kwargs={
                    "time_stamp": time_stamp,
                    "domain": domain,
                    "domain_url": domain_url,
                    "collection_value": collection_value,
                    "brand": brand,
                    "mpn": mpn,
                    "sku_id": sku_id,
                    "main_material": main_material,
                    "material": material,
                    "color_value": color_value,
                    "product_images_info": product_images_info,
                    "size_dimensions": size_dimensions,
                    "language_code": language_code,
                    "country_code": country_code,
                    "reviews_rating_value": reviews_rating_value,
                    "reviews_number": reviews_number,
                    "content": content,
                    "specification": specification,
                    "total_languages": len(json_data),
                },
            )

    def collect_content_and_specification(
        self,
        response,
        time_stamp,
        domain,
        domain_url,
        collection_value,
        brand,
        mpn,
        sku_id,
        main_material,
        material,
        color_value,
        product_images_info,
        size_dimensions,
        language_code,
        country_code,
        reviews_rating_value,
        reviews_number,
        content,
        specification,
        total_languages,
    ):
        sku_title = ""
        sku_short_description = ""
        dimension_pairs_text = ""
        script_tag_content = response.css(
            'script[type="application/ld+json"]::text'
        ).getall()
        for script_content in script_tag_content:
            json_data = json.loads(script_content)
            if "description" in json_data:
                sku_title = json_data["name"]
                sku_short_description = json_data["description"]
                break
        if not sku_title:
            sku_title = "N/A"

        dimension_pairs = {}
        dimension_items = response.css(
            ".product-attributes .attribute-value .attribute-dimension"
        )
        for item in dimension_items:
            key = item.css(".attribute-dimension::text").get(default="").strip()
            value = item.xpath("normalize-space(following-sibling::text()[1])").get()
            dimension_pairs[key] = value
        if dimension_pairs:
            dimension_pairs_text = " ".join(
                [f"{key}: {value}" for key, value in dimension_pairs.items()]
            )

        description_text = [
            text.strip()
            for text in response.css(
                "span.attribute-label::text, span.attribute-value-item::text"
            ).getall()
        ]
        descriptions_text = " ".join(text.strip() for text in description_text).strip()
        sku_long_description = (
            sku_short_description + descriptions_text + dimension_pairs_text
            if sku_short_description
            else descriptions_text
        )
        content[language_code] = {
            "sku_link": response.url,
            "sku_title": sku_title,
            "sku_short_description": sku_short_description,
            "sku_long_description": sku_long_description,
        }

        script_tag_contents = response.css(
            'script[type="application/ld+json"]::text'
        ).getall()
        sales_price = ""
        availability = ""
        currency_code = ""
        for script_content in script_tag_contents:
            try:
                json_data = json.loads(script_content)
                if "offers" in json_data:
                    currency_code = json_data["offers"].get("priceCurrency")
                    availability = json_data["offers"].get("availability")
                    break
            except Exception as e:
                continue  # Ignore bad JSON

        # Get base price
        sales_price = response.css(
            "div.price-attribute span.sales.sales-origin > span.value::attr(content)"
        ).get()
        base_price = (
            response.css(
                "div.price-attribute span.strike-through.list > span.value::attr(content)"
            ).get()
            or sales_price
        )

        # Get shipping expenses and lead time
        shipping_info = response.css(
            "#shipping-returns > div > div:nth-of-type(1) > div:nth-of-type(2) > p:nth-of-type(1)"
        )
        text_nodes = shipping_info.css("::text").getall()
        shipping_expenses = text_nodes[0].strip() if len(text_nodes) > 0 else ""
        shipping_lead_time = text_nodes[1].strip() if len(text_nodes) > 1 else ""

        # Availability
        product_availability = self.check_product_availability(availability)
        availability_status = product_availability[0]
        out_of_stock_text = product_availability[1]

        # Sizes
        sizes = response.css(
            ".col-12 > .size-container > .size-attributes.selectable > button::attr(data-sizeattr)"
        ).getall()
        unique_sizes_list = list(set([s.strip() for s in sizes]))

        specification[country_code] = {
            "lang": language_code,
            "domain_country_code": country_code,
            "currency": currency_code if currency_code else "default_currency_code",
            "base_price": base_price if base_price else 0.0,
            "sales_price": sales_price if sales_price else base_price,
            "active_price": sales_price if sales_price else base_price,
            "availability": availability_status if availability_status else "NA",
            "availability_message": out_of_stock_text if out_of_stock_text else "NA",
            "shipping_lead_time": shipping_lead_time if shipping_lead_time else "NA",
            "shipping_expenses": shipping_expenses if shipping_expenses else 0.0,
            "marketplace_retailer_name": "pandora",
            "condition": "NEW",
            "reviews_rating_value": reviews_rating_value,
            "reviews_number": reviews_number,
            "size_available": unique_sizes_list if unique_sizes_list else [],
            "sku_link": response.url,
        }
        if len(content) >= total_languages:
            item = ProductItem()
            item["date"] = time_stamp
            item["domain"] = domain
            item["domain_url"] = domain_url
            item["collection_name"] = collection_value
            item["brand"] = brand
            item["mpn"] = mpn
            item["manufacturer"] = self.name
            item["sku"] = sku_id
            item["main_material"] = main_material
            item["secondary_material"] = material
            item["sku_color"] = color_value
            item["image_url"] = product_images_info
            item["size_dimensions"] = size_dimensions
            item["specification"] = specification
            item["content"] = content

            yield item

    def check_product_availability(self, availability):
        try:
            availability_value = availability.lower()
            if "instock" in availability_value:
                out_of_stock_text = "AVAILABLE"
                return "Yes", out_of_stock_text
            else:
                out_of_stock_text = "Temporarily out of stock"
                return "No", out_of_stock_text
        except Exception as e:
            return "No"
