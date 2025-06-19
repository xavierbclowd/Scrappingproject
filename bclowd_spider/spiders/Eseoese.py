import os
import uuid
import json
import re
import logging
import datetime
from urllib.parse import urljoin, urlparse, parse_qs

import scrapy
from scrapy import Request
from scrapy.exceptions import DropItem
from inline_requests import inline_requests
from scrapy.utils.project import get_project_settings

from bclowd_spider.items import ProductItem
from bclowd_spider.settings import upload_images_to_azure_blob_storage

class EseoeseSpider(scrapy.Spider):
    name = "Eseoese"
    custom_settings = {
        "HANDLE_HTTPSTATUS_LIST": [404, 400, 429, 403, 500, 430],
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "CONCURRENT_REQUESTS_PER_IP": 4,
        "DOWNLOAD_DELAY": 1,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
        "ROTATING_PROXY_BAN_STATUS_CODES": [403, 500, 502, 503, 504, 429],
        "RETRY_TIMES": 5,
        "HTTPERROR_ALLOWED_CODES": [429],
        "DOWNLOADER_MIDDLEWARES": {
            "bclowd_spider.middlewares.RotateUserAgentMiddleware": 400,
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
            "rotating_proxies.middlewares.RotatingProxyMiddleware": 610,
            "rotating_proxies.middlewares.BanDetectionMiddleware": 620,
            "bclowd_spider.middlewares.BclowdSpiderDownloaderMiddleware": 543,
        },
        # Optional: cache responses locally to avoid re-downloading on retries
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_EXPIRATION_SECS": 86400,  # 1 day
        "HTTPCACHE_DIR": "httpcache",
    }

    spec_mapping = json.dumps([
        {"countryName": "es", "lang": "es", "codeUrl": "es"},
        # {"countryName": "fr", "lang": "en", "codeUrl": "fr"},
    ])
    language_mapping = json.dumps([
        {"language_countryCode": "es", "language": "es"},
        {"language_countryCode": "en", "language": "en"},
    ])

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) "
                      "Gecko/20100101 Firefox/98.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;"
                  "q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_page_skus = []
        self.directory = get_project_settings().get("FILE_PATH") or "."
        os.makedirs(self.directory, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
        logging.basicConfig(
            filename=os.path.join(self.directory, f"{ts}_{self.name}.log"),
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )

    def start_requests(self):
        yield Request(
            "https://www.eseoese.com/en",
            headers=self.headers,
            callback=self.parse_categories,
            dont_filter=True
        )

    def parse_categories(self, response):
        rels = set(response.css('.desktop-menu-list-lvl-3 > a::attr(href)').getall())
        for rel in rels:
            first = urljoin(response.url, rel) + "?page=1"
            self.logger.info(f"Scheduling first page: {first}")
            yield Request(first, headers=self.headers, callback=self.parse, dont_filter=True)

    def parse(self, response):
        prods = response.css('.container-fluid>.grid-items>div.grid-item')
        if not prods:
            self.logger.info("No products on this page → stop pagination.")
            return

        skus = []
        for p in prods:
            sku = p.css('form.inset::attr(data-duplicate-product-sku)').get()
            href = response.urljoin(p.css('a.product-list-img-link::attr(href)').get())
            skus.append(sku)
            yield Request(
                href,
                headers=self.headers,
                callback=self.parse_product,
                cb_kwargs={'product_url': href, 'sku': sku},
                dont_filter=True
            )

        page = int(parse_qs(urlparse(response.url).query).get('page', ['1'])[0])
        if page == 1:
            self.first_page_skus = skus
        elif skus == self.first_page_skus:
            self.logger.info(f"Detected repeat of page1 SKUs on page {page} → stopping.")
            return

        base = f"{urlparse(response.url).scheme}://{urlparse(response.url).netloc}{urlparse(response.url).path}"
        next_page = f"{base}?page={page+1}"
        self.logger.info(f"Following next page: {next_page}")
        yield Request(next_page, headers=self.headers, callback=self.parse, dont_filter=True)

    @inline_requests
    def parse_product(self, response, product_url, sku):
        main_material = ''
        total_stock = ''
    
        # 1) Safely load the first JSON-LD block
        data_product = response.css('script[type="application/ld+json"]::text').get()
        if not data_product:
            self.logger.warning(f"No JSON-LD found for {response.url}, skipping JSON parsing")
            script_json_data = {}
        else:
            try:
                script_json_data = json.loads(data_product)
            except ValueError:
                self.logger.warning(f"Malformed JSON-LD on {response.url}, skipping JSON parsing")
                script_json_data = {}
    
        # 2) Extract totalStock if available
        total_stock = script_json_data.get("totalStock", '')
    
        # 3) Extract main_material from customTagValues (if present)
        for tag in script_json_data.get("customTagValues") or []:
            name = tag.get("name", "")
            if "Composition and care" in name:
                value = tag.get("value", "")
                parts = value.split("<br><br>")
                if len(parts) > 1:
                    main_material = parts[1].split("<br>")[0]
                break
    
        # badge & color
        badge = (response.css('div.product-status-message.featured::text').get() or "").strip()
        color = (response.css('div.product-list-ct-name::text').get() or "").strip()
    
        # JSON-LD images
        list_img = []
        for sc in response.css('script[type="application/ld+json"]::text').getall():
            try:
                jd = json.loads(sc)
                if 'image' in jd:
                    list_img.extend(jd['image'])
                    break
            except ValueError:
                continue
    
        # build URL without language
        parts = product_url.split("/")
        url_without_language = "/".join(parts[4:]) if len(parts) > 4 else ""
    
        # SKU fallback from JSON-LD
        if not sku:
            text = "".join(response.css('script[type="application/ld+json"]::text').getall())
            m = re.search(r'(\d+)-\d+', text)
            if m:
                sku = m.group(1)
                self.logger.info(f"Re-parsed SKU {sku} from JSON-LD for {product_url}")
            else:
                self.logger.warning(f"No SKU for {product_url}, dropping item")
                raise DropItem("missing SKU")
    
        # 1) content per language
        content = {}
        for m in json.loads(self.language_mapping):
            code, lang = m['language_countryCode'], m['language']
            url = f"https://www.eseoese.com/{code}/{url_without_language}"
            resp = yield Request(url, headers=self.headers, dont_filter=True)
            if resp.status == 200:
                info = self.collect_content_information(resp)
                content[lang] = {"sku_link": url, **info}
            elif resp.status in (301, 302):
                loc = resp.headers.get(b"Location").decode()
                resp2 = yield Request(resp.urljoin(loc), headers=self.headers, dont_filter=True)
                if resp2.status == 200:
                    info = self.collect_content_information(resp2)
                    content[lang] = {"sku_link": resp2.url, **info}
    
        # 2) specs per country (pass total_stock if desired)
        specification = {}
        for s in json.loads(self.spec_mapping):
            codeUrl, lang, countryName = s['codeUrl'], s['lang'], s['countryName']
            url_c = f"https://www.eseoese.com/{codeUrl}/{url_without_language}"
            resp_c = yield Request(url_c, headers=self.headers, dont_filter=True)
            if resp_c.status == 200:
                info = self.collect_specification_info(resp_c, lang, url_c, countryName, total_stock)
                specification[countryName.lower()] = info
            elif resp_c.status in (301, 302):
                loc = resp_c.headers.get(b"Location").decode()
                resp2 = yield Request(resp_c.urljoin(loc), headers=self.headers, dont_filter=True)
                if resp2.status == 200:
                    info = self.collect_specification_info(resp2, lang, url_c, countryName, total_stock)
                    specification[countryName.lower()] = info
    
        # 3) image download/storage
        is_prod = get_project_settings().get("IS_PRODUCTION", False)
        images_info = []
        if is_prod:
            images_info = upload_images_to_azure_blob_storage(self, list_img)
        else:
            dir_sku = os.path.join(self.directory, sku)
            os.makedirs(dir_sku, exist_ok=True)
            for img_url in list_img:
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                res_img = yield Request(img_url, headers=self.headers, dont_filter=True)
                fn = os.path.join(dir_sku, f"{uuid.uuid4()}.jpg")
                with open(fn, "wb") as f:
                    f.write(res_img.body)
                images_info.append(fn)
    
        # yield the item
        domain, domain_url = self.extract_domain_domain_url(response.url)
        ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
        item = ProductItem()
        item['date'] = ts
        item['domain'] = domain
        item['domain_url'] = domain_url
        item['collection_name'] = ''
        item['brand'] = "Eseoese"
        item['manufacturer'] = self.name
        item['product_badge'] = badge
        item['sku'] = sku
        item['sku_color'] = color
        item['main_material'] = main_material
        item['secondary_material'] = ''
        item['image_url'] = images_info
        item['size_dimensions'] = []
        item['content'] = content
        item['specification'] = specification
        yield item

    def collect_content_information(self, response):
        title = short = ""
        for sc in response.css('script[type="application/ld+json"]::text').getall():
            try:
                jd = json.loads(sc)
                if 'description' in jd:
                    title = jd.get("name", "")
                    short = jd.get("description", "")
                    break
            except:
                pass
        full_description = response.css('div#collapse-long-desc div.accordion-body *::text').getall()
        sku_long_description_text = ' '.join([t.strip() for t in full_description if t.strip()])
        return {
            "sku_title": title,
            "sku_short_description": short,
            "sku_long_description": sku_long_description_text
        }

    def collect_specification_info(self, resp, language, country_url, countryName, total_stock):
        sizes = []
        for el in resp.css('div.productOptionValue[data-lc-product-option-value]'):
            raw = el.attrib.get('data-lc-product-option-value')
            if raw:
                data = json.loads(raw)
                sizes.append(data.get('value'))
        currency = price = availability = ""
        for sc in resp.css('script[type="application/ld+json"]::text').getall():
            try:
                jd = json.loads(sc)
                if 'offers' in jd:
                    offers = jd['offers']
                    currency = offers.get("priceCurrency", "")
                    price = offers.get("price", "")
                    availability = offers.get("availability", "")
                    break
            except:
                pass
        shipping = resp.css('.html-output>font::text').get() or ""
        avail_flag, avail_msg = self.check_product_availability(availability)
        return {
            "lang": language.lower(),
            "domain_country_code": countryName.lower(),
            "currency": currency,
            "base_price": price if price else 0.0,
            "sales_price": price if price else 0.0 ,
            "active_price": price if price else 0.0 ,
            "stock_quantity": total_stock,
            "availability": avail_flag,
            "availability_message": avail_msg,
            "shipping_lead_time": '',
            "shipping_expenses": shipping,
            "marketplace_retailer_name": "",
            "condition": "NEW",
            "reviews_rating_value": '',
            "reviews_number": '',
            "size_available": sizes,
            "sku_link": country_url
        }

    def extract_domain_domain_url(self, real_url):
        ex = __import__('tldextract').extract(real_url)
        return ex.domain, ex.registered_domain

    def check_product_availability(self, availability):
        low = (availability or "").lower()
        if "instock" in low:
            return "Yes", "AVAILABLE"
        return "No", "Temporarily out of stock"
