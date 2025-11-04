import datetime
import json
import logging
import re
import scrapy
from bclowd_spider.items import ProductItem


class KendraSpider(scrapy.Spider):
    name = "Kendrascott"

    handle_httpstatus_list = [430, 500, 403, 404, 410, 400]

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    }

    def start_requests(self):
        # Use sitemap to get all products
        sitemap_urls = [
            "https://www.kendrascott.com/sitemap_0-product.xml",
            "https://www.kendrascott.com/sitemap_1-product.xml",
        ]
        for sitemap_url in sitemap_urls:
            yield scrapy.Request(
                sitemap_url, callback=self.parse_sitemap, headers=self.headers
            )

    def parse_sitemap(self, response):
        """Parse sitemap XML and extract product URLs"""
        # Extract all product URLs from sitemap
        product_urls = response.xpath('//*[local-name()="loc"]/text()').getall()
        self.logger.info(
            f"Found {len(product_urls)} products in sitemap: {response.url}"
        )

        for product_url in product_urls:
            # Skip Yellow Rose products that cause 500 errors
            if "yellow-rose" in product_url.lower():
                continue

            # Extract SKU from URL
            sku_match = re.search(r"/([0-9]+)\.html$", product_url) or re.search(
                r"/([^/]+)\.html$", product_url
            )
            if sku_match:
                sku_id = sku_match.group(1)
                yield scrapy.Request(
                    url=product_url,
                    callback=self.parse_product,
                    headers=self.headers,
                    cb_kwargs={"product_url": product_url, "sku_id": sku_id},
                    dont_filter=True,
                )

    def parse_product(self, response, product_url, sku_id):
        # Skip products that show error page
        if response.css(".container.oops-page-found"):
            self.logger.info(f"Skipping error page: {product_url}")
            return

        brand = ""
        list_img = []
        mpn = ""

        # Extract comprehensive product data from JSON-LD structured data
        script_tag_content = response.css(
            'script[type="application/ld+json"]::text'
        ).getall()

        json_ld_data = {}
        for script_tag in script_tag_content:
            try:
                json_data = json.loads(script_tag)
                if json_data.get("@type") == "Product":
                    json_ld_data = json_data
                    mpn = json_data.get("mpn") or json_data.get("sku")

                    # Extract images
                    images = json_data.get("image", [])
                    list_img.extend([img for img in images if img and img.strip()])

                    # Extract brand
                    brand_data = json_data.get("brand")
                    if brand_data:
                        brand = brand_data.get("name")
                    break
            except Exception:
                continue

        # For discontinued products (410 status), we won't have JSON-LD images
        # In those cases, just leave list_img empty rather than getting UI images

        content_info = self.extract_content(response)
        content = {
            "en": {
                "sku_link": response.url,
                "sku_title": content_info["sku_title"],
                "sku_short_description": content_info["sku_short_description"],
                "sku_long_description": content_info["sku_long_description"],
            }
        }

        sizes = [
            size.strip()
            for size in response.css("button.size-options.selectable::text").getall()
        ]
        base_price = self.extract_price(response)

        # Extract availability from JSON-LD or page elements
        availability = "Yes"
        availability_message = "AVAILABLE"

        # Check JSON-LD for availability
        for script_content in script_tag_content:
            try:
                json_data = json.loads(script_content)
                if json_data.get("@type") == "Product":
                    offers = json_data.get("offers", {})
                    if offers:
                        availability_status = offers.get("availability", "")
                        if "instock" in availability_status.lower():
                            availability = "Yes"
                            availability_message = "AVAILABLE"
                        elif "outofstock" in availability_status.lower():
                            availability = "No"
                            availability_message = "OUT OF STOCK"
                        elif "limitedavailability" in availability_status.lower():
                            availability = "Limited"
                            availability_message = "LIMITED AVAILABILITY"
                    break
            except:
                continue

        # Fallback: Check for out of stock indicators in HTML
        if availability == "Yes":
            out_of_stock_indicators = response.css(
                '.out-of-stock, .unavailable, .sold-out, [data-availability="false"]'
            ).get()
            if out_of_stock_indicators:
                availability = "No"
                availability_message = "OUT OF STOCK"

        # Extract size dimensions
        size_dimensions = []
        size_dimension = response.css(
            '#collapsible-details-1 h3:contains("Size") + p::text'
        ).get()
        strap_width = response.css(
            '#collapsible-details-1 h3:contains("Strap Width") + p::text'
        ).get()
        strap_length = response.css(
            '#collapsible-details-1 h3:contains("Strap Length") + p::text'
        ).get()
        carat_weight = response.css(
            '#collapsible-details-1 h3:contains("Carat Weight") + p::text'
        ).get()

        if size_dimension:
            size_dimensions.append("Size: " + size_dimension.strip())
        elif strap_width and strap_length:
            size_dimensions.append("Size Width: " + strap_width.strip())
            size_dimensions.append("Size Length: " + strap_length.strip())
        if carat_weight:
            size_dimensions.append("Carat Weight: " + carat_weight.strip())

        specification = {
            "us": {
                "active_price": base_price,
                "availability": availability,
                "AvailabilityMessage": availability_message,
                "base_price": base_price,
                "condition": "NEW",
                "currency": "USD",
                "domain_country_code": "us",
                "lang": "en",
                "marketplace_retailer_name": "kendrascott",
                "reviews_number": None,
                "reviews_rating_value": "0.0",
                "sales_price": base_price,
                "shipping_expenses": "Free on eligible orders",
                "shipping_lead_time": "Will arrive in 3-5 business days after processing and shipping",
                "size_availability": sizes + size_dimensions,
                "sku_link": product_url,
                "stock_quantity": "",
            }
        }

        domain = "kendrascott"
        domain_url = "kendrascott.com"
        time_stamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Extract color and materials
        product_color = ""
        main_material = ""
        secondary_material = ""

        # First try JSON-LD data
        if json_ld_data:
            json_material = json_ld_data.get("material", "")
            json_color = json_ld_data.get("color", "")

            if json_material:
                main_material = json_material
            if json_color:
                product_color = json_color

        # Fallback to HTML extraction
        if not product_color:
            product_color = response.css(".color-name span::text").get() or ""
            if not product_color:
                product_color = (
                    response.css(
                        "[data-gtm-color-general]::attr(data-gtm-color-general)"
                    ).get()
                    or ""
                )

        # Extract main material from various sources
        if not main_material:
            main_material = response.css(
                '#collapsible-details-1 h3:contains("Metal") + p::text'
            ).get()

        if not main_material:
            main_material = response.css(
                '#collapsible-details-1 h3:contains("Material") + p::text'
            ).get()

        # Secondary material is less common
        if not secondary_material:
            secondary_material = response.css(
                '#collapsible-details-1 h3:contains("Secondary Material") + p::text'
            ).get()

        # Clean up the materials
        if main_material:
            main_material = main_material.strip()
        if secondary_material:
            secondary_material = secondary_material.strip()
        if product_color:
            product_color = product_color.strip()

        # Extract product badge
        product_badge = ""
        badge = (
            response.css(".js-gtm-badge .title-s::text").get()
            or response.css(".badge-best-seller .title-s::text").get()
            or ""
        )
        if badge:
            product_badge = badge.strip()
        else:
            # Fallback to other badge patterns
            badge = response.css("[data-badge-name]::text").get() or ""
            if badge:
                product_badge = badge.strip()

        item = ProductItem()
        item["date"] = time_stamp
        item["domain"] = domain
        item["domain_url"] = domain_url
        item["brand"] = brand if brand else "Kendra Scott"
        item["collection_name"] = None
        item["product_badge"] = product_badge
        item["manufacturer"] = self.name.replace("Old", "")
        item["sku"] = mpn if mpn else sku_id
        item["sku_color"] = product_color if product_color else None
        item["main_material"] = main_material if main_material else None
        item["secondary_material"] = secondary_material if secondary_material else None
        # size_dimensions moved to specification.size_availability
        item["image_url"] = list_img
        item["content"] = content
        item["specification"] = specification

        yield item

    def extract_content(self, response):
        # Extract sku_title from JSON-LD first (most reliable), then fall back to HTML
        sku_title = ""
        try:
            script_tag = response.css('script[type="application/ld+json"]::text').get()
            if script_tag:
                json_data = json.loads(script_tag)
                if json_data.get("@type") == "Product":
                    sku_title = json_data.get("name", "")
        except:
            pass

        # Fallback to HTML if JSON-LD doesn't work
        if not sku_title:
            sku_title_element = response.css("h1.product-name::text").get()
            sku_title = sku_title_element.strip() if sku_title_element else ""

        # Extract short description from the specific div you mentioned
        short_description = ""
        short_desc_element = response.css(
            'div.value.content[id="collapsible-description-1"]::text'
        ).get()
        if short_desc_element:
            short_description = short_desc_element.strip()
        else:
            # Try alternative selectors
            short_desc_element = response.css("#collapsible-description-1::text").get()
            if short_desc_element:
                short_description = short_desc_element.strip()

        # Extract long description from the tab content area you specified
        long_description = short_description  # Start with short description

        # Get detailed product information from the details tab
        detail_elements = response.css("#pro-details .value.content").getall()
        for element in detail_elements:
            from scrapy import Selector

            sel = Selector(text=element)
            text_content = sel.css("::text").getall()
            detail_text = " ".join([t.strip() for t in text_content if t.strip()])
            if detail_text and detail_text not in long_description:
                long_description += " " + detail_text

        # Also try to get details from the structured format you showed
        detail_sections = response.css(
            "#pro-details h3::text, #pro-details p::text"
        ).getall()
        if detail_sections:
            detail_text = " ".join([t.strip() for t in detail_sections if t.strip()])
            if detail_text and detail_text not in long_description:
                long_description += " " + detail_text

        self.logger.info(
            f"Extracted content - Title: '{sku_title[:50]}...', Short: '{short_description[:50]}...', Long: '{long_description[:100]}...'"
        )

        return {
            "sku_title": sku_title,
            "sku_short_description": short_description,
            "sku_long_description": long_description,
        }

    def extract_price(self, response):
        """Extract price from product page"""
        price_selectors = [
            "span.sales::attr(data-formatted-price)",
            "span#product-list-price::attr(content)",
            ".price-sales .value::text",
            ".price .value::text",
        ]

        for selector in price_selectors:
            price = response.css(selector).get()
            if price:
                price_value = self.extract_price_info(price.strip())
                if price_value and price_value != (None,):
                    return price_value

        return "0.00"

    def extract_price_info(self, price_string):
        match = re.search(r"([^\d]*)([\d.,]+)", price_string)
        if match:
            currency_symbol, numerical_value = match.groups()
            pattern = r"\,\d{3}(?![\d,])"
            match = re.search(pattern, numerical_value)
            if match:
                numerical_value = numerical_value.replace(",", "")
            pattern = r"\.\d{3}(?![\d.])"
            match = re.search(pattern, numerical_value)
            if match:
                numerical_value = numerical_value.replace(".", "")
            numerical_value = numerical_value.replace(",", ".")
            if "." not in numerical_value:
                numerical_value = numerical_value + ".00"
            return numerical_value
        else:
            return (None,)
