# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# from scrapy.downloadermiddlewares.retry import RetryMiddleware
# from scrapy.utils.response import response_status_message
# import time

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from random import randint
from .settings import rotate_headers


class RotateUserAgentMiddleware:
    def process_request(self, request, spider):
        request.headers.update(rotate_headers())


class BclowdSpiderSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ZenRowsDownloaderMiddleware:
    """Custom middleware to handle ZenRows requests"""

    def process_request(self, request, spider):
        if request.meta.get("skip_scrapy_download"):
            # Return a fake response to trigger the callback
            from scrapy.http.response.html import HtmlResponse

            fake_response = HtmlResponse(
                url=request.url,
                status=200,
                body=b"{}",  # Empty JSON body
                encoding="utf-8",
            )
            return fake_response
        return None


class BclowdSpiderDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    # def process_response(self, request, response, spider):
    #         if request.meta.get('dont_retry', False):
    #             return response
    #         elif response.status == 429:
    #             self.crawler.engine.pause()
    #             time.sleep(60) # If the rate limit is renewed in a minute, put 60 seconds, and so on.
    #             self.crawler.engine.unpause()
    #             reason = response_status_message(response.status)
    #             return self._retry(request, reason, spider) or response
    #         elif response.status in self.retry_http_codes:
    #             reason = response_status_message(response.status)
    #             return self._retry(request, reason, spider) or response
    #         return response

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
