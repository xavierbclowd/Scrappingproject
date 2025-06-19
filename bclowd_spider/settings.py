# Scrapy settings for bclowd_spider project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from random import randint
import requests, os, cloudscraper
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContentSettings,
    generate_blob_sas,
    BlobSasPermissions,
)
from io import BytesIO
from datetime import datetime, timedelta
from itertools import cycle


BOT_NAME = 'bclowd_spider'

SPIDER_MODULES = ['bclowd_spider.spiders']
NEWSPIDER_MODULE = 'bclowd_spider.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'bclowd_spider (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 10

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 2  # A slight delay to prevent hitting server limits
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 10
CONCURRENT_REQUESTS_PER_IP = 10
# RETRY_ENABLED = True
# RETRY_PRIORITY_ADJUST = -1

# Disable cookies (enabled by default)
COOKIES_ENABLED = True

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False
# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'bclowd_spider.middlewares.BclowdSpiderSpiderMiddleware': 543,
# }
DNS_RESOLVER = "scrapy.resolver.CachingHostnameResolver"
DOWNLOAD_HANDLERS = {
    "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler"
}
# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    
    # 'scrapoxy.ProxyDownloaderMiddleware': 100,
    # 'scrapoxy.BlacklistDownloaderMiddleware': 101,
    'bclowd_spider.middlewares.RotateUserAgentMiddleware': 400,
    'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
    # 'scrapeops_scrapy.middleware.retry.RetryMiddleware': 550,      
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
    'bclowd_spider.middlewares.BclowdSpiderDownloaderMiddleware': 543,   
}

## Insert Your List of Proxies Here
ROTATING_PROXY_LIST = [
    # 'private.residential.proxyrack.net:10000',
    # 'private.residential.proxyrack.net:10001',
    'p.webshare.io:10000',
    'p.webshare.io:10001',
    'p.webshare.io:10002',
    'p.webshare.io:10003',
    'p.webshare.io:10004',
    'p.webshare.io:10005',
    'p.webshare.io:10006',
    'p.webshare.io:10007',
    'p.webshare.io:10008',
    'p.webshare.io:10009',
    'p.webshare.io:10010',
    'p.webshare.io:10011',
    'p.webshare.io:10012',
    'p.webshare.io:10013',
    'p.webshare.io:10014',
    'p.webshare.io:10015',
    'p.webshare.io:10016',
    'p.webshare.io:10017',
    'p.webshare.io:10018'
    # 'p.webshare.io:10019'
    # 'p.webshare.io:10020',
    # 'p.webshare.io:10021',
    # 'p.webshare.io:10022',
    # 'p.webshare.io:10023',
    # 'p.webshare.io:10024',
    # 'p.webshare.io:10025',
    # 'p.webshare.io:10026',
    # 'p.webshare.io:10027',
    # 'p.webshare.io:10028',
    # 'p.webshare.io:10029',
    # 'p.webshare.io:10030',
    # 'p.webshare.io:10031',
    # 'p.webshare.io:10032',
    # 'p.webshare.io:10033',
    # 'p.webshare.io:10034',
    # 'p.webshare.io:10035',
    # 'p.webshare.io:10036',
    # 'p.webshare.io:10037',
    # 'p.webshare.io:10038',
    # 'p.webshare.io:10039',
    # 'p.webshare.io:10040',
    # 'p.webshare.io:10041',
    # 'p.webshare.io:10042',
    # 'p.webshare.io:10043',
    # 'p.webshare.io:10044',
    # 'p.webshare.io:10045',
    # 'p.webshare.io:10046',
    # 'p.webshare.io:10047',
    # 'p.webshare.io:10048',
    # 'p.webshare.io:10049',
    # 'p.webshare.io:10050',
    # 'p.webshare.io:10051',
    # 'p.webshare.io:10052',
    # 'p.webshare.io:10053',
    # 'p.webshare.io:10054',
    # 'p.webshare.io:10055',
    # 'p.webshare.io:10056',
    # 'p.webshare.io:10057',
    # 'p.webshare.io:10058',
    # 'p.webshare.io:10059',
    # 'p.webshare.io:10060',
    # 'p.webshare.io:10061',
    # 'p.webshare.io:10062',
    # 'p.webshare.io:10063',
    # 'p.webshare.io:10064',
    # 'p.webshare.io:10065',
    # 'p.webshare.io:10066',
    # 'p.webshare.io:10067',
    # 'p.webshare.io:10068',
    # 'p.webshare.io:10069',
    # 'p.webshare.io:10070',
    # 'p.webshare.io:10071',
    # 'p.webshare.io:10072',
    # 'p.webshare.io:10073',
    # 'p.webshare.io:10074'
]

# RESIDENTIAL_PROXIES = [
#     "http://p.webshare.io:9999"
# ]

proxy_cycle = cycle(ROTATING_PROXY_LIST)
# Configure which HTTP response codes should trigger a retry
# RETRY_HTTP_CODES = [403, 500, 443, 502, 503, 504, 522, 524, 429, 408] #400 404
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 429, 408]

# Configure the maximum number of times Scrapy will retry a request
RETRY_TIMES = 10  # Adjust this value based on your needs
DOWNLOAD_TIMEOUT = 30  # Lower the timeout to quickly drop slow responses.
ROTATING_PROXY_PAGE_RETRY_TIMES = 10  # number of times to retry downloading a page using a different proxy. After this amount of retries failure is considered a page failure, not a proxy failure.
ROTATING_PROXY_CLOSE_SPIDER = False   # When True, spider is stopped if there are no alive proxies. If False (default), then when there is no alive proxies all dead proxies are re-checked.

# #SCRAPOXY PROXIES MONITORING
# SCRAPOXY_MASTER = "http://localhost:8888"
# SCRAPOXY_API = "http://localhost:8890/api"
# SCRAPOXY_USERNAME = "vpk2vtc1f8ryswv1042s"
# SCRAPOXY_PASSWORD = "8lvtl4nv8z5t7n927qci9"
# SCRAPOXY_BLACKLIST_HTTP_STATUS_CODES = [400, 429, 500, 503]
# SCRAPOXY_SLEEP_MIN = 60
# SCRAPOXY_SLEEP_MAX = 180

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
        'scrapeops_scrapy.extension.ScrapeOpsMonitor': 500, 
        }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'bclowd_spider.pipelines.AzureImageUploadPipeline': 300,
    'bclowd_spider.pipelines.BclowdSpiderPipeline': 400,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 1
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 4
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = True

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 43200  # 0 means ‘never expire’
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = [500, 302]
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'


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
HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/100.0.100.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 OPR/78.0.4093.147",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"
]

### local logging
# current_path = os.path.dirname(os.path.realpath(__file__))
# FILE_PATH = os.path.dirname(os.path.dirname(current_path)) + "/logs"
FILE_PATH = '/tmporal/scrapy-bclowd_new/'

# IS_PRODUCTION = False
IS_PRODUCTION = False
AZURE_BLOB_ACCOUNT_NAME = 'datastorage'
AZURE_BLOB_ACCOUNT_KEY = 'AAk1I/iqSozCHHx""fBeN5zWukxjQC+ssKwUaougFTm0/NkeWzRjWYqbdqngfzjrse/DdHiXKGmCKYRRzNk+ASt3LpANg=='
AZURE_BLOB_CONTAINER_NAME = 'crawlingimages'
AZURE_BLOB_MAX_CHUNK_SIZE = 4 * 1024 * 1024

# CosmosDB (DocumentDB) Configuration
# DOCDB_HOST = 'https://127.0.0.1:8081'
DOCDB_HOST = 'https://localhost:8081'
DOCDB_DB = 'bclowddb'
DOCDB_COLLECTION = 'ROCK03'
DOCDB_MASTER_KEY = 'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='


# def upload_images_to_azure_blob_storage(self, imag_urls):
#     blob_service_client = BlobServiceClient(
#         account_url=f"https://{AZURE_BLOB_ACCOUNT_NAME}.blob.core.windows.net/", credential=AZURE_BLOB_ACCOUNT_KEY
#     )
#     container_client = blob_service_client.get_container_client(AZURE_BLOB_CONTAINER_NAME)
#     image_paths = []
#     image_data = ''
#     for image_url in imag_urls:
#         if image_url:
#             try:
#                 if not image_url or image_url == "N/A" or image_url == "":
#                     continue
#                 proxy = next(proxy_cycle)
#                 scraper = cloudscraper.create_scraper()
#                 response = scraper.get(image_url, headers=headers, proxies={'http': proxy, 'https': proxy})
#                 if response.status_code != 200:
#                     print(
#                         f"Failed to get Image: {image_url} - Status code: {response.status_code}"
#                     )
#                     continue
#                 image_data = BytesIO(response.content)

#             except Exception as e:
#                 print(e)

#             image_name = os.path.basename(image_url).split("?")[0]
#             # Create a blob client
#             blob_client = container_client.get_blob_client(blob=image_name)

#             # Upload the file in chunks
#             if not blob_client.exists():
#                 file_size = len(image_data.getbuffer())
#                 upload_offset = 0
#                 while upload_offset < file_size:
#                     chunk_data = image_data.read(AZURE_BLOB_MAX_CHUNK_SIZE)
#                     chunk_size = len(chunk_data)
#                     blob_client.upload_blob(
#                         chunk_data, length=chunk_size, max_concurrency=4
#                     )
#                     upload_offset += chunk_size
#             else:
#                 print(
#                     f"Resource by blob {image_name} Already exists. Moving to next..."
#                 )

#             content_settings = ContentSettings(
#                 content_type=response.headers.get("Content-Type")
#             )
#             blob_client.set_http_headers(content_settings=content_settings)
#             if blob_client.exists():
#                 print(
#                     f"The blob {image_name} was uploaded successfully to Azure Blob Storage!"
#                 )

#                 sas_expiry = datetime.utcnow() + timedelta(
#                     days=365
#                 )  # Expiry time for the SAS URL
#                 sas_permissions = BlobSasPermissions(read=True)
#                 sas_token = generate_blob_sas(
#                     account_name=AZURE_BLOB_ACCOUNT_NAME,
#                     account_key=AZURE_BLOB_ACCOUNT_KEY,
#                     container_name=AZURE_BLOB_CONTAINER_NAME,
#                     blob_name=image_name,
#                     permission=sas_permissions,
#                     expiry=sas_expiry,
#                 )
#                 sas_url = blob_client.url  # + '?' + sas_token # SAS url
#                 image_url = sas_url
#                 image_paths.append(image_url)
#             else:
#                 print(
#                     f"There was an error uploading the blob {image_name} to Azure Blob Storage."
#                 )
#         else:
#             break
#     return image_paths

def upload_images_to_azure_blob_storage(self, imag_urls):
   blob_service_client = BlobServiceClient(
       account_url=f"https://{AZURE_BLOB_ACCOUNT_NAME}.blob.core.windows.net/", 
       credential=AZURE_BLOB_ACCOUNT_KEY
   )
   container_client = blob_service_client.get_container_client(AZURE_BLOB_CONTAINER_NAME)
   image_paths = []
   
   for image_url in imag_urls:
       if not image_url or image_url == "N/A" or image_url == "":
           continue

       # Add https: if missing from URL
       if image_url.startswith('//'):
           image_url = 'https:' + image_url
           
       try:
           proxy = next(proxy_cycle)
           scraper = cloudscraper.create_scraper()
           response = scraper.get(image_url, headers=headers, proxies={'http': proxy, 'https': proxy})
           
           if response.status_code != 200:
               print(f"Failed to get Image: {image_url} - Status code: {response.status_code}")
               continue
               
           image_data = BytesIO(response.content)
           image_name = os.path.basename(image_url).split("?")[0]
           blob_client = container_client.get_blob_client(blob=image_name)

           if not blob_client.exists():
               file_size = len(image_data.getbuffer())
               upload_offset = 0
               while upload_offset < file_size:
                   chunk_data = image_data.read(AZURE_BLOB_MAX_CHUNK_SIZE)
                   chunk_size = len(chunk_data)
                   blob_client.upload_blob(chunk_data, length=chunk_size, max_concurrency=4)
                   upload_offset += chunk_size
           else:
               print(f"Resource by blob {image_name} Already exists. Moving to next...")

           content_settings = ContentSettings(content_type=response.headers.get("Content-Type"))
           blob_client.set_http_headers(content_settings=content_settings)
           
           if blob_client.exists():
               print(f"The blob {image_name} was uploaded successfully to Azure Blob Storage!")
               sas_expiry = datetime.now() + timedelta(days=365*1000)
               sas_permissions = BlobSasPermissions(read=True)
               sas_token = generate_blob_sas(
                   account_name=AZURE_BLOB_ACCOUNT_NAME,
                   account_key=AZURE_BLOB_ACCOUNT_KEY,
                   container_name=AZURE_BLOB_CONTAINER_NAME,
                   blob_name=image_name,
                   permission=sas_permissions,
                   expiry=sas_expiry,
               )
               image_url = blob_client.url
               image_paths.append(image_url)
           else:
               print(f"There was an error uploading the blob {image_name} to Azure Blob Storage.")

       except Exception as e:
           print(f"Error processing {image_url}: {e}")
           continue

   return image_paths

def rotate_headers():
    HEADERS["user-agent"] = USER_AGENTS[randint(0, len(USER_AGENTS) - 1)]
    return HEADERS
