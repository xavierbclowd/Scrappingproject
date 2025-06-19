# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import hashlib
import os
import logging
from io import BytesIO
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas, ContentSettings
import requests
import json
import time
from scrapy.exceptions import DropItem
# useful for handling different item types with a single interface
from scrapy.utils.project import get_project_settings
import pydocumentdb.document_client as document_client
from scrapy.exceptions import DropItem

id_time_stamp = datetime.now().strftime("%Y-%m-%d")
class BclowdSpiderPipeline(object):
    def __init__(self,  docdb_host, docdb_db, docdb_coll, docdb_mkey):
        self.docdb_host = docdb_host
        self.docdb_db = docdb_db
        self.docdb_coll = docdb_coll
        self.docdb_mkey = docdb_mkey

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            docdb_host=crawler.settings.get("DOCDB_HOST"),
            docdb_db=crawler.settings.get("DOCDB_DB"),
            docdb_coll=crawler.settings.get("DOCDB_COLLECTION"),
            docdb_mkey=crawler.settings.get("DOCDB_MASTER_KEY"),
        )

    @staticmethod
    def gen_docid_from_string(s):
        m = hashlib.sha256()
        m.update(str.encode(s))
        return m.hexdigest()

    def open_spider(self, spider):
        # create documentDb client instance
        self.client = document_client.DocumentClient(self.docdb_host,
                                                     {'masterKey': self.docdb_mkey})
        # create a database if not yet created
        database_definition = {'id': self.docdb_db}
        databases = list(self.client.QueryDatabases({
            'query': 'SELECT * FROM root r WHERE r.id=@id',
            'parameters': [
                {'name': '@id', 'value': database_definition['id']}
            ]
        }))
        feeddb = None
        if (len(databases) > 0):
            feeddb = databases[0]
        else:
            print("database is created:%s" % self.docdb_db)
            feeddb = self.client.CreateDatabase(database_definition)

        # create a collection if not yet created
        collection_definition = {'id': self.docdb_coll}
        collections = list(self.client.QueryCollections(
            feeddb['_self'],
            {
                'query': 'SELECT * FROM root r WHERE r.id=@id',
                'parameters': [
                    {'name': '@id', 'value': collection_definition['id']}
                ]
            }))
        self.feedcoll = None
        if (len(collections) > 0):
            self.feedcoll = collections[0]
        else:
            print("collection is created:%s" % self.docdb_coll)
            self.feedcoll = self.client.CreateCollection(
                feeddb['_self'], collection_definition)

    # executed in ending spider
    def close_spider(self, spider):
        pass

    def remove_null_empty_fields(self, data):
        if isinstance(data, dict):
            # Filter out null or empty values in the dictionary
            return {key: self.remove_null_empty_fields(value) for key, value in data.items() if
                    value is not None and value != '' and value != []}
        elif isinstance(data, list):
            # Filter out null or empty values in the list
            return [self.remove_null_empty_fields(item) for item in data if item is not None and item != '' and item != []]
        else:
            # Return the value unchanged for non-dictionary and non-list types
            return data

    def process_item(self, item, spider):
        
        if not item.get('sku'):
            spider.logger.warning(
                f"No SKU for {item['content']['en']['sku_link']}, dropping item"
            )
            raise DropItem("missing SKU")
        print(f"Formatted date: {id_time_stamp}")
        docid = BclowdSpiderPipeline.gen_docid_from_string(item['domain'] + item['sku'] + id_time_stamp)
        document_definition = {
            'id': docid,
            'date': [item['date']] if item.get('date') else [],
            'domain': [item['domain']] if item.get('domain') else [],
            'domain_url': [item['domain_url']] if item.get('domain_url') else [],
            'collection_name': [item['collection_name']] if item.get('collection_name') else [],
            'season': [item['season']] if item.get('season') else [],
            'brand': [item['brand']] if item.get('brand') else [],
            'product_badge': [item['product_badge']] if item.get('product_badge') else [],
            'manufacturer': [item['manufacturer']] if item.get('manufacturer') else [],
            'gender': [item['gender']] if item.get('gender') else [],
            'sku': [item['sku']] if item.get('sku') else [],
            'mpn': [item['mpn']] if item.get('mpn') else [],
            'gtin8': [item['gtin8']] if item.get('gtin8') else [],
            'gtin12': [item['gtin12']] if item.get('gtin12') else [],
            'gtin13': [item['gtin13']] if item.get('gtin13') else [],
            'gtin14': [item['gtin14']] if item.get('gtin14') else [],
            'sku_color': [item['sku_color']] if item.get('sku_color') else [],
            'main_material': [item['main_material']] if item.get('main_material') else [],
            'secondary_material': [item['secondary_material']] if item.get('secondary_material') else [],
            'image_url': item['image_url'] if item.get('image_url') else [],
            'size_dimensions': item['size_dimensions'] if item.get('size_dimensions') else [],
            'content': [item['content']] if item.get('content') else [],
            'specification': [item['specification']] if item.get('specification') else [],
            'tags': item['tags'] if item.get('tags') else [],
            'categories': item['categories'] if item.get('categories') else [],
        }

        clean_document_definition = self.remove_null_empty_fields(document_definition)
        # check if duplicated
        documents = list(self.client.QueryDocuments(
            self.feedcoll['_self'],
            {
                'query': 'SELECT * FROM root r WHERE r.id=@id',
                'parameters': [
                    {'name': '@id', 'value': clean_document_definition['id']}
                ]
            }))

        if (len(documents) < 1):
            print("document is added:id:%s" % clean_document_definition['id'])
            created_document = self.client.CreateDocument(self.feedcoll['_self'], clean_document_definition)
            return item

class AzureImageUploadPipeline:
    def __init__(self, azure_account_name, azure_account_key, container_name, max_chunk_size):
        self.azure_account_name = azure_account_name
        self.azure_account_key = azure_account_key
        self.container_name = container_name
        self.max_chunk_size = max_chunk_size
        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{azure_account_name}.blob.core.windows.net/",
            credential=azure_account_key
        )
        self.container_client = self.blob_service_client.get_container_client(container_name)

    @classmethod
    def from_crawler(cls, crawler):
        azure_account_name = crawler.settings.get("AZURE_BLOB_ACCOUNT_NAME")
        azure_account_key = crawler.settings.get("AZURE_BLOB_ACCOUNT_KEY")
        container_name = crawler.settings.get("AZURE_BLOB_CONTAINER_NAME")
        max_chunk_size = crawler.settings.get("AZURE_BLOB_MAX_CHUNK_SIZE", 4 * 1024 * 1024)
        return cls(azure_account_name, azure_account_key, container_name, max_chunk_size)

    def process_item(self, item, spider):
        # Process only if there are image URLs to upload
        if 'image_url' in item and item['image_url']:
            azure_image_urls = []
            for url in item['image_url']:
                if not url or url.strip() == "" or url == "N/A":
                    continue

                # Ensure URL has scheme
                if url.startswith('//'):
                    url = 'https:' + url

                try:
                    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, stream=True, timeout=30)
                    if response.status_code != 200:
                        spider.logger.error(f"Failed to download image {url} - Status: {response.status_code}")
                        continue

                    image_data = BytesIO(response.content)
                    image_name = os.path.basename(url.split("?")[0])
                    blob_client = self.container_client.get_blob_client(blob=image_name)

                    if not blob_client.exists():
                        image_data.seek(0)
                        blob_client.upload_blob(image_data, overwrite=True)

                    # Generate a SAS token valid for 1 year
                    sas_expiry = datetime.now() + timedelta(days=365*1000)
                    sas_permissions = BlobSasPermissions(read=True)
                    sas_token = generate_blob_sas(
                        account_name=self.azure_account_name,
                        account_key=self.azure_account_key,
                        container_name=self.container_name,
                        blob_name=image_name,
                        permission=sas_permissions,
                        expiry=sas_expiry,
                    )
                    azure_url = f"{blob_client.url}?{sas_token}"
                    azure_image_urls.append(azure_url)
                except Exception as e:
                    spider.logger.error(f"Error processing image {url}: {e}")
                    continue

            item['image_url'] = azure_image_urls  # Replace raw URLs with Azure URLs
        return item