# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
import os
import uuid
import pymongo

from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter

class UbereatsCrawlerPipeline:

    def __init__(self):
        self._client = pymongo.MongoClient(
            os.environ.get('MONGODB_URI')
        )
        self._db = self._client[os.environ.get('MONGODB_DB')]
        self._collection = self._db[os.environ.get('MONGODB_COLLECTION')]

    def close_spider(self, spider):
        self._collection.create_index([('geo', pymongo.GEOSPHERE)])

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        label = adapter.get('label')
        data = adapter.get('data')
        if (label is None) or (data is None):
            raise DropItem('No label and data found in item.')

        data['label'] = label

        # create index.
        data["_id"] = data["uuid"]
        try:
            data['_id'] = data['uuid']
        except KeyError:
            data['_id'] = data['uuid'] = uuid.uuid4()
        try:
            data['geo'] = {
                'type': 'Point',
                'coordinates': [data['location']['longitude'], data['location']['latitude']]
            }
        except KeyError:
            data['geo'] = {
                'type': 'Point',
                'coordinates': [0, 0]
            }

        # create embbeding.
        # TODO:

        self._collection.update_one(
            {'_id': data['_id']},
            {'$set': data},
            upsert=True
        )
