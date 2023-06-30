# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
import os
import uuid
import pymongo
import logging

from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter


class RestaurantDocumentTransformer:

    def __init__(self, data):
        self.data = data

    def __iter__(self):
        assert self.data, 'RestaurantDocumentTransformer: No data to transform.'
        for data in self.data:
            yield data


class RestaurantDocumentIdentityTransformer(RestaurantDocumentTransformer):

    def __init__(self, data):
        super().__init__(data)


class RestaurantItemFlattenTransformer(RestaurantDocumentTransformer):
    '''
    Flatten the items offered by all the restaurants in the data into a single row of informations.

    The following information is extracted from the restaurant document:
    - restaurant_id
    - restaurant_name
    - restaurant_address
    - restaurant_geo
    - restaurant_category

    - item_id
    - item_name
    - item_description
    - item_image_url
    '''

    def cols(self):
        '''
        Return the list of columns in the order they are returned by the iterator.
        '''
        return [
            'restaurant_id',
            'restaurant_name',
            'restaurant_address',
            'restaurant_geo',
            'restaurant_category',
            'item_id',
            'item_name',
            'item_description',
            'item_image_url',
        ]

    def __iter__(self):
        for restaurant in self.data:
            restaurant_id = restaurant.get('_id')
            restaurant_name = restaurant.get('name')
            restaurant_address = restaurant.get('location', {}).get('address')
            restaurant_lat = restaurant.get('location', {}).get('latitude')
            restaurant_lon = restaurant.get('location', {}).get('longitude')
            restaurant_category = restaurant.get('categories')

            menus = restaurant.get('catalogSectionsMap', {})
            for menu in menus.values():
                for section in menu:
                    items = section.get("payload", {}).get(
                        "standardItemsPayload", {}).get("catalogItems", [])
                    for item in items:
                        item_id = item['uuid']
                        item_name = item['title']
                        item_description = item.get('itemDescription')
                        item_image_url = item['imageUrl']
                        yield [
                            restaurant_id,
                            restaurant_name,
                            restaurant_address,
                            (restaurant_lat, restaurant_lon),
                            restaurant_category,
                            item_id,
                            item_name,
                            item_description,
                            item_image_url,
                        ]


class RestaurantDocumentToPrompt(RestaurantDocumentTransformer):

    def __init__(self, data):
        super().__init__(data)

    def __iter__(self):
        raise NotImplementedError("Not implemented yet")


class RestaurantLocator:

    def __init__(self, uri, db, collection):
        self._mongo = pymongo.MongoClient(uri)
        self._db = self._mongo[db]
        self._collection = self._db[collection]

    def __call__(self, lat, lon, radius, limit=10):
        filt = {
            'geo': {
                '$near': {
                    '$geometry': {
                        'type': 'Point',
                        'coordinates': [lon, lat]
                    },
                    '$maxDistance': radius
                }
            }
        }

        ret = self._collection.find(filt)
        if limit:
            ret = ret.limit(limit)

        return ret


class UbereatsCrawlerPipeline:

    def __init__(self):
        if os.environ.get('MONGODB_URI'):
            self._dry_run = False
            self._client = pymongo.MongoClient(
                os.environ.get('MONGODB_URI')
            )
            self._db = self._client[os.environ.get('MONGODB_DB')]
            self._collection = self._db[os.environ.get('MONGODB_COLLECTION')]
        else:
            self._dry_run = True
        logging.info(f'Pipeline dry run: {self._dry_run}')

    def close_spider(self, spider):
        self._collection.create_index([('geo', pymongo.GEOSPHERE)])

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        label = adapter.get('label')
        data_ = adapter.get('data')
        if (label is None) or (data_ is None):
            raise DropItem('No label and data found in item.')
        
        data = dict(data_)
        data['label'] = label

        if self._dry_run:
            logging.info(f'Dry run: {data}')
            return

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

        return data["storeURL"], data["uuid"]
