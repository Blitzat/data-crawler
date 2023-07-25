# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
import os
import uuid
import pymongo
import logging
import requests
import pandas as pd
import torch
import clip

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

class EmbeddingsGenerator:

    def __init__(self, df, model):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = model
        self.text_embeddings = self.get_embeddings(df)
        '''
        generate embeddings from text or image data
        '''
    def get_embeddings(self, df):
        item_names = df.item_name
        item_descriptions = df.item_description

        # keep only the first 20 words
        item_texts = item_names.combine(item_descriptions, lambda a, b: (a + " made by " + b).split()[:20] if pd.notna(b) else a)
        item_texts = item_texts.apply(lambda x: ' '.join(x)).tolist()
        if self.device  == "cuda":
            text_tokens = clip.tokenize(item_texts, truncate=True).cuda()
        else:
            text_tokens = clip.tokenize(item_texts, truncate=True)
        text_embeddings = self.encode(text_tokens)
        return text_embeddings
    
    def encode(self, data, batch_size = 1000, is_image=False):
        
        with torch.no_grad():
            
            sum_embeddings = []
            N = data.shape[0]
            batch_size = batch_size
            for i in range(0, N, batch_size):
                if i + batch_size > N:
                    batch = data[i : N]
                else:
                    batch = data[i : i + batch_size]

                if is_image:
                    sum_embeddings = sum_embeddings + [self.model.encode_image(batch).float()]
                else:
                    sum_embeddings = sum_embeddings + [self.model.encode_text(batch).float()]
        
        embeddings = torch.cat(sum_embeddings, dim= 0)
        embeddings /= embeddings.norm(dim=-1, keepdim=True)

        return embeddings
    

class ArzueGeoEncoder:

    def __init__(self):
        self._session = requests.Session()
        self._api_key = os.environ.get('ARZUE_MAPS_API_KEY')
        assert self._api_key, 'ArzueGeoEncoder: No API key provided.'

    def __call__(self, address, country="US"):
        param = {
            "api-version": "1.0",
            "query": address,
            "countrySet": country,
            "subscription-key": self._api_key,
        }
        endpoint = "https://atlas.microsoft.com/search/address/json"
        try:
            response = self._session.get(endpoint, params=param)
            response.raise_for_status()
            data = response.json()
            if data['summary']['totalResults'] == 0:
                logging.warning(f'ArzueGeoEncoder: No results found for {address}')
                return 0, 0
            else:
                return data['results'][0]['position']['lat'], data['results'][0]['position']['lon']
        except Exception as e:
            logging.error(f'ArzueGeoEncoder: {e}')
            return 0, 0


class UbereatsCrawlerPipeline:

    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, self.preprocess = clip.load("ViT-B/32", device=self.device)
        self.geo_encoder = ArzueGeoEncoder()
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
                'coordinates': self.geo_encoder(
                    data['location']['address']
                )
            }

        # create embbeding.
        result = RestaurantItemFlattenTransformer([data,])
        col_names = result.cols()
        restaurants_df = pd.DataFrame(data=result, columns=col_names)
        text_embeddings = EmbeddingsGenerator(restaurants_df, self.model).text_embeddings  # get the embeddings for the item
        restaurants_df['text_embeddings'] = text_embeddings.tolist()

        # assgin embedding to each corresponding item
        menus = data.get('catalogSectionsMap', {})
        for menu in menus.values():
            for section in menu:
                items = section.get("payload", {}).get(
                    "standardItemsPayload", {}).get("catalogItems", [])
                for item in items:
                    item_id = item['uuid']
                    item_embedding_series = restaurants_df.loc[restaurants_df['item_id'] == item_id, 'text_embeddings']
                    item_embedding = list(item_embedding_series)[0]
                    item['text_embedding'] = item_embedding


        self._collection.update_one(
            {'_id': data['_id']},
            {'$set': data},
            upsert=True
        )

        return data["storeURL"], data["uuid"]

