import scrapy
import json
import re
import logging

from pathlib import Path
from ..items import UbereatsCrawlerItem

# self-defined modules
from .constants import URL_ROOT
from .constants import URL_GET_SEO_FEED
from .constants import URL_GET_STORE_INFO
from .constants import XPATH_CATEGORIES
from .constants import XPATH_UUID_SCRIPT


class UbereatsSpider(scrapy.Spider):
    # Name of the spider
    name = 'ubereats'

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)

        self.__store_uuid_seen = set()

    def start_requests(self):
        # current dir is top level dir of the project
        city_file = open('./major_cities.txt', mode='r')
        for city in city_file:
            yield scrapy.Request(url=f'{URL_ROOT}/city/{city}',
                                 callback=self.__get_all_menus_by_city,
                                 errback=self.__process_failed_request,
                                 cb_kwargs={'label': f'{city}'})

    def parse(self, response):
        raise Exception(
            "No default callback parser! Please specify callback in each scrapy request."
        )

    def __get_all_menus_by_city(self, response, label):
        all_category_paths = self.__get_all_category_paths(response)

        for category in all_category_paths:
            yield scrapy.Request(
                url=URL_GET_SEO_FEED,
                callback=self.__get_all_menus_by_city_and_category,
                errback=self.__process_failed_request,
                method='POST',
                headers={
                    'content-type': 'application/json',
                    'x-csrf-token': 'x',
                },
                body=json.dumps({
                    'pathname': category,
                }),
                cb_kwargs={'label': label})

    def __get_all_menus_by_city_and_category(self, response, label):
        feeds = json.loads(response.text)
        if feeds['status'] == 'failure':
            yield {
                'label': 'failure',
                'data': {
                    'url': response.request.url,
                    'body': json.loads(response.request.body)
                }
            }
            return

        for item in feeds["data"]["elements"][4]["feedItems"]:
            uuid = item["uuid"]
            if uuid not in self.__store_uuid_seen:
                self.__store_uuid_seen.add(uuid)
                yield scrapy.Request(url=URL_GET_STORE_INFO,
                                     callback=self.__process_store_info,
                                     errback=self.__process_failed_request,
                                     method='POST',
                                     headers={
                                         'content-type': 'application/json',
                                         'x-csrf-token': 'x',
                                     },
                                     body=json.dumps({'storeUuid': uuid}),
                                     cb_kwargs={
                                         'label': label,
                                         'uuid': uuid
                                     })

    def __process_store_info(self, response, label, uuid):
        res = json.loads(response.text)

        if res['status'] == 'failure':
            yield {'label': 'failure', 'data': {'uuid': uuid}}
        else:
            data = res['data']
            item = UbereatsCrawlerItem(
                uuid=data['uuid'],
                name=data['title'],
                location=data['location'],
                hours=data['hours'],
                categories=data['categories'],
                sections=data['sections'],
                reviews=data['storeReviews'],
                catalogSectionsMap=data['catalogSectionsMap'])
            yield {'label': label, 'data': item}

    def __process_failed_request(self, failure):
        self.log(f"Fail to request {failure.request.url}",
                 level=logging.WARNING)

    def __get_all_category_paths(self, response):
        """The method returns a list of url paths of all categories scawled
        from 'https://www.ubereats.com/city/{city_name}-{state_postal_abbr}'.
        
        Example return is 
        ['/category/berkeley-ca/fast-food', 
        '/category/berkeley-ca/breakfast-and-brunch', ... ] 
        """
        return response.xpath(XPATH_CATEGORIES).getall()[1:]

    def __get_all_store_uuids_from_script(self, response):
        """The method finds all store uuids encrypted in html script
        (<script type="application/json" id="__REDUX_STATE__">) 
        on the page 'https://www.ubereats.com/category/{city_name}-{state_postal_abbr}/{category_name}'.

        Duplicated uuids are removed.
        """
        script = response.xpath(XPATH_UUID_SCRIPT).get()

        if script is None:
            raise Exception(
                "Failed to extract script that includes uuids from page. Page structure may have changed."
            )

        regex_uuid_from_script = r"storeUUID[^-]*([0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})"

        unckecked_uuids = re.findall(pattern=regex_uuid_from_script,
                                     string=script)

        uuids = []
        for uuid in unckecked_uuids:
            if uuid not in self.__store_uuid_seen:
                uuids.append(uuid)
                self.__store_uuid_seen.add(uuid)

        return uuids