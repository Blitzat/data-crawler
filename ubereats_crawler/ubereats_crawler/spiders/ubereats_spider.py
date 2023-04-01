import scrapy
import json
import re

from pathlib import Path
from urllib.parse import unquote

# self-defined modules
from .constants import URL_ROOT
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
        city_file = open('./major_cities.json', mode='r')
        cities = json.load(city_file)
        for state, cities in cities.items():
            for city in cities:
                yield scrapy.Request(url=f'{URL_ROOT}/city/{city}-{state}',
                                     callback=self.__get_all_menus_by_city)

    def parse(self, response):
        raise Exception(
            "No default callback parser! Please specify callback in each scrapy request."
        )

    def __get_all_menus_by_city(self, response):
        all_category_paths = self.__get_all_category_paths(response)

        for category in all_category_paths:
            yield scrapy.Request(
                url=f'{URL_ROOT}{category}',
                callback=self.__get_all_menus_by_city_category)

    def __get_all_menus_by_city_category(self, response):
        uuids = self.__get_all_store_uuid(response)
        for uuid in uuids:
            yield {"uuid": uuid}

    def __get_all_category_paths(self, response):
        """
        The method returns a list of url paths of all categories scawled
        from 'https://www.ubereats.com/city/{city_name}-{state_postal_abbr}'.
        
        Example return is 
        ['/category/berkeley-ca/fast-food', 
        '/category/berkeley-ca/breakfast-and-brunch', ... ] 
        """
        return response.xpath(XPATH_CATEGORIES).getall()

    def __get_all_store_uuid(self, response):
        """
        The method finds all store uuids encrypted in html script
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
