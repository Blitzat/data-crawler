# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
import os
import json

from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter
from datetime import datetime


class UbereatsCrawlerPipeline:

    # output dir for crawling this time
    out_dir: str

    def __init__(self):
        self.cur_city = None
        self.cur_file = None

    def open_spider(self, spider):
        output_top_dir = os.path.join(os.getcwd(), "outputs")
        if not os.path.exists(output_top_dir):
            os.makedirs(output_top_dir)

        time = datetime.now().strftime(r"%Y-%m-%d %H:%M:%S")
        self.out_dir = os.path.join(output_top_dir, time)
        os.makedirs(self.out_dir)

    def close_spider(self, spider):
        self.cur_file.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        city = adapter.get('city')
        content = adapter.get('content')
        if (city is not None) and (content is not None):
            if self.cur_city != city:
                # close previous file
                if self.cur_file is not None:
                    self.cur_file.close()

                file_name = os.path.join(self.out_dir, f'{city}.txt')
                self.cur_file = open(file_name, 'a+')
                self.cur_city = city

            # append each item as a json obj to the file
            json.dump(obj=adapter.get('content'),
                      fp=self.cur_file,
                      ensure_ascii=False)
            self.cur_file.write(os.linesep)
            return item

        else:
            raise DropItem(f"Missing fields in {item}")
