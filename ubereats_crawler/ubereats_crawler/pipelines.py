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

    def __init__(self):
        self.files = {}
        self.out_dir = None

    def open_spider(self, spider):
        if not hasattr(spider, 'OUTDIR'):
            raise Exception(
                "Must specify output directory by adding '-a OUTDIR=output_dir'"
            )

        self.out_dir = os.path.join(os.getcwd(), spider.OUTDIR)
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        # time = datetime.now().strftime(r"%Y-%m-%d %H:%M:%S")
        # self.out_dir = os.path.join(self.out_dir, time)
        # os.makedirs(self.out_dir)

    def close_spider(self, spider):
        for label in self.files:
            self.files[label].write(']')
            self.files[label].close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        label = adapter.get('label')
        data = adapter.get('data')
        if (label is not None) and (data is not None):
            if label not in self.files:
                file_name = os.path.join(self.out_dir, f'{label}.json')
                self.files[label] = open(file_name, 'a+')
                self.files[label].write('[')
            else:
                self.files[label].write(',')

            # append each item as a json obj to the file
            json.dump(obj=dict(data), fp=self.files[label], ensure_ascii=False)
            return item

        else:
            raise DropItem(f"Missing fields in {item}")
