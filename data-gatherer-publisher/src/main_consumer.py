# consume messages from topic and write them to file

import logging

from consumer.BreadCrumbDataConsumer import BreadCrumbDataConsumer

logging.basicConfig(format="'%(asctime)s' %(name)s : %(message)s'", level=logging.INFO)
logger = logging.getLogger('main_consumer')

if __name__ == '__main__':
    BreadCrumbDataConsumer().consume_breadcrumb_records()
