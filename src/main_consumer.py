# consume messages from topic and write them to file

import logging
import os

from log.LoggerHelper import init_root_logger
from src.consumer.BreadCrumbDataConsumer import BreadCrumbDataConsumer

file_name = os.path.basename(__file__)

init_root_logger(file_name)
logger = logging.getLogger(file_name)

if __name__ == '__main__':
    BreadCrumbDataConsumer().consume_breadcrumb_records()
