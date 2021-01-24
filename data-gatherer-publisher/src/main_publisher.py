# Publish daily breadcrumb data from given data file

import logging
import os
import sys

from definitions import DOWNLOADER_OUTPUT_DIR
from downloader.BreadCrumbDataDownloader import BreadCrumbDataDownloader
from publisher.BreadCrumbDataPublisher import BreadCrumbDataPublisher

logging.basicConfig(format="'%(asctime)s' %(name)s : %(message)s'", level=logging.INFO)
logger = logging.getLogger('main_publish')

if __name__ == '__main__':
    if len(sys.argv) == 1:
        logger.error('Please pass on the name of the json data file as a param. The file will be searched for from the {} directory'.format(DOWNLOADER_OUTPUT_DIR))
        sys.exit(-1)

    data_file_name = None
    if len(sys.argv) >= 2:
        data_file_name = sys.argv[1]
        logger.info("Got {} as data file name that will be used to publish records to kafka".format(data_file_name))

    if data_file_name:
        breadcrumb_records = BreadCrumbDataDownloader.load_downloaded_data(os.path.join(DOWNLOADER_OUTPUT_DIR, data_file_name))
        BreadCrumbDataPublisher().publish_breadcrumb_records(breadcrumb_records)
