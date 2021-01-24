import logging
import os
import sys

from definitions import LOG_DIR, OUTPUT_DIR
from downloader.BreadCrumbDataDownloader import BreadCrumbDataDownloader
from publisher.BreadCrumbDataPublisher import BreadCrumbDataPublisher

logging.basicConfig(filename=os.path.join(LOG_DIR, 'logs.txt'),
                    filemode='a', format="'%(asctime)s' %(name)s : %(message)s'",
                    level=logging.INFO)

logger = logging.getLogger('main')
logger.addHandler(logging.StreamHandler(sys.stdout))

if __name__ == '__main__':
    data_file_path = None
    if len(sys.argv) == 2:
        data_file_name = sys.argv[1]
        logger.info("Got {} as data file name that will be used to publish records to kafka".format(data_file_name))
        data_file_path = os.path.join(OUTPUT_DIR, data_file_name)

    downloaded_data_file_path = BreadCrumbDataDownloader.download_daily_data()

    if downloaded_data_file_path or data_file_path:
        breadcrumb_records = BreadCrumbDataDownloader.load_downloaded_data(downloaded_data_file_path if downloaded_data_file_path else data_file_path)
        BreadCrumbDataPublisher().publish_breadcrumb_records(breadcrumb_records)
