# Download daily breadcrumb data and publish it to Kafka topic

import logging
import os
import sys

from definitions import LOG_DIR
from downloader.BreadCrumbDataDownloader import BreadCrumbDataDownloader
from publisher.BreadCrumbDataPublisher import BreadCrumbDataPublisher

LOG_FORMATTER = "'%(asctime)s' %(name)s : %(message)s'"

logging.basicConfig(filename=os.path.join(LOG_DIR, 'logs.txt'),
                    filemode='a', format=LOG_FORMATTER,
                    level=logging.INFO)

root_logger = logging.getLogger()

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(LOG_FORMATTER))
root_logger.addHandler(handler)

logger = logging.getLogger('main_downloader_publisher')
logger.addHandler(logging.StreamHandler(sys.stdout))

if __name__ == '__main__':
    downloaded_data_file_path = BreadCrumbDataDownloader.download_daily_data()

    if downloaded_data_file_path:
        breadcrumb_records = BreadCrumbDataDownloader.load_downloaded_data(downloaded_data_file_path)
        BreadCrumbDataPublisher().publish_breadcrumb_records(breadcrumb_records)
