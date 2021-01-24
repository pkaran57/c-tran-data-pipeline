import logging
import os
import sys

from definitions import LOG_DIR
from downloader.BreadCrumbDataDownloader import BreadCrumbDataDownloader
from publisher.BreadCrumbDataPublisher import BreadCrumbDataPublisher

logging.basicConfig(filename=os.path.join(LOG_DIR, 'logs.txt'),
                    filemode='a', format="'%(asctime)s' %(name)s : %(message)s'",
                    level=logging.INFO)

logger = logging.getLogger('main')
logger.addHandler(logging.StreamHandler(sys.stdout))

if __name__ == '__main__':
    downloaded_data_file_path = BreadCrumbDataDownloader.download_daily_data()
    if downloaded_data_file_path:
        breadcrumb_records = BreadCrumbDataDownloader.load_downloaded_data(downloaded_data_file_path)
        BreadCrumbDataPublisher().publish_breadcrumb_records(breadcrumb_records)
