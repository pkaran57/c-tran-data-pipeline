import logging
import os
import sqlalchemy
from tqdm import tqdm

from src.consumer.BreadCrumb import BreadCrumb
from src.definitions import DOWNLOADER_OUTPUT_DIR
from src.downloader.BreadCrumbDataDownloader import BreadCrumbDataDownloader
from src.postgres.BreadCrumbRepository import BreadCrumbRepository

logging.basicConfig(format="'%(asctime)s' %(name)s : %(message)s'", level=logging.INFO)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
logger = logging.getLogger('temp')

if __name__ == '__main__':
    logger.info('sqlalchemy version = {}'.format(sqlalchemy.__version__))

    breadcrumb_records = BreadCrumbDataDownloader.load_downloaded_data(os.path.join(DOWNLOADER_OUTPUT_DIR, '2021-01-19.json'))

    logger.info('Validating breadcrumb records ...'.format(sqlalchemy.__version__))
    breadcrumbs = list()
    for breadcrumb_dict in tqdm(breadcrumb_records):
        try:
            bread_crumb = BreadCrumb.parse_obj(breadcrumb_dict)
            breadcrumbs.append(bread_crumb)
        except Exception as ex:
            # logger.error('Encountered an error parsing a bread crumb.', ex)
            continue

    bread_crumb_repo = BreadCrumbRepository()
    bread_crumb_repo.bulk_save_breadcrumbs(breadcrumbs)
