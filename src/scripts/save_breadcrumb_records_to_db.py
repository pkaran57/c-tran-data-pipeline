from os import listdir

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

    json_file_names = [file for file in listdir(DOWNLOADER_OUTPUT_DIR) if file.endswith('.json')]

    logger.info(f'Saving records for {len(json_file_names)} dates ...')
    for json_file_name in tqdm(json_file_names):
        breadcrumb_records = BreadCrumbDataDownloader.load_downloaded_data(os.path.join(DOWNLOADER_OUTPUT_DIR, json_file_name))

        logger.info('Validating breadcrumb records ...')
        breadcrumbs = list()
        for breadcrumb_dict in tqdm(breadcrumb_records):
            try:
                bread_crumb = BreadCrumb.parse_obj(breadcrumb_dict)
                breadcrumbs.append(bread_crumb)
            except Exception as ex:
                # logger.error('Encountered an error parsing a bread crumb.', ex)
                continue

        bread_crumb_repo = BreadCrumbRepository()
        bread_crumb_repo.bulk_save_breadcrumbs(breadcrumbs, None)
