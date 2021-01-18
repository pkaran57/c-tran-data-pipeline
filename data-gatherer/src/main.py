import cgi
import logging
import os
import shutil

import requests

from definitions import OUTPUT_DIR

logging.basicConfig(format="'%(asctime)s' %(name)s : %(message)s'", level=logging.INFO)
logger = logging.getLogger('main')


def download_data():
    logger.info('Starting download ...')

    response = requests.get('http://rbi.ddns.net/getBreadCrumbData', stream=True)

    assert response.ok, "Got the following response code on downloading file: {}".format(response.status_code)

    file_name = cgi.parse_header(response.headers['content-disposition'])[1]['filename']
    file_path = os.path.join(OUTPUT_DIR, file_name)

    if os.path.exists(file_path) and os.path.isfile(file_path):
        logging.info("A file with name '{}' already exists at '{}', not downloading the file again.".format(file_name, file_path))
        return

    with open(file_path, 'wb') as file:
        shutil.copyfileobj(response.raw, file)

    logger.info('Downloaded the following file: {}'.format(file_path))


if __name__ == '__main__':
    download_data()
