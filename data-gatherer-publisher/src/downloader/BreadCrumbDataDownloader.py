import cgi
import json
import logging
import os
import shutil
import sys

import requests

from definitions import OUTPUT_DIR


class BreadCrumbDataDownloader:
    _BREADCRUMB_DATA_SERVICE_URL = 'http://rbi.ddns.net/getBreadCrumbData'

    _logger = logging.getLogger('BreadCrumbDataDownloader')
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    @classmethod
    def download_daily_data(cls):
        """
        :return: path of file that the data was downloaded to, None if the data was already downloaded prior to this method call
        """
        cls._logger.info('Starting download ...')

        response = requests.get(cls._BREADCRUMB_DATA_SERVICE_URL, stream=True)

        assert response.ok, "Got the following response code on downloading file: {}".format(response.status_code)

        file_name = cgi.parse_header(response.headers['content-disposition'])[1]['filename']
        file_path = os.path.join(OUTPUT_DIR, file_name)

        if os.path.exists(file_path) and os.path.isfile(file_path):
            cls._logger.info("A file with name '{}' already exists at '{}', not downloading the file again.".format(file_name, file_path))
            return None

        with open(file_path, 'wb') as file:
            shutil.copyfileobj(response.raw, file)

        cls._logger.info('Downloaded the following file: {}'.format(file_path))

        return file_path

    @classmethod
    def load_downloaded_data(cls, data_file_path):
        with open(data_file_path) as data_file:
            data = json.load(data_file)
            cls._logger.info('Found {} records from {} file'.format(len(data), data_file))
            return data
