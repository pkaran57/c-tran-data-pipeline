import json
import logging
import os
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.definitions import DOWNLOADER_OUTPUT_DIR


class StopEventsDataDownloader:
    _ENDPOINT_URL = 'http://rbi.ddns.net/getStopEvents'
    _logger = logging.getLogger('StopEventsDataDownloader')

    @classmethod
    def download_and_get_daily_stop_events(cls):
        """
        :return: path of file that the data was downloaded to, None if the data was already downloaded prior to this method call
        """
        file_path = cls._get_file_path()

        cls._logger.info('Starting download ...')

        response = requests.get(cls._ENDPOINT_URL)
        assert response.ok, "Got the following response code on downloading file: {}".format(response.status_code)

        soup = BeautifulSoup(response.content, 'lxml')
        stop_events_dict = dict()
        for tag in soup.find_all('table'):
            trip_id = re.search('Stop Events for trip (.+?) for today', str(tag.previous)).group(1)
            trip_df = cls._get_table_df(tag)

            assert trip_id
            assert trip_df is not None

            stop_events_dict[trip_id] = trip_df.to_json(orient="records")

        with open(file_path, 'w') as file:
            json.dump(stop_events_dict, file)

        return stop_events_dict

    @classmethod
    def _get_file_path(cls):
        return os.path.join(DOWNLOADER_OUTPUT_DIR, datetime.today().strftime('%Y-%m-%d') + '-stop-events.json')

    @staticmethod
    def _get_table_df(tag):
        return pd.read_html(str(tag))[0]

    @classmethod
    def load_downloaded_data(cls, data_file_path):
        with open(data_file_path) as data_file:
            data = json.load(data_file)
            cls._logger.info('Found {} records from {} file'.format(len(data), data_file))

            parsed_data = dict()
            for key, value in data.items():
                parsed_data[key] = pd.read_json(value)

            return parsed_data
