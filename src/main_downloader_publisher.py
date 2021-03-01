# Download daily breadcrumb data and publish it to Kafka topic

import logging
import os

from downloader.BreadCrumbDataDownloader import BreadCrumbDataDownloader
from log.LoggerHelper import init_root_logger
from publisher.BreadCrumbDataPublisher import BreadCrumbDataPublisher
from src.downloader.StopEventsDataDownloader import StopEventsDataDownloader
from src.publisher.StopEventsDataPublisher import StopEventsDataPublisher

file_name = os.path.basename(__file__)

init_root_logger(file_name)
logger = logging.getLogger(file_name)

if __name__ == '__main__':
    downloaded_data_file_path = BreadCrumbDataDownloader.download_daily_data()

    if downloaded_data_file_path:

        downloader = StopEventsDataDownloader()
        stp_events = downloader.download_and_get_daily_stop_events()
        StopEventsDataPublisher().publish_stop_event_records(stp_events)

        breadcrumb_records = BreadCrumbDataDownloader.load_downloaded_data(downloaded_data_file_path)
        BreadCrumbDataPublisher().publish_breadcrumb_records(breadcrumb_records)
