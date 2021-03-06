# print num of records in all downloaded json files

from os import listdir

import os

from src.definitions import DOWNLOADER_OUTPUT_DIR
from src.downloader.BreadCrumbDataDownloader import BreadCrumbDataDownloader

if __name__ == '__main__':
    json_files = [file for file in listdir(DOWNLOADER_OUTPUT_DIR) if file.endswith('.json')]

    for file in sorted(json_files):
        records = BreadCrumbDataDownloader.load_downloaded_data(os.path.join(DOWNLOADER_OUTPUT_DIR, file))
        print('Found {} records in {}'.format(len(records), file))
