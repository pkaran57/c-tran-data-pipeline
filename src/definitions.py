import os

ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)  # Root to the project
DOWNLOADER_OUTPUT_DIR = os.path.join(ROOT_DIR, 'data-downloader-output')
CONSUMER_OUTPUT_DIR = os.path.join(ROOT_DIR, 'data-consumer-output')
CONFIGS_DIR = os.path.join(ROOT_DIR, 'configs')
LOG_DIR = os.path.join(ROOT_DIR, 'logs')

BREADCRUMB_DATA_TOPIC = 'breadcrumb-data-topic'
