import os
from configparser import ConfigParser

from definitions import CONFIGS_DIR


class KafkaHelper:

    @classmethod
    def get_kafka_configs(cls):
        config_parser = ConfigParser()
        config_parser.read(os.path.join(CONFIGS_DIR, 'kafka.ini'))

        return dict(config_parser['DEFAULT'])
