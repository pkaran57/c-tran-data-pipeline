import os
from configparser import ConfigParser

from definitions import CONFIGS_DIR


class KafkaHelper:

    @classmethod
    def get_kafka_configs(cls):
        config_parser = ConfigParser()
        config_parser.read(os.path.join(CONFIGS_DIR, 'kafka.ini'))

        return {
            'bootstrap.servers': config_parser['DEFAULT']['bootstrap.servers'],
            'sasl.mechanisms': config_parser['DEFAULT']['sasl.mechanisms'],
            'security.protocol': config_parser['DEFAULT']['security.protocol'],
            'sasl.username': config_parser['DEFAULT']['sasl.username'],
            'sasl.password': config_parser['DEFAULT']['sasl.password']
        }
