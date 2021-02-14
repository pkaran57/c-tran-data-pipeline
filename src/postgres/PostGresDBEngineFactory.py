import logging
import os
from configparser import ConfigParser
from sqlalchemy import create_engine

from src.definitions import CONFIGS_DIR


class PostGresDBEngineFactory:
    _logger = logging.getLogger(__name__)

    _SSL_KEY = 'sslkey'
    _SSL_CERT = 'sslcert'
    _SSL_ROOT_CERT = 'sslrootcert'
    _SSL_MODE = 'sslmode'

    _engine = None

    @classmethod
    def get_engine(cls):
        if cls._engine:
            return cls._engine
        else:
            cls._logger.info('Creating postgres engine ...')
            cls._engine = create_engine(cls._get_connection_url(), connect_args=cls._get_ssl_properties())
            return cls._engine

    @classmethod
    def _get_connection_url(cls):
        config_parser = ConfigParser()
        config_parser.read(os.path.join(CONFIGS_DIR, 'postgres.ini'))

        configs = dict(config_parser['DEFAULT'])

        return "postgresql://{}:{}@{}:{}/{}".format(
            configs['user'], configs['password'], configs['host'], configs['port'], configs['dbname']
        )

    @classmethod
    def _get_ssl_properties(cls):
        config_parser = ConfigParser()
        config_parser.read(os.path.join(CONFIGS_DIR, 'postgres.ini'))

        configs = dict(config_parser['DEFAULT'])

        ssl_properties = dict()
        ssl_properties[cls._SSL_MODE] = configs[cls._SSL_MODE]
        ssl_properties[cls._SSL_ROOT_CERT] = configs[cls._SSL_ROOT_CERT]
        ssl_properties[cls._SSL_CERT] = configs[cls._SSL_CERT]
        ssl_properties[cls._SSL_KEY] = configs[cls._SSL_KEY]

        return ssl_properties
