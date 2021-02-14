import logging
import os
import sys

from src.definitions import LOG_DIR

LOG_FORMATTER = "'%(asctime)s' %(name)s : %(message)s'"


def init_root_logger(file_name, log_level=logging.INFO):
    logging.basicConfig(filename=os.path.join(LOG_DIR, '{}.txt'.format(file_name)),
                        filemode='a', format=LOG_FORMATTER,
                        level=log_level)

    root_logger = logging.getLogger()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMATTER))
    root_logger.addHandler(handler)
