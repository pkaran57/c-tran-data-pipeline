import logging
import os
import sys

from definitions import LOG_DIR

LOG_FORMATTER = "'%(asctime)s' %(name)s : %(message)s'"


def init_root_logger(file_name):
    logging.basicConfig(filename=os.path.join(LOG_DIR, '{}.txt'.format(file_name)),
                        filemode='a', format=LOG_FORMATTER,
                        level=logging.INFO)

    root_logger = logging.getLogger()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMATTER))
    root_logger.addHandler(handler)
