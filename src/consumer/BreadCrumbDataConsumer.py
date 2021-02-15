import logging
import os
from confluent_kafka.cimpl import Consumer
from datetime import datetime

from src.consumer.BreadCrumb import BreadCrumb
from src.definitions import BREADCRUMB_DATA_TOPIC, CONSUMER_OUTPUT_DIR
from src.kafka.KafkaHelper import KafkaHelper
from src.postgres.BreadCrumbRepository import BreadCrumbRepository


class BreadCrumbDataConsumer:
    _logger = logging.getLogger('BreadCrumbDataConsumer')

    def __init__(self):
        kafka_configs = KafkaHelper.get_kafka_configs()
        kafka_configs['group.id'] = 'python_breadcrumb_data_consumer'
        kafka_configs['auto.offset.reset'] = 'earliest'
        self._consumer = Consumer(kafka_configs)

        self._bread_crumb_repo = BreadCrumbRepository()

    def consume_breadcrumb_records(self, topic=BREADCRUMB_DATA_TOPIC, messages_output_file_name='consumed_messages.txt'):
        with open(os.path.join(CONSUMER_OUTPUT_DIR, messages_output_file_name), 'a') as messages_output_file:

            self._logger.info("Starting breadcrumb consumer for {} topic. Messages will be written to {} file.".format(topic, messages_output_file))
            self._consumer.subscribe([topic])

            consumed_records_count = 0
            records_saved_to_db_count = 0
            breadcrumbs = list()
            last_saved_to_db = datetime.now()
            try:
                while True:

                    duration_from_last_saved_to_db = datetime.now() - last_saved_to_db
                    if len(breadcrumbs) >= 50_000 or (len(breadcrumbs) > 0 and duration_from_last_saved_to_db.total_seconds() > (60 * 2)):
                        self._bread_crumb_repo.bulk_save_breadcrumbs(breadcrumbs)
                        records_saved_to_db_count += len(breadcrumbs)
                        breadcrumbs.clear()
                        last_saved_to_db = datetime.now()

                        self._logger.info('Number of messages consumed = {}, records saved to db = {}'.format(consumed_records_count, records_saved_to_db_count))

                    msg = self._consumer.poll(1.0)
                    if msg is None:
                        continue
                    elif msg.error():
                        self._logger.error('error: {}'.format(msg.error()))
                    else:
                        consumed_records_count += 1
                        message_data = msg.value().decode("utf-8")
                        messages_output_file.write(message_data + '\n')
                        self._logger.debug('Number of messages consumed = {}'.format(consumed_records_count))

                        try:
                            breadcrumb = BreadCrumb.parse_raw(message_data)
                            breadcrumbs.append(breadcrumb)
                        except Exception as ex:
                            self._logger.debug('Encountered an error parsing a bread crumb.', ex)
            finally:
                self._consumer.close()
                self._bread_crumb_repo.bulk_save_breadcrumbs(breadcrumbs)
