import logging
import os

from confluent_kafka.cimpl import Consumer

from definitions import CONSUMER_OUTPUT_DIR, BREADCRUMB_DATA_TOPIC
from kafka.KafkaHelper import KafkaHelper


class BreadCrumbDataConsumer:
    _logger = logging.getLogger('BreadCrumbDataConsumer')

    def __init__(self):
        kafka_configs = KafkaHelper.get_kafka_configs()
        kafka_configs['group.id'] = 'python_breadcrumb_data_consumer'
        kafka_configs['auto.offset.reset'] = 'earliest'

        self._consumer = Consumer(kafka_configs)

    def consume_breadcrumb_records(self, topic=BREADCRUMB_DATA_TOPIC, messages_output_file_name='consumed_messages.txt'):
        with open(os.path.join(CONSUMER_OUTPUT_DIR, messages_output_file_name), 'a') as messages_output_file:

            self._logger.info("Starting breadcrumb consumer for {} topic. Messages will be written to {} file.".format(topic, messages_output_file))
            self._consumer.subscribe([topic])

            consumed_records_count = 0
            try:
                while True:
                    msg = self._consumer.poll(1.0)
                    if msg is None:
                        continue
                    elif msg.error():
                        self._logger.error('error: {}'.format(msg.error()))
                    else:
                        consumed_records_count += 1
                        message_data = str(msg.value())
                        messages_output_file.write(message_data)
                        self._logger.info('Number of messages consumed = {}'.format(consumed_records_count))
            finally:
                self._consumer.close()
