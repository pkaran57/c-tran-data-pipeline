import json
import logging
import sys

from confluent_kafka.cimpl import Producer

from definitions import BREADCRUMB_DATA_TOPIC
from kafka.KafkaHelper import KafkaHelper


class BreadCrumbDataPublisher:
    _logger = logging.getLogger('BreadCrumbDataPublisher')
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    def __init__(self):
        kafka_configs = KafkaHelper.get_kafka_configs()
        self._producer = Producer(kafka_configs)

    def publish_breadcrumb_records(self, breadcrumb_records, topic=BREADCRUMB_DATA_TOPIC):
        self._logger.info("Publishing {} breadcrumb records to {} topic ...".format(len(breadcrumb_records), topic))
        delivered_records = 0

        def callback(err, msg):
            nonlocal delivered_records
            if err is not None:
                self._logger.error("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            else:
                delivered_records += 1
                self._logger.debug("Published record to topic {} partition [{}] @ offset {}"
                                   .format(msg.topic(), msg.partition(), msg.offset()))
                self._logger.debug('Published records count: {}'.format(delivered_records))

        for record in breadcrumb_records:
            self._producer.produce(topic, value=json.dumps(record), on_delivery=callback)
            self._producer.poll(0)

        self._producer.flush()

        self._logger.info('Done delivering records to {} topic! A total of {} records were published'.format(topic, delivered_records))
