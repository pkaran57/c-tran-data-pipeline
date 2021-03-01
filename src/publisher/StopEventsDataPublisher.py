import json
import logging
from confluent_kafka.cimpl import Producer

from src.definitions import STOP_EVENT_TOPIC
from src.kafka.KafkaHelper import KafkaHelper


class StopEventsDataPublisher:
    _logger = logging.getLogger('StopEventsDataPublisher')

    def __init__(self):
        kafka_configs = KafkaHelper.get_kafka_configs()
        self._producer = Producer(kafka_configs)

    def publish_stop_event_records(self, stop_event_records, topic=STOP_EVENT_TOPIC):
        self._logger.info("Publishing {} stop event records to {} topic ...".format(len(stop_event_records), topic))
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

        for trip_id, stop_events in stop_event_records.items():
            stop_data_record = dict()
            stop_data_record[trip_id] = stop_events

            self._producer.produce(topic, value=json.dumps(stop_data_record), on_delivery=callback)
            self._producer.poll(0)

        self._producer.flush()

        self._logger.info('Done delivering records to {} topic! A total of {} records were published'.format(topic, delivered_records))
