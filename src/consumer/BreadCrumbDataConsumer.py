import json
import logging
import pandas as pd
from confluent_kafka.cimpl import Consumer
from datetime import datetime

from src.consumer.BreadCrumb import BreadCrumb
from src.definitions import BREADCRUMB_DATA_TOPIC, STOP_EVENT_TOPIC
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
        self._trips_stop_data = dict()

    def consume_breadcrumb_records(self):

        self._logger.info("Starting breadcrumb data consumer ...")
        self._consumer.subscribe([STOP_EVENT_TOPIC, BREADCRUMB_DATA_TOPIC])

        stop_events_records_count = 0
        consumed_breadcrumb_records_count = 0
        bread_crumb_records_saved_to_db_count = 0
        breadcrumbs = list()
        last_saved_to_db = datetime.now()
        try:
            while True:

                duration_from_last_saved_to_db = datetime.now() - last_saved_to_db
                if len(breadcrumbs) >= 50_000 or (len(breadcrumbs) > 0 and duration_from_last_saved_to_db.total_seconds() > (60 * 2)):
                    self._bread_crumb_repo.bulk_save_breadcrumbs(breadcrumbs, self._trips_stop_data)
                    bread_crumb_records_saved_to_db_count += len(breadcrumbs)
                    breadcrumbs.clear()
                    last_saved_to_db = datetime.now()

                    self._logger.info(
                        'Number of breadcrumb records consumed = {}, stop event records consumed = {}, records saved to db = {}'.format(consumed_breadcrumb_records_count,
                                                                                                                                        stop_events_records_count,
                                                                                                                                        bread_crumb_records_saved_to_db_count))

                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                elif msg.error():
                    self._logger.error('error: {}'.format(msg.error()))
                else:
                    msg_topic = msg.topic()
                    message_data = msg.value().decode("utf-8")

                    if msg_topic == BREADCRUMB_DATA_TOPIC:
                        consumed_breadcrumb_records_count += 1
                        self.process_bread_crumb_record(breadcrumbs, message_data)
                    elif msg_topic == STOP_EVENT_TOPIC:
                        stop_events_records_count += 1
                        self.process_stop_event_records(message_data)

                    self._logger.debug(
                        'Number of breadcrumb records consumed = {}, stop event records consumed = {}'.format(consumed_breadcrumb_records_count, stop_events_records_count))
        finally:
            self._consumer.close()
            self._bread_crumb_repo.bulk_save_breadcrumbs(breadcrumbs, self._trips_stop_data)

    def process_bread_crumb_record(self, breadcrumbs, message_data):
        try:
            breadcrumb = BreadCrumb.parse_raw(message_data)
            breadcrumbs.append(breadcrumb)
        except Exception as ex:
            self._logger.debug('Encountered an error parsing a bread crumb.', ex)

    def process_stop_event_records(self, message_data):
        try:
            trip_stop_dict = json.loads(message_data)
            trip_id = list(trip_stop_dict.keys())[0]

            if trip_id not in self._trips_stop_data.keys():
                trip_stop_events_df = pd.read_json(list(trip_stop_dict.values())[0])
                first_row = trip_stop_events_df.iloc[0]

                self._trips_stop_data[trip_id] = {'route_id': first_row['route_number'],
                                                  'service_key': first_row['service_key'],
                                                  'direction': first_row['direction']}

        except Exception as ex:
            self._logger.debug('Encountered an error parsing a stop events record.', ex)
