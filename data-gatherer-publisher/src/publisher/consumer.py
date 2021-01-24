#!/home/ad35/env/bin/python

from confluent_kafka import Consumer
import json
import ccloud_lib
from publisher.kafka.KafkaHelper import KafkaHelper

if __name__ == '__main__':

    args = ccloud_lib.parse_args()
    kafka_configs = KafkaHelper.get_kafka_configs()
    topic = args.topic

    consumer = Consumer(kafka_configs)

    consumer.subscribe([topic])

    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                count = data['count']
                total_count += count
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(record_key, record_value, total_count))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
