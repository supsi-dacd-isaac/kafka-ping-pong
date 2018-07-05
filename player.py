# --------------------------------------------------------------------------- #
# Importing section
# --------------------------------------------------------------------------- #

import time
import numpy as np
import argparse
import random
import logging
import string
import json
import sys

from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --------------------------------------------------------------------------- #
# Functions section
# --------------------------------------------------------------------------- #

def get_now_data(length_stuff=10):
    now = datetime.now(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    now_utc_micros = (now - epoch) // timedelta(microseconds=1)
    str_stuff = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(length_stuff))
    data = dict(unix_ts=int(now_utc_micros/1e3), str_ts=now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], stuff=str_stuff)
    return json.dumps(data)

def send_stuff(now_data, topic):
    logger.info('Send -> %s' % now_data)
    future = producer.send(topic, str(now_data).encode())

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as e:
        # Decide what to do if produce request failed...
        logger.error('EXCEPTION: %s' % str(e))
        pass

    # Successful result returns assigned partition and offset
    logging.info('Topic=%s' % record_metadata.topic)
    logging.info('Partition=%s' % record_metadata.partition)
    logging.info('Offset=%s' % record_metadata.offset)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
if __name__ == "__main__":

    # --------------------------------------------------------------------------- #
    # Input arguments
    # --------------------------------------------------------------------------- #
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--bootstrap_server', help='bootstrap server')
    arg_parser.add_argument('--group', help='group')
    arg_parser.add_argument('--producer_topic', help='producer topic')
    arg_parser.add_argument('--consumer_topic', help='consumer topic')
    arg_parser.add_argument('--stuff_length', help='stuff length')
    arg_parser.add_argument('--delay', help='delay')
    args = arg_parser.parse_args()

    if args.bootstrap_server is not None:
        bootstrap_server = args.bootstrap_server
    else:
        bootstrap_server = 'localhost:9092'

    if args.group is not None:
        group = args.group
    else:
        group = 'goofy'

    if args.producer_topic is not None:
        producer_topic = args.producer_topic
    else:
        producer_topic = 'test_producer'

    if args.consumer_topic is not None:
        consumer_topic = args.consumer_topic
    else:
        consumer_topic = 'test_consumer'

    if args.stuff_length is not None:
        stuff_length = int(args.stuff_length)
    else:
        stuff_length = 4

    if args.delay is not None:
        delay = float(args.delay)
    else:
        delay = 5

    # --------------------------------------------------------------------------- #
    # Set logging object
    # --------------------------------------------------------------------------- #
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=None)

    # --------------------------------------------------------------------------- #
    # Starting program
    # --------------------------------------------------------------------------- #
    logger.info("Starting program")

    # create consumer and producer instances
    consumer = KafkaConsumer(consumer_topic, group_id=group, bootstrap_servers=[bootstrap_server])
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])

    logger.info('Wait for 5 seconds')
    time.sleep(5)
    logger.info('Consumer ready to get data')

    diffs = np.array([])
    for message in consumer:
        # get producer data
        producer_data = json.loads(message.value.decode('utf-8'))
        producer_utc_msec = int(producer_data['unix_ts'])

        # get consumer timestamp
        consumer_data_json = get_now_data(4)
        consumer_data = json.loads(consumer_data_json)
        consumer_utc_msec = int(consumer_data['unix_ts'])
        diff_msec = consumer_utc_msec - producer_utc_msec

        # append data
        diffs = np.append(diffs, diff_msec)

        logger.info("%s;%d;%d; key=%s value=%s; diff:%i ms" % (message.topic,
                                                               message.partition,
                                                               message.offset,
                                                               message.key,
                                                               message.value,
                                                               diff_msec))

        logger.info('min=%i; median=%i; max=%i; stdev=%.1f; N=%i' % (np.min(diffs),
                                                                     np.median(diffs),
                                                                     np.max(diffs),
                                                                     np.std(diffs),
                                                                     len(diffs)))

        time.sleep(delay)

        # response = input("Enter:")
        send_stuff(now_data=get_now_data(stuff_length), topic=producer_topic)

