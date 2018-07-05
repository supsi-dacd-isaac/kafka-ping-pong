# --------------------------------------------------------------------------- #
# Importing section
# --------------------------------------------------------------------------- #

import ftplib
import os
import calendar
import sys
import time
import shutil
import argparse
import configparser
import numpy as np
import random
import logging
import string
import json

from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError

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
    arg_parser.add_argument('--topic', help='topic')
    args = arg_parser.parse_args()

    bootstrap_server = args.bootstrap_server
    topic = args.topic

    # --------------------------------------------------------------------------- #
    # Set logging object
    # --------------------------------------------------------------------------- #
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=None)

    # --------------------------------------------------------------------------- #
    # Starting program
    # --------------------------------------------------------------------------- #
    logger.info('Start to play!')

    # create consumer and producer instances
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    send_stuff(now_data=get_now_data(4), topic=topic)


