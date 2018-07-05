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
import logging
import json

from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer

def get_now_utc():
    now = datetime.now(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    now_utc_micros = (now - epoch) // timedelta(microseconds=1)
    data = dict(unix_ts=int(now_utc_micros/1e3), str_ts=now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    return json.dumps(data)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    # --------------------------------------------------------------------------- #
    # Configuration file
    # --------------------------------------------------------------------------- #

    # --------------------------------------------------------------------------- #
    # Set logging object
    # --------------------------------------------------------------------------- #
    log_file = None

    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=log_file)

    # --------------------------------------------------------------------------- #
    # Starting program
    # --------------------------------------------------------------------------- #
    logger.info("Starting program")

    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('testing',
                             group_id='scrooge',
                             bootstrap_servers=['localhost:9092'])
    diffs = np.array([])
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`

        # get producer data
        producer_data = json.loads(message.value.decode('utf-8'))
        producer_utc_msec = int(producer_data['unix_ts'])

        # get consumer timestamp
        consumer_data_json = get_now_utc()
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

