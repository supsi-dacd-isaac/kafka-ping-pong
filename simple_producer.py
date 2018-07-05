# --------------------------------------------------------------------------- #
# Importing section
# --------------------------------------------------------------------------- #

import ftplib
import os
import calendar
import sys
import time
import random
import logging
import string
import datetime
import json

from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

def get_now_data(length_stuff=10):
    now = datetime.now(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    now_utc_micros = (now - epoch) // timedelta(microseconds=1)
    str_stuff = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(length_stuff))
    data = dict(unix_ts=int(now_utc_micros/1e3), str_ts=now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], stuff=str_stuff)
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

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Asynchronous by default
    for i in range(0, 1):
        now_data = get_now_data(4)
        logger.info('Send -> %s' % now_data)
        future = producer.send('testing', str(now_data).encode())

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

        # wait for 10 ms
        time.sleep(0.01)
