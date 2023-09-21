import argparse
import os
import sys
import random
from scipy import stats
import datetime
import time
from google.cloud import pubsub_v1

def run(TOPIC_NAME, PROJECT_ID, INTERVAL=200):
    """
    Publishes simulated sensor readings to a Pub/Sub topic.

    Args:
        TOPIC_NAME: The Cloud Pub/Sub topic to write to.
        PROJECT_ID: GCP Project ID.
        INTERVAL: Interval in milliseconds between message publications.

    Raises:
        KeyboardInterrupt: If the program is interrupted by the user.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    sensorNames = ['Pressure_1', 'Pressure_2', 'Pressure_3', 'Pressure_4', 'Pressure_5']
    sensorCenterLines = [1992, 2080, 2390, 1732, 1911]
    standardDeviation = [442, 388, 354, 403, 366]

    c = 0

    while True:
        for pos in range(0, 5):
            sensor = sensorNames[pos]
            reading = stats.truncnorm.rvs(-1, 1, loc=sensorCenterLines[pos], scale=standardDeviation[pos])
            timeStamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            message = timeStamp + ',' + sensor + ',' + str(reading)
            publisher.publish(topic_path, data=message.encode('utf-8'))
            c = c + 1
        time.sleep(INTERVAL / 1000)
        if c == 100:
            print("Published 100 Messages")
            c = 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--TOPIC_NAME",
        help="The Cloud Pub/Sub topic to write to.\n"
             '"apache-beam-simulation".',
    )
    parser.add_argument(
        "--PROJECT_ID",
        help="GCP Project ID.\n"
             '"logical-craft-384210".',
    )
    parser.add_argument(
        "--INTERVAL",
        type=int,
        default=200,
        help="Interval in milliseconds which will publish messages (default 200 ms).\n"
             '"300"',
    )
    args = parser.parse_args()
    try:
        run(args.TOPIC_NAME, args.PROJECT_ID, args.INTERVAL)
    except KeyboardInterrupt:
        print('Interrupted: Stopped Publishing messages')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
