import argparse
import datetime
import json
import logging
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import pandas as pd
from apache_beam.io import fileio

class interpolateSensors(beam.DoFn):
    """
    Custom DoFn class to interpolate sensor values.
    """
    def process(self, sensorValues):
        """
        Interpolates sensor values by taking the mean of each sensor's values.

        Args:
            sensorValues: Tuple containing the timestamp and sensor values.

        Returns:
            A list containing a JSON string with interpolated sensor values.
        """
        (timestamp, values) = sensorValues
        df = pd.DataFrame(values)
        df.columns = ["Sensor", "Value"]
        df["Value"] = pd.to_numeric(df["Value"])
        json_string = json.loads(df.groupby(["Sensor"]).mean().T.iloc[0].to_json())
        json_string["timestamp"] = timestamp
        return [json_string]

def isMissing(jsonData):
    """
    Checks if any sensor values are missing in the JSON data.

    Args:
        jsonData: JSON data containing sensor values.

    Returns:
        True if any sensor values are missing, False otherwise.
    """
    return len(jsonData.values()) == 6

def roundTime(dt=None, roundTo=1):
    """
    Rounds the given datetime to the specified interval.

    Args:
        dt: Datetime object to be rounded. If None, current datetime is used.
        roundTo: Rounding interval in seconds.

    Returns:
        Rounded datetime as a string.
    """
    if dt is None:
        dt = datetime.datetime.now()
    dt = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')  # Convert string to datetime
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds + roundTo/2) // roundTo * roundTo
    return str(dt + datetime.timedelta(0, rounding-seconds, -dt.microsecond))

def run(subscription_name, output_table, interval=1.0, pipeline_args=None):
    """
    Runs the pipeline to read data from Pub/Sub, interpolate sensor values, and write to BigQuery.

    Args:
        subscription_name: Pub/Sub subscription name.
        output_table: BigQuery table path.
        interval: Number of seconds to aggregate.
        pipeline_args: Additional pipeline options.
    """
    schema = 'Timestamp:TIMESTAMP, PRESSURE_1:FLOAT, PRESSURE_2:FLOAT, PRESSURE_3:FLOAT, PRESSURE_4:FLOAT, PRESSURE_5:FLOAT'
    with beam.Pipeline(options=PipelineOptions(pipeline_args, streaming=True, save_main_session=True)) as p:
        data = (p
            | 'ReadData from pubsub subscription' >> ReadFromPubSub(subscription=subscription_name)
            | "Decode hit use Map and lambda" >> beam.Map(lambda x: x.decode())
            | "Convert to list" >> beam.Map(lambda x: x.split(','))
            | "to tuple use Map and lambda " >> beam.Map(lambda x: (roundTime(x[0].split('.')[0]), (x[1], x[2])))
        )

        bq = (
            data
            | "Applying Fixed Window of 15 secs" >> beam.WindowInto(window.FixedWindows(size=15))
            | "Groupby" >> beam.GroupByKey()
            | "Interpolate" >> beam.ParDo(interpolateSensors())
            | "Filter Missing" >> beam.Filter(isMissing)
            | "Write to Big Query - append values to bigquery table" >> beam.io.WriteToBigQuery(output_table, schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__": 
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--SUBSCRIPTION_NAME",
        help="The Cloud Pub/Sub subscription to read from.\n"
        '"projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>".',
    )
    parser.add_argument(
        "--BQ_TABLE",
        help = "Big Query Table Path.\n"
        '"<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>"')
    parser.add_argument(
        "--AGGREGATION_INTERVAL",
        type = int,
        default = 1,
        help="Number of seconds to aggregate.\n",

    )
    args, pipeline_args = parser.parse_known_args()
    run(
        args.SUBSCRIPTION_NAME,
        args.BQ_TABLE,
        args.AGGREGATION_INTERVAL,
        pipeline_args
    )
