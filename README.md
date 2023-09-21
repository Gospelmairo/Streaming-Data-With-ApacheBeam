# Streaming_data_with_Apache_beam

# Sensor Data Processing Pipeline

This repository contains the code for a sensor data processing pipeline implemented using Apache Beam. The pipeline processes sensor readings and performs interpolation and filtering before storing the data in BigQuery.


### Part 1: Simulating Sensor Data

To simulate sensor data and publish it to a Pub/Sub topic, run the `pubsub_publisher.py` script. This script generates random sensor readings for five sensors and publishes them to the specified Pub/Sub topic at regular intervals.

#### Usage
```
python pubsub_publisher.py --TOPIC_NAME <pubsub-topic> --PROJECT_ID <project-id> [--INTERVAL <interval>]
```

- `TOPIC_NAME`: The Cloud Pub/Sub topic to publish the sensor data to.
- `PROJECT_ID`: The ID of the Google Cloud Platform (GCP) project.
- `INTERVAL` (optional): Interval in milliseconds between message publications (default is 200 ms).

### Part 2: Processing Sensor Data

To process the sensor data using Apache Beam and store it in BigQuery, run the `apacheBeampipeline.py` script. This script reads the sensor data from the specified Pub/Sub subscription, performs aggregation, interpolation, and filtering, and writes the processed data to a BigQuery table.

#### Usage
```
python apacheBeampipeline.py --SUBSCRIPTION_NAME <pubsub-subscription> --BQ_TABLE <bigquery-table> [--AGGREGATION_INTERVAL <interval>]
```

- `SUBSCRIPTION_NAME`: The Cloud Pub/Sub subscription to read the sensor data from.
- `BQ_TABLE`: The path of the BigQuery table to store the processed data in (format: `<project-id>:<dataset>.<table>`).
- `AGGREGATION_INTERVAL` (optional): Number of seconds to aggregate the sensor data (default is 1 second).

## Prerequisites

- Python 3.x
- Apache Beam
- pandas
- google-cloud-pubsub
- google-cloud-bigquery

## Setup and Installation

1. Clone this repository:
```
git clone https://github.com/krissemmy/Streaming_data_with_Apache_beam.git

```

2. Create a Virtual environment
```
python -m virtualenv my-venv   
```
3. Activate the virtual environment
```
source my-venv/bin/activate
```
4. Install the required dependencies:
```
pip install -r requirements.txt
```

5. Set up your Google Cloud credentials by following the instructions in the [Authentication](https://cloud.google.com/docs/authentication/getting-started) documentation.

## License

This project is licensed under the [MIT License](LICENSE).
