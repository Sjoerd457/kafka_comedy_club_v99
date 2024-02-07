"""
producer.py
Description: This file creates a FastAPI application responsible for producing fake TTF data and sending it to a Kafka topic.
It simulates real-time data generation for testing and development purposes. The data includes time series information
such as open, high, low, and close prices for a fictional ticker. The application uses environment variables for configuration
and supports dynamic reloading for development. It also includes a mechanism to create the Kafka topic if it does not exist.
Use commands:
- Start: uvicorn example_producer:producer_app --reload
- If port taken: uvicorn example_producer:producer_app --reload --port 8001
- Or kill process that uses port: isof -i:8000; kill -9 <ps_id>
- Check output: docker exec -it cli-tools kafka-console-consumer --bootstrap-server broker0:29092 --topic ttf_data.basic.python
"""


import os
import json
import random
from datetime import datetime
import asyncio

from dotenv import load_dotenv
from fastapi import FastAPI
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv(verbose=True)

# Configuration variables
DEBUG = True
UPDATE_SECONDS = 5  # Interval for generating and sending new data

# Initialize FastAPI app
producer_app = FastAPI()


class FakeTTFData:
    """Generates fake TTF (Title Transfer Facility) data for testing."""

    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.ticker = "%TFM 1!-ICN"
        self.open = round(random.uniform(100, 200), 2)
        self.high = round(random.uniform(self.open, self.open + 10), 2)
        self.low = round(random.uniform(self.open - 10, self.open), 2)
        self.close = round(random.uniform(self.low, self.high), 2)

    def json(self):
        """Serializes the data into JSON format."""
        return json.dumps({
            "timestamp": self.timestamp,
            "ticker": self.ticker,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
        })


@producer_app.on_event('startup')
async def startup_event():
    """On startup, create Kafka topic if it doesn't exist."""
    client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])
    try:
        topic = NewTopic(name=os.environ['TOPICS_TTF_DATA_NAME'],
                         num_partitions=int(os.environ['TOPICS_TTF_DATA_PARTITIONS']),
                         replication_factor=int(os.environ['TOPICS_TTF_DATA_REPLICAS']))
        client.create_topics([topic])
    except TopicAlreadyExistsError as e:
        print(e)
    finally:
        client.close()


def make_producer():
    """Creates and returns a KafkaProducer instance."""
    return KafkaProducer(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])


async def get_ttf_data():
    """Generates and sends TTF data to Kafka at regular intervals."""
    producer = make_producer()
    while True:
        if DEBUG:
            data = FakeTTFData()
        producer.send(
            os.environ['TOPICS_TTF_DATA_NAME'],
            key=data.ticker.lower().replace(r's+', '-').encode('utf-8'),
            value=data.json().encode('utf-8')
        )
        producer.flush()
        await asyncio.sleep(UPDATE_SECONDS)


@producer_app.on_event("startup")
async def start_generator():
    """Starts the data generation task."""
    asyncio.create_task(get_ttf_data())
