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
import logging
import asyncio

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic

from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

import producers.schemas_avro as schemas_avro
from producers.fake_financial_data import FakeFinancialData
from utils.configure_logging import configure_logging

load_dotenv(verbose=True)

configure_logging()
logger = logging.getLogger(__name__)

# Configuration variables
DEBUG = True
UPDATE_SECONDS: float = float(os.environ['UPDATE_SECONDS'])  # Interval for generating and sending new data
KAFKA_TOPIC: str = os.environ['TOPICS_TTF_DATA_AVRO_NAME']
KAFKA_BOOTSTRAP_SERVERS: str = os.environ['BOOTSTRAP_SERVERS']
KAFKA_PARTITIONS: int = int(os.environ['TOPICS_TTF_DATA_AVRO_PARTITIONS'])
KAFKA_REPLICAS: int = int(os.environ['TOPICS_TTF_DATA_AVRO_REPLICAS'])
KAFKA_SCHEMA_REGISTRY_URL: str = os.environ['SCHEMA_REGISTRY_URL']
# Initialize FastAPI app
producer_app_avro = FastAPI()


@producer_app_avro.on_event('startup')
async def startup_event():
    client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    topic = NewTopic(KAFKA_TOPIC,
                     num_partitions=KAFKA_PARTITIONS,
                     replication_factor=KAFKA_REPLICAS)
    try:
        futures = client.create_topics([topic])
        for topic_name, future in futures.items():
            future.result()
            logger.info(f"Create topic {topic_name}")
    except Exception as e:
        logger.warning(e)


def make_producer() -> SerializingProducer:

    schema_reg_client = SchemaRegistryClient({'url': KAFKA_SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(schema_registry_client=schema_reg_client,
                                     schema_str=schemas_avro.ttf_data_value_v1,
                                     to_dict=lambda obj, ctx: obj.dict())

    return SerializingProducer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'linger.ms': 300,
        'enable.idempotence': 'true',
        'max.in.flight.requests.per.connection': 1,
        'acks': 'all',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'partitioner': 'murmur2_random'
    })


class FakeFinancialDataCallback:

    def __init__(self, ttf_data: FakeFinancialData):
        self.ttf_data = ttf_data

    def __call__(self, err, msg):
        if err:
            logger.error(f"Failed to produce {self.ttf_data}", exc_info=err)
        else:
            # Access Pydantic model data using .dict() method for logging
            ticker = self.ttf_data.ticker
            timestamp = self.ttf_data.timestamp
            logger.info(f"""
                            Successfully produced {ticker} at timestamp {timestamp}
                            to partition {msg.partition()}
                            at offset {msg.offset()}
                        """)


async def get_ttf_data():
    """Generates and sends TTF data to Kafka at regular intervals."""
    producer = make_producer()
    while True:
        if DEBUG:
            data = FakeFinancialData.generate()
        producer.produce(topic=KAFKA_TOPIC,
                         key=data.ticker.lower().replace(r's+', '-'),
                         # Pass Pydantic model instance directly
                         #
                         value=data,
                         on_delivery=FakeFinancialDataCallback(data))
        producer.flush()
        await asyncio.sleep(UPDATE_SECONDS)


@producer_app_avro.on_event("startup")
async def start_generator():
    """Starts the data generation task."""
    asyncio.create_task(get_ttf_data())

if __name__ == "__main__":
    uvicorn.run("example_producer_avro:producer_app_avro", host="0.0.0.0", port=8000, reload=True)
