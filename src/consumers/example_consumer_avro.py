"""
Kafka to Cassandra Consumer

Description:
This script consumes messages from a Kafka topic containing ICE data (Intercontinental Exchange)
and inserts the data into a Cassandra database. It connects to Kafka using the provided
bootstrap servers and consumes messages from the specified topic. The consumed data is
then inserted into the Cassandra database, where it is stored in a keyspace named 'realtime_data'
and a table named 'market_data'. The script assumes that the necessary environment variables
for Kafka and Cassandra configurations are loaded using dotenv.

Usage:
Ensure that the required environment variables are set:
- TOPICS_ICE_DATA_NAME: Kafka topic name containing ICE data
- BOOTSTRAP_SERVERS: Kafka bootstrap servers
- CASSANDRA_HOSTS: Cassandra hosts

Then run the script using Python.
"""

# Import required libraries
import logging
import os
from dotenv import load_dotenv
from cassandra.query import SimpleStatement
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

import producers.schemas_avro as schemas_avro
from utils.configure_logging import configure_logging
from utils.cassandra_utils import setup_casandra
# Load environment variables from .env file
load_dotenv()

configure_logging()
logger = logging.getLogger(__name__)

# Kafka configuration
UPDATE_SECONDS: float = float(os.environ['UPDATE_SECONDS'])  # Interval for generating and sending new data
KAFKA_TOPIC: str = os.environ['TOPICS_TTF_DATA_AVRO_NAME']
KAFKA_CONSUMER_GROUP = os.environ['CONSUMER_GROUP_AVRO']
KAFKA_BOOTSTRAP_SERVERS: str = os.environ['BOOTSTRAP_SERVERS']
KAFKA_PARTITIONS: int = int(os.environ['TOPICS_TTF_DATA_AVRO_PARTITIONS'])
KAFKA_REPLICAS: int = int(os.environ['TOPICS_TTF_DATA_AVRO_REPLICAS'])
KAFKA_SCHEMA_REGISTRY_URL: str = os.environ['SCHEMA_REGISTRY_URL']

# Cassandra configuration
KEYSPACE: str = os.getenv('KEYSPACE')
TABLE_NAME: str = os.getenv('TABLE_NAME')


def make_consumer() -> DeserializingConsumer:
    # create a SchemaRegistryClient
    schema_reg_client = SchemaRegistryClient({'url': KAFKA_SCHEMA_REGISTRY_URL})

    # create a AvroDeserializer
    avro_deserializer = AvroDeserializer(schema_registry_client=schema_reg_client,
                                         schema_str=schemas_avro.ttf_data_value_v1)

    # create and return DeserializingConsumer
    return DeserializingConsumer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                                  'key.deserializer': StringDeserializer('utf_8'),
                                  'value.deserializer': avro_deserializer,
                                  'group.id': KAFKA_CONSUMER_GROUP,
                                  'enable.auto.commit': 'false'})


def example_consumer_avro(logger: 'logging.Logger') -> None:
    """Consume messages from Kafka and insert into Cassandra."""
    # Connect to Cassandra
    session = setup_casandra()

    # Create consumer
    consumer = make_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    try:
        while True:
            # Poll for a message with a timeout (e.g., 1 second)
            message = consumer.poll(1.0)
            if message is None:
                continue  # No message received within the timeout period
            if message.error():
                logger.error(f"Consumer error: {message.error()}")
                continue

            data: dict = message.value()
            query: 'SimpleStatement' = SimpleStatement(f"""
                    INSERT INTO {KEYSPACE}.{TABLE_NAME} (timestamp, ticker, open, high, low, close)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """)
            session.execute(query, (
                data['timestamp'],
                data['ticker'],
                float(data['open']),
                float(data['high']),
                float(data['low']),
                float(data['close'])
            ))
            logger.info(f"Inserted data into Cassandra: {data}")  # Log the inserted data
    finally:
        consumer.close()


if __name__ == '__main__':

    # Call example_consumer with the logger
    example_consumer_avro(logger)
