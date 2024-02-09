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
import json
import logging
import os
from dotenv import load_dotenv
from cassandra.query import SimpleStatement
from kafka import KafkaConsumer
from utils.configure_logging import configure_logging
from utils.cassandra_utils import setup_casandra


# Load environment variables from .env file
load_dotenv()

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_TOPIC: str = os.environ['TOPICS_TTF_DATA_NAME']
KAFKA_BOOTSTRAP_SERVERS: str = os.environ['BOOTSTRAP_SERVERS']

# Cassandra configuration
KEYSPACE: str = os.getenv('KEYSPACE')
TABLE_NAME: str = os.getenv('TABLE_NAME')


def deserialize_value(value: bytes) -> dict:
    """Deserialize JSON value from Kafka message."""
    return json.loads(value.decode('utf-8'))


def example_consumer(logger: 'logging.Logger') -> None:
    """Consume messages from Kafka and insert into Cassandra."""
    # Connect to Cassandra
    session = setup_casandra()

    # Start Kafka consumer
    consumer: 'KafkaConsumer' = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=deserialize_value,
        auto_offset_reset='latest'
    )

    # Consume messages and insert into Cassandra
    for message in consumer:
        data: dict = message.value
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


if __name__ == '__main__':
    # Call example_consumer with the logger
    example_consumer(logger)
