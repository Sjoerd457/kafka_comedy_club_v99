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
import sys
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
KAFKA_TOPIC: str = os.environ['TOPICS_TTF_DATA_NAME']
KAFKA_BOOTSTRAP_SERVERS: str = os.environ['BOOTSTRAP_SERVERS']

# Cassandra configuration
CASSANDRA_HOST: list = [os.environ['CASSANDRA_HOST']]
CASSANDRA_PORT: str = os.environ['CASSANDRA_PORT']
KEYSPACE: str = os.getenv('KEYSPACE')
TABLE_NAME: str = os.getenv('TABLE_NAME')

print(f"Cassandra hosts: {CASSANDRA_HOST}, Keyspace:{KEYSPACE}, Table name:{TABLE_NAME}")


def deserialize_value(value: bytes) -> dict:
    """Deserialize JSON value from Kafka message."""
    return json.loads(value.decode('utf-8'))


def create_keyspace_and_table(session: 'Cluster') -> None:
    """Create keyspace and table in Cassandra if they don't exist."""
    # Create keyspace
    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
    """)

    # Use keyspace
    session.set_keyspace(KEYSPACE)

    # Create table
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        timestamp text,
        ticker text,
        open double,
        high double,
        low double,
        close double,
        PRIMARY KEY (ticker, timestamp)
    )
    """)


def example_consumer(logger: 'logging.Logger') -> None:
    """Consume messages from Kafka and insert into Cassandra."""
    # Connect to Cassandra
    cluster = Cluster(CASSANDRA_HOST, port=CASSANDRA_PORT)
    session = cluster.connect()
    create_keyspace_and_table(session)

    # Configure logging to use the provided logger
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler('logs/app.log')])
    logging.getLogger(__name__).info('example_consumer started')  # Log a message indicating the start of the consumer

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
    # Set up logging to only output to console when running this script directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]  # Log to stdout
    )

    # Get the logger
    logger = logging.getLogger(__name__)
    logger.info('example_consumer started')  # Log a message indicating the start of the consumer

    # Call example_consumer with the logger
    example_consumer(logger)
