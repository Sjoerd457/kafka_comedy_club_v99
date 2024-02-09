# cassandra_utils.py
import os
from dotenv import load_dotenv
from cassandra.cluster import Cluster

load_dotenv()

# Cassandra configuration
CASSANDRA_HOST: list = [os.environ['CASSANDRA_HOST']]
CASSANDRA_PORT: str = os.environ['CASSANDRA_PORT']
KEYSPACE: str = os.getenv('KEYSPACE')
TABLE_NAME: str = os.getenv('TABLE_NAME')


def setup_casandra() -> Cluster:
    """
    Create keyspace and table in Cassandra if they don't exist.
    Return session.
    """
    cluster = Cluster(CASSANDRA_HOST, port=CASSANDRA_PORT)
    session = cluster.connect()

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
    return session
