# main.py

import logging
import multiprocessing
import uvicorn

from dotenv import load_dotenv
from waitress import serve

from utils.configure_logging import configure_logging
from utils.cassandra_utils import setup_casandra
from connectors.manage_cassandra_connector import start_cassandra_connector, check_cassandra_connector_status
from consumers.example_consumer import example_consumer
from consumers.example_consumer_avro import example_consumer_avro
from dashboard.example_dashboard import server as dash_server


load_dotenv()

configure_logging()
logger = logging.getLogger(__name__)

USE_AVRO = True
USE_CONNECTOR = True


def serve_dash():
    # Use Waitress to serve the Dash app's server
    serve(dash_server, host='0.0.0.0', port=8050)


if __name__ == "__main__":

    # Create a multiprocessing.Process for the FastAPI server
    if USE_AVRO:
        api_process = multiprocessing.Process(target=uvicorn.run, args=("src.producers.example_producer_avro:producer_app_avro",), kwargs={"host": "0.0.0.0", "port": 8000, "reload": True})
    else:
        api_process = multiprocessing.Process(target=uvicorn.run, args=("src.producers.example_producer:producer_app",), kwargs={"host": "0.0.0.0", "port": 8000, "reload": True})
    api_process.start()  # Start the FastAPI server process

    if USE_CONNECTOR:
        setup_casandra()
        start_cassandra_connector()
        check_cassandra_connector_status()
    else:
        if USE_AVRO:
            consumer_process = multiprocessing.Process(target=example_consumer_avro, args=(logger,))
        else:
            consumer_process = multiprocessing.Process(target=example_consumer, args=(logger,))
        consumer_process.start()  # Start the Kafka consumer process

    # Add a multiprocessing.Process for the Dash dashboard using Waitress
    dash_process = multiprocessing.Process(target=serve_dash)
    dash_process.start()
