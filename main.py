# main.py

import uvicorn
import multiprocessing
import logging
import os
from dotenv import load_dotenv
from waitress import serve

from consumers.example_consumer import example_consumer
from dashboard.example_dashboard import server as dash_server

# Load environment variables from .env file
load_dotenv()


def setup_logging():
    # Create logs directory if it doesn't exist
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    # Configure logging to write to a file and terminal
    log_file_path = os.path.join(logs_dir, 'main.log')
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create a stream handler to log to the terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(console_handler)

    logger = logging.getLogger(__name__)
    logger.info('Logging setup complete')
    return logger


def serve_dash():
    # Use Waitress to serve the Dash app's server
    serve(dash_server, host='0.0.0.0', port=8050)


if __name__ == "__main__":
    # Setup logging
    logger = setup_logging()

    # Create a multiprocessing.Process for the FastAPI server
    api_process = multiprocessing.Process(target=uvicorn.run, args=("src.producers.example_producer:producer_app",), kwargs={"host": "0.0.0.0", "port": 8000, "reload": True})
    api_process.start()  # Start the FastAPI server process

    # Run the Kafka consumer with the logger passed as an argument
    consumer_process = multiprocessing.Process(target=example_consumer, args=(logger,))
    consumer_process.start()  # Start the Kafka consumer process

    # Add a multiprocessing.Process for the Dash dashboard using Waitress
    dash_process = multiprocessing.Process(target=serve_dash)
    dash_process.start()
