"""
manage_cassandra_connector.py

Description:
This script manages the lifecycle of a Cassandra sink connector in a Kafka ecosystem. It provides functions to start, check the status, and delete the Cassandra sink connector via the Kafka Connect REST API. It's designed to facilitate easy connector management for development and maintenance purposes.

Usage:
- To check the connector's status: python manage_cassandra_connector.py --status
- To start the connector: python manage_cassandra_connector.py --start
- To delete the connector: python manage_cassandra_connector.py --delete

Dependencies:
- requests: For making HTTP requests to the Kafka Connect REST API.
- json: For parsing and constructing JSON payloads.
- logging: For logging the actions and responses.
- pathlib: For file and path manipulations in a platform-independent way.
"""

import json
import logging
import requests
from pathlib import Path
from typing import Dict, Any
from utils.configure_logging import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# API configuration
ROOT: str = "http://localhost:8083/connectors"
CONNECTOR_NAME: str = "cassandra-sink-connector"
SCRIPT_DIR: Path = Path(__file__).resolve().parent
JSON_FILE: Path = SCRIPT_DIR / "cassandra-sink-connector.json"


def read_json_file(file_path: Path) -> Dict[str, Any]:
    """Reads a JSON file and returns its contents as a dictionary."""
    with file_path.open("r") as file:
        return json.load(file)


def start_cassandra_connector() -> None:
    """Starts the Cassandra sink connector by sending a configuration to the Kafka Connect REST API."""
    try:
        url: str = f"{ROOT}/{CONNECTOR_NAME}/config"
        config_data_dict: Dict[str, Any] = read_json_file(JSON_FILE)
        response: requests.Response = requests.put(url, json=config_data_dict)
        logger.info(f"Response status code: {response.status_code}, Response JSON: {response.json()}")
    except Exception as e:
        logger.error(f"The following Exception occurred: {e}")


def check_cassandra_connector_status() -> None:
    """Checks the status of the Cassandra sink connector."""
    try:
        url: str = f"{ROOT}/{CONNECTOR_NAME}/status"
        response: requests.Response = requests.get(url)
        logger.info(f"Response status code: {response.status_code}, Response JSON: {response.json()}")
    except Exception as e:
        logger.error(f"The following Exception occurred: {e}")


def delete_cassandra_connector() -> None:
    """Deletes the Cassandra sink connector."""
    try:
        url: str = f"{ROOT}/{CONNECTOR_NAME}"
        response: requests.Response = requests.delete(url)
        if response.status_code == 204:
            logger.info(f"Connector '{CONNECTOR_NAME}' deleted successfully.")
        else:
            logger.info(f"Response status code: {response.status_code}, Response JSON: {response.json()}")
    except requests.exceptions.RequestException as e:
        logger.error(f"The following RequestException occurred: {e}")
    except json.JSONDecodeError:
        logger.error("JSONDecodeError: No JSON content to parse from response.")


if __name__ == "__main__":
    # Example usage: This part of the script could be expanded to handle command line arguments for different actions.
    check_cassandra_connector_status()
    start_cassandra_connector()
    delete_cassandra_connector()
    check_cassandra_connector_status()
