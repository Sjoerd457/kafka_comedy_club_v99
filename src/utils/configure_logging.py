# logging_config.py
import logging
import os


def configure_logging():
    # Create logs directory if it doesn't exist
    logs_dir = '../../logs'
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
