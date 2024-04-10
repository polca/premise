"""
Module to create a logger with the given handler.
"""

import logging.config
from pathlib import Path
from logging.handlers import QueueHandler
from multiprocessing import Queue

import yaml

from .filesystem_constants import DATA_DIR

LOG_CONFIG = DATA_DIR / "utils" / "logging" / "logconfig.yaml"
DIR_LOG_REPORT = Path.cwd() / "export" / "logs"

# if DIR_LOG_REPORT folder does not exist
# we create it
if not Path(DIR_LOG_REPORT).exists():
    Path(DIR_LOG_REPORT).mkdir(parents=True, exist_ok=True)

# Assuming you have a global or passed-in queue for multiprocessing logging
log_queue = Queue()
is_config_loaded = False  # Flag to track if the logging config has been loaded

def get_loggers_list_from_config():
    with open(LOG_CONFIG, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
        return list(config['loggers'].keys())

def load_logging_config():
    global is_config_loaded
    # Check if the logging configuration has already been loaded
    if not is_config_loaded:
        with open(LOG_CONFIG, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
            logging.config.dictConfig(config)
        is_config_loaded = True  # Set the flag to True after loading the config


def create_logger(logger_name):
    """
    Creates and returns a logger configured to use a QueueHandler for multiprocessing.

    Args:
    - logger_name: The name of the logger to be created, matching one defined in the YAML config.

    Returns:
    A logger instance configured with a QueueHandler.
    """
    # Ensure the logging configuration is loaded.
    load_logging_config()

    # Create or get the logger by name.
    logger = logging.getLogger(logger_name)

    # Check if the logger already has a QueueHandler attached.
    # This prevents adding multiple QueueHandlers if this function is called multiple times.
    if not any(isinstance(handler, QueueHandler) for handler in logger.handlers):
        # Attach a QueueHandler that forwards logs to the shared log_queue.
        q_handler = QueueHandler(log_queue)
        logger.addHandler(q_handler)

    return logger


def empty_log_files():
    """
    Empties all log files specified in the logging configuration.
    """
    with open(LOG_CONFIG, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
        handlers = config.get('handlers', {})

        for handler_name, handler_config in handlers.items():
            if handler_config.get('class') == 'logging.FileHandler':
                filename = handler_config.get('filename')
                if filename:
                    # Open the file in write mode with an empty string to clear it
                    with open(filename, 'w', encoding='utf-8') as log_file:
                        log_file.write('')