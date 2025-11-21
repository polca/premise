"""Utilities for creating and maintaining loggers used across the project."""

from __future__ import annotations

import logging
import logging.config
from multiprocessing import Queue
from pathlib import Path

import yaml

from .filesystem_constants import DATA_DIR

LOG_CONFIG = DATA_DIR / "utils" / "logging" / "logconfig.yaml"
DIR_LOG_REPORT = Path.cwd() / "export" / "logs"

if not DIR_LOG_REPORT.exists():
    DIR_LOG_REPORT.mkdir(parents=True, exist_ok=True)


# Assuming you have a global or passed-in queue for multiprocessing logging
log_queue = Queue()
is_config_loaded = False


def create_logger(handler: str) -> logging.Logger:
    """Create and configure a logger with the given handler name.

    :param handler: Name of the logger handler to retrieve from the logging configuration.
    :type handler: str
    :return: A configured logger instance.
    :rtype: logging.Logger
    """

    global is_config_loaded

    if not is_config_loaded:
        with open(LOG_CONFIG, encoding="utf-8") as file:
            config = yaml.safe_load(file)
        logging.config.dictConfig(config)
        is_config_loaded = True

    return logging.getLogger(handler)


def empty_log_files() -> None:
    """Delete every ``.log`` file in :data:`DIR_LOG_REPORT` if possible.

    The function removes log files created during previous runs. When the file
    cannot be removed because it is still locked, it is truncated instead so
    that subsequent log entries start fresh.

    :return: ``None``. The log directory is modified in place.
    :rtype: None
    """

    for file in DIR_LOG_REPORT.iterdir():
        if file.suffix == ".log":
            try:
                file.unlink()
            except PermissionError:
                try:
                    with open(file, "w", encoding="utf-8") as log_file:
                        log_file.write("")
                except PermissionError:
                    pass
