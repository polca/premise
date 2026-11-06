"""Utilities for creating and maintaining loggers used across the project."""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path

import yaml

from .filesystem_constants import DATA_DIR

LOG_CONFIG = DATA_DIR / "utils" / "logging" / "logconfig.yaml"
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
        for handler_config in config.get("handlers", {}).values():
            filename = handler_config.get("filename")
            if filename:
                Path(filename).parent.mkdir(parents=True, exist_ok=True)
        logging.config.dictConfig(config)
        is_config_loaded = True

    return logging.getLogger(handler)
