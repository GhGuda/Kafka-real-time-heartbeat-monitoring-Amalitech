"""
logging_config.py

Centralized logging configuration for the application.
Ensures consistent logging format and file persistence.
"""

import logging
import os
from logging.handlers import RotatingFileHandler


LOG_DIR = "app/logs"
LOG_FILE = "heartbeat.log"
LOG_LEVEL = logging.INFO


def setup_logging() -> None:
    """
    Configure application-wide logging.

    - Logs written to file
    - Logs printed to console
    - Rotating file handler to prevent large log files
    """

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    log_path = os.path.join(LOG_DIR, LOG_FILE)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # File handler with rotation (5MB per file, keep 3 backups)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
    )
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)

    # Avoid duplicate handlers
    if not root_logger.handlers:
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)



