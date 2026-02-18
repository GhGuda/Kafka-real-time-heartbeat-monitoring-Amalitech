"""
helper.py

Signal handling utilities.
"""

import logging

logger = logging.getLogger(__name__)


def shutdown_handler(signum, frame, shutdown_event):
    """
    Set shutdown event for graceful termination.
    """
    logger.info("Shutdown signal received. Triggering shutdown event.")
    shutdown_event.set()
