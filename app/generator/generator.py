"""
data_generator.py

Synthetic real-time heart rate data generator
with realistic distribution and anomaly simulation.
"""

import json
import random
import signal
import threading
import time
from datetime import datetime
from typing import Dict, List
import logging
from datetime import datetime, timezone

from app.config.helper import shutdown_handler
from app.config.logging_config import setup_logging


# ---------------------------
# Configuration
# ---------------------------

CUSTOMER_IDS: List[str] = [f"CUST_{i:03d}" for i in range(1, 11)]

# Database constraint envelope
HEART_RATE_DB_MIN: int = 30
HEART_RATE_DB_MAX: int = 220

# Normal physiological range
HEART_RATE_NORMAL_MIN: int = 60
HEART_RATE_NORMAL_MAX: int = 100

ANOMALY_PROBABILITY: float = 0.05

INTERVAL_SECONDS: float = 1.0
MAX_RUNTIME_SECONDS: int = 60


# ---------------------------
# Initialize Logging
# ---------------------------

setup_logging()
logger = logging.getLogger(__name__)


# ---------------------------
# Graceful Shutdown
# ---------------------------

shutdown_event = threading.Event()

signal.signal(signal.SIGINT, lambda s, f: shutdown_handler(s, f, shutdown_event))
signal.signal(signal.SIGTERM, lambda s, f: shutdown_handler(s, f, shutdown_event))


# ---------------------------
# Heart Rate Logic
# ---------------------------

def generate_heart_rate() -> int:
    """
    Generate a heart rate value.

    Returns:
        int: Simulated heart rate within DB constraint range (30–220).
             Mostly normal values with occasional anomalies.
    """
    if random.random() < ANOMALY_PROBABILITY:
        # Generate anomaly within full DB envelope
        heart_rate = random.randint(HEART_RATE_DB_MIN, HEART_RATE_DB_MAX)
        logger.debug(f"Anomaly generated: {heart_rate}")
        return heart_rate

    # Normal physiological range
    return random.randint(
        HEART_RATE_NORMAL_MIN,
        HEART_RATE_NORMAL_MAX,
    )


def generate_heartbeat() -> Dict[str, object]:
    """
    Generate a synthetic heartbeat event.

    Returns:
        Dict[str, object]: Event payload containing
            - customer_id
            - timestamp (UTC ISO format, timezone-aware)
            - heart_rate
    """
    return {
        "customer_id": random.choice(CUSTOMER_IDS),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate": generate_heart_rate(),
    }


def run_generator() -> None:
    """
    Run heartbeat generation loop with timeout control
    and graceful shutdown support.
    """

    start_time = time.time()
    logger.info("Starting synthetic heartbeat generator...")

    while not shutdown_event.is_set():

        # Timeout condition
        if time.time() - start_time > MAX_RUNTIME_SECONDS:
            logger.info("Max runtime reached. Stopping generator...")
            break

        event = generate_heartbeat()
        logger.info(json.dumps(event))

        time.sleep(INTERVAL_SECONDS)

    logger.info("Generator stopped cleanly.")



if __name__ == "__main__":
    run_generator()
