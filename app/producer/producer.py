"""
producer.py

Robust Kafka producer for streaming synthetic heartbeat events.
Designed for containerized environments.
"""

import json
import os
import time
import signal
import threading
import logging
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from dotenv import load_dotenv
from app.config.logging_config import setup_logging
from app.generator.generator import generate_heartbeat
from app.config.helper import shutdown_handler


# ---------------------------
# Load Environment Variables
# ---------------------------

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeat-topic")
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", "1"))
KAFKA_RETRY_BACKOFF_SECONDS = int(os.getenv("KAFKA_RETRY_BACKOFF_SECONDS", "5"))


# ---------------------------
# Logging Setup
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
# Kafka Producer Setup
# ---------------------------

def create_producer() -> KafkaProducer:
    """
    Attempt to create Kafka producer.
    Retries until Kafka becomes available.
    """
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=10,
                request_timeout_ms=10000,
                acks="all",
            )
            logger.info("Connected to Kafka successfully.")
            return producer

        except NoBrokersAvailable:
            logger.warning(
                f"Kafka not available at {KAFKA_BROKER}. "
                f"Retrying in {KAFKA_RETRY_BACKOFF_SECONDS} seconds..."
            )
            time.sleep(KAFKA_RETRY_BACKOFF_SECONDS)


# ---------------------------
# Delivery Callback
# ---------------------------

def delivery_report(record_metadata: Any) -> None:
    """
    Log successful message delivery metadata.
    """
    logger.debug(
        f"Delivered to {record_metadata.topic} "
        f"[partition {record_metadata.partition}] "
        f"offset {record_metadata.offset}"
    )


# ---------------------------
# Main Producer Loop
# ---------------------------

def run_producer() -> None:
    """
    Main producer execution loop.
    """
    logger.info("Starting Kafka producer container...")

    producer = create_producer()

    while not shutdown_event.is_set():
        try:
            event = generate_heartbeat()

            future = producer.send(KAFKA_TOPIC, value=event)

            # Non-blocking delivery check
            future.add_callback(delivery_report)

            producer.flush(timeout=5)

            time.sleep(SEND_INTERVAL_SECONDS)

        except KafkaError as e:
            logger.error(f"Kafka error during send: {e}")

        except Exception as e:
            logger.exception(f"Unexpected error: {e}")

    logger.info("Producer shutting down...")
    producer.flush()
    producer.close()
    logger.info("Producer stopped cleanly.")


if __name__ == "__main__":
    run_producer()
