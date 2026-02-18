"""
consumer.py

Kafka consumer that validates heartbeat events
and writes them to PostgreSQL.
"""

import os
import json
import time
import signal
import logging
import threading
from typing import Dict

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import psycopg

from dotenv import load_dotenv
from app.config.logging_config import setup_logging
from app.config.helper import shutdown_handler


# ---------------------------
# Environment
# ---------------------------

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeat-topic")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "heartbeat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "heartbeat_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "heartbeat_pass")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")


# ---------------------------
# Logging
# ---------------------------

setup_logging()
logger = logging.getLogger(__name__)


# ---------------------------
# Shutdown Handling
# ---------------------------

shutdown_event = threading.Event()
signal.signal(signal.SIGINT, lambda s, f: shutdown_handler(s, f, shutdown_event))
signal.signal(signal.SIGTERM, lambda s, f: shutdown_handler(s, f, shutdown_event))


# ---------------------------
# Kafka Consumer
# ---------------------------

def create_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="heartbeat-consumer-group",
            )
            logger.info("Connected to Kafka.")
            return consumer

        except NoBrokersAvailable:
            logger.warning("Kafka not ready. Retrying in 5 seconds...")
            time.sleep(5)


# ---------------------------
# PostgreSQL Connection
# ---------------------------

def create_db_connection():
    while True:
        try:
            conn = psycopg.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                port=POSTGRES_PORT,
            )
            conn.autocommit = False
            logger.info("Connected to PostgreSQL.")
            return conn

        except psycopg2.OperationalError:
            logger.warning("PostgreSQL not ready. Retrying in 5 seconds...")
            time.sleep(5)


# ---------------------------
# Validation
# ---------------------------

def validate_event(event: Dict) -> bool:
    """
    Validate heartbeat event payload.
    """
    try:
        if not all(k in event for k in ("customer_id", "timestamp", "heart_rate")):
            return False

        heart_rate = int(event["heart_rate"])
        if not (30 <= heart_rate <= 220):
            return False

        return True

    except Exception:
        return False


# ---------------------------
# Insert Logic
# ---------------------------

def insert_event(cursor, event: Dict):
    query = """
        INSERT INTO customer_heartbeats
        (customer_id, recorded_at, heart_rate)
        VALUES (%s, %s, %s)
    """

    cursor.execute(
        query,
        (
            event["customer_id"],
            event["timestamp"],
            event["heart_rate"],
        ),
    )


# ---------------------------
# Main Loop
# ---------------------------

def run_consumer():
    logger.info("Starting consumer service...")

    consumer = create_consumer()
    conn = create_db_connection()
    cursor = conn.cursor()

    while not shutdown_event.is_set():

        for message in consumer.poll(timeout_ms=1000).values():

            for record in message:
                event = record.value

                if validate_event(event):
                    try:
                        insert_event(cursor, event)
                        conn.commit()
                        consumer.commit()
                        logger.debug("Event inserted successfully.")

                    except Exception as e:
                        conn.rollback()
                        logger.error(f"DB insert failed: {e}")

                else:
                    logger.warning(f"Invalid event skipped: {event}")

    logger.info("Shutting down consumer...")
    consumer.close()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    run_consumer()
