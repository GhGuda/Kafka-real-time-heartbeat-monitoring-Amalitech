"""
Kafka consumer that validates heartbeat events
and writes them to PostgreSQL.
"""

import os
import json
import time
import signal
import logging
import threading

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import start_http_server, Counter, Gauge


import psycopg
from dotenv import load_dotenv
from app.config.logging_config import setup_logging
from app.config.helper import shutdown_handler

from app.core.validation import validate_event


# ---------------------------
# Environment
# ---------------------------

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeat-topic")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "heartbeat-dead-letter")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "heartbeat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "heartbeat_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "heartbeat_pass")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

METRICS_LOG_INTERVAL = 10  # seconds

# ---------------------------
# Logging
# ---------------------------

setup_logging()
logger = logging.getLogger(__name__)


# ---------------------------
# Metrics
# ---------------------------

# ---------------------------
# Prometheus Metrics
# ---------------------------

MESSAGES_RECEIVED = Counter(
    "heartbeat_messages_received_total",
    "Total number of messages received from Kafka"
)

MESSAGES_VALID = Counter(
    "heartbeat_messages_valid_total",
    "Total number of valid messages processed"
)

MESSAGES_INVALID = Counter(
    "heartbeat_messages_invalid_total",
    "Total number of invalid messages sent to DLQ"
)

MESSAGES_INSERTED = Counter(
    "heartbeat_messages_inserted_total",
    "Total number of messages inserted into PostgreSQL"
)

CONSUMER_LAG = Gauge(
    "heartbeat_consumer_lag",
    "Current Kafka consumer lag"
)


shutdown_event = threading.Event()
signal.signal(signal.SIGINT, lambda s, f: shutdown_handler(s, f, shutdown_event))
signal.signal(signal.SIGTERM, lambda s, f: shutdown_handler(s, f, shutdown_event))


# ---------------------------
# Kafka Dead Letter Queue Producer
# ---------------------------
def create_dlq_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


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

        except psycopg.OperationalError:
            logger.warning("PostgreSQL not ready. Retrying in 5 seconds...")
            time.sleep(5)




# ---------------------------
# Insert Batch Logic
# ---------------------------
def insert_batch(cursor, events):
    query = """
        INSERT INTO customer_heartbeats
        (customer_id, recorded_at, heart_rate)
        VALUES (%s, %s, %s)
    """

    values = [
        (
            e["customer_id"],
            e["timestamp"],
            e["heart_rate"],
        )
        for e in events
    ]

    cursor.copy(query, values)
    
 


# ---------------------------
# calculate_consumer_lag
# ---------------------------
def calculate_consumer_lag(consumer):
    lag_info = []

    partitions = consumer.assignment()

    for partition in partitions:
        end_offset = consumer.end_offsets([partition])[partition]
        committed = consumer.committed(partition)

        if committed is None:
            committed = 0

        lag = end_offset - committed

        lag_info.append(
            {
                "partition": partition.partition,
                "lag": lag,
            }
        )

    return lag_info



# ---------------------------
# Main Loop
# ---------------------------

def run_consumer():
    """
    Main consumer loop.

    Responsibilities:
    - Poll Kafka for heartbeat events
    - Validate events
    - Send invalid events to Dead Letter Topic (DLQ)
    - Batch insert valid events into PostgreSQL
    - Commit Kafka offsets manually
    - Log processing metrics and consumer lag periodically
    - Gracefully shut down on termination signal
    """

    logger.info("Starting consumer service...")

    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
    POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))

    # Metrics counters
    total_received = 0
    total_valid = 0
    total_invalid = 0
    total_inserted = 0
    last_metrics_log = time.time()
    BATCH_TIMEOUT = 2  # seconds
    last_flush_time = time.time()

    dlq_producer = create_dlq_producer()
    consumer = create_consumer()
    conn = create_db_connection()
    cursor = conn.cursor()

    batch = []
    
    start_http_server(8000)
    logger.info("Metrics server started on port 8000")
    MESSAGES_RECEIVED.inc()


    while not shutdown_event.is_set():

        records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

        for messages in records.values():
            for record in messages:
                total_received += 1
                event = record.value
                MESSAGES_RECEIVED.inc()


                if validate_event(event):
                    batch.append(event)
                    total_valid += 1
                    MESSAGES_VALID.inc()
                else:
                    logger.warning(f"Invalid event sent to DLQ: {event}")
                    dlq_producer.send(DLQ_TOPIC, event)
                    total_invalid += 1
                    MESSAGES_INVALID.inc()

        # Hybrid insert strategy: batch insert if batch size or timeout reached
        if len(batch) >= BATCH_SIZE or (time.time() - last_flush_time >= BATCH_TIMEOUT and batch):
            try:
                start_batch_time = time.time()

                insert_batch(cursor, batch)
                conn.commit()
                consumer.commit()

                batch_duration = time.time() - start_batch_time
                total_inserted += len(batch)
                MESSAGES_INSERTED.inc(len(batch))

                logger.info(
                    f"Inserted batch of {len(batch)} events "
                    f"in {batch_duration:.2f} seconds."
                )

                batch.clear()

            except Exception as e:
                conn.rollback()
                logger.error(f"Batch insert failed: {e}")

        # --------- Metrics & Lag Logging ---------
        current_time = time.time()

        if current_time - last_metrics_log >= METRICS_LOG_INTERVAL:

            elapsed = current_time - last_metrics_log
            rate = total_received / elapsed if elapsed > 0 else 0

            lag_info = calculate_consumer_lag(consumer)
            total_lag = sum(lag["lag"] for lag in lag_info)
            CONSUMER_LAG.set(total_lag)

            logger.info(
                f"[METRICS] "
                f"received={total_received} "
                f"valid={total_valid} "
                f"invalid={total_invalid} "
                f"inserted={total_inserted} "
                f"rate={rate:.2f} msg/sec "
                f"lag={lag_info}"
            )

            # Reset counters for next interval
            total_received = 0
            total_valid = 0
            total_invalid = 0
            total_inserted = 0
            last_metrics_log = current_time

    # Flush remaining batch before shutdown
    if batch:
        try:
            insert_batch(cursor, batch)
            conn.commit()
            consumer.commit()
            logger.info(f"Inserted final batch of {len(batch)} events.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Final batch insert failed: {e}")

    logger.info("Shutting down consumer...")
    consumer.close()
    cursor.close()
    conn.close()



if __name__ == "__main__":
    run_consumer()
