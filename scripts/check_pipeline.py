#!/usr/bin/env python3
"""Simple pipeline smoke-test.

Produces one heartbeat message to Kafka, waits for the consumer to insert
it into PostgreSQL, then verifies the row exists.

Usage: python scripts/check_pipeline.py
"""
import os
import time
import json
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
import psycopg


load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeat-topic")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "heartbeat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "heartbeat_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "heartbeat_pass")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))


def produce_message(producer, payload):
    producer.send(KAFKA_TOPIC, value=payload)
    producer.flush(timeout=10)


def count_matching(conn, customer_id, recorded_at):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM customer_heartbeats WHERE customer_id = %s AND recorded_at >= %s",
            (customer_id, recorded_at),
        )
        return cur.fetchone()[0]


def main():
    # Prepare test payload
    customer_id = str(uuid.uuid4())[:8]
    recorded_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "customer_id": customer_id,
        "timestamp": recorded_at,
        "heart_rate": 72,
    }

    print("Connecting to Kafka broker:", KAFKA_BROKER)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Connecting to Postgres:", POSTGRES_HOST)
    conn = None
    # connect to Postgres (retry a few times)
    for _ in range(5):
        try:
            conn = psycopg.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                port=POSTGRES_PORT,
            )
            conn.autocommit = True
            break
        except Exception as e:
            print("Postgres not ready, retrying:", e)
            time.sleep(2)

    if conn is None:
        print("Failed to connect to Postgres; aborting.")
        return 2

    # count before
    before = count_matching(conn, customer_id, recorded_at)
    print(f"Rows matching before produce: {before}")

    print("Producing test message to Kafka topic", KAFKA_TOPIC)
    produce_message(producer, payload)

    # wait for consumer to process; poll DB for new row
    deadline = time.time() + 30
    found = 0
    while time.time() < deadline:
        found = count_matching(conn, customer_id, recorded_at)
        if found > before:
            break
        time.sleep(1)

    if found > before:
        print("Success: message processed and inserted into Postgres.")
        return 0
    else:
        print("Timed out waiting for consumer to insert message.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
