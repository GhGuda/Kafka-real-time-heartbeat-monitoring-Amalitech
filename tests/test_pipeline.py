import os
import time
import json
import uuid
from datetime import datetime, timezone

import pytest
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


@pytest.mark.integration
def test_full_pipeline_processes_message():
    # Prepare test payload
    customer_id = str(uuid.uuid4())[:8]
    recorded_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "customer_id": customer_id,
        "timestamp": recorded_at,
        "heart_rate": 72,
    }

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # connect to Postgres
    conn = None
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
        except Exception:
            time.sleep(1)

    assert conn is not None, "Could not connect to Postgres for test"

    before = count_matching(conn, customer_id, recorded_at)

    produce_message(producer, payload)

    deadline = time.time() + 30
    found = 0
    while time.time() < deadline:
        found = count_matching(conn, customer_id, recorded_at)
        if found > before:
            break
        time.sleep(1)

    assert found > before, "Consumer did not insert message into Postgres within timeout"
