"""
Unit tests for synthetic heartbeat data generator.
"""

import re
from datetime import datetime
from app.generator.generator import (
    generate_heart_rate,
    generate_heartbeat,
    HEART_RATE_DB_MIN,
    HEART_RATE_DB_MAX,
    HEART_RATE_NORMAL_MIN,
    HEART_RATE_NORMAL_MAX,
)


def test_generate_heart_rate_within_db_bounds():
    """Heart rate must always stay within DB constraint (30–220)."""
    for _ in range(1000):
        hr = generate_heart_rate()
        assert HEART_RATE_DB_MIN <= hr <= HEART_RATE_DB_MAX


def test_generate_heart_rate_typical_distribution():
    """Most heart rates should fall within normal physiological range."""
    normal_count = 0
    total = 1000

    for _ in range(total):
        hr = generate_heart_rate()
        if HEART_RATE_NORMAL_MIN <= hr <= HEART_RATE_NORMAL_MAX:
            normal_count += 1

    # Since anomaly probability is 5%, at least ~90% should be normal
    assert normal_count > total * 0.85


def test_generate_heartbeat_structure():
    """Generated heartbeat should contain required keys."""
    event = generate_heartbeat()

    assert "customer_id" in event
    assert "timestamp" in event
    assert "heart_rate" in event


def test_generate_heartbeat_timestamp_format():
    """Timestamp should be ISO formatted and timezone-aware."""
    event = generate_heartbeat()
    ts = event["timestamp"]

    # Ensure ISO parseable
    parsed = datetime.fromisoformat(ts)
    assert parsed.tzinfo is not None


def test_generate_heartbeat_customer_id_format():
    """Customer ID should follow CUST_XXX pattern."""
    event = generate_heartbeat()
    customer_id = event["customer_id"]

    pattern = r"^CUST_\d{3}$"
    assert re.match(pattern, customer_id)