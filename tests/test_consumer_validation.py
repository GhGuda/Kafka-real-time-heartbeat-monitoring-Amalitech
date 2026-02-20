"""
Unit tests for consumer validation logic.
"""

import pytest
from app.core.validation import validate_event


def test_valid_event():
    """Valid heartbeat event should pass validation."""
    event = {
        "customer_id": "CUST_001",
        "timestamp": "2026-01-01T12:00:00Z",
        "heart_rate": 80,
    }

    assert validate_event(event) is True


def test_invalid_missing_field():
    """Event missing required fields should fail validation."""
    event = {
        "customer_id": "CUST_001",
        "heart_rate": 80,
    }

    assert validate_event(event) is False


def test_invalid_heart_rate_low():
    """Heart rate below DB constraint should fail."""
    event = {
        "customer_id": "CUST_001",
        "timestamp": "2026-01-01T12:00:00Z",
        "heart_rate": 10,
    }

    assert validate_event(event) is False


def test_invalid_heart_rate_high():
    """Heart rate above DB constraint should fail."""
    event = {
        "customer_id": "CUST_001",
        "timestamp": "2026-01-01T12:00:00Z",
        "heart_rate": 500,
    }

    assert validate_event(event) is False