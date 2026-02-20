"""
Event validation logic for heartbeat pipeline.
"""

from typing import Dict

def validate_event(event: Dict) -> bool:
    """
        Validate heartbeat event schema and constraints.

        Args:
            event (dict): Event payload

        Returns:
            bool: True if valid, False otherwise
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