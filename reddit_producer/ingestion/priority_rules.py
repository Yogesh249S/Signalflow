"""
ingestion/priority_rules.py
===========================
CHANGE: no logic change. Cleaned up to a lookup-table pattern so adding
a new tier only requires one line, not a new if/elif branch.
"""

# Age thresholds in minutes → priority label
# Evaluated top-to-bottom; first match wins
_TIERS: list[tuple[float, str]] = [
    (5,    "aggressive"),   # <5 min old  — refresh every 5 min
    (60,   "normal"),       # <60 min old — refresh every 30 min
    (1440, "slow"),         # <24 h old   — refresh every 2 h
]


def calculate_priority(created_utc: float, now: float) -> str:
    """
    Return the polling priority label for a post given its creation
    Unix timestamp and the current time.
    """
    age_minutes = (now - created_utc) / 60
    for threshold, label in _TIERS:
        if age_minutes < threshold:
            return label
    return "inactive"
