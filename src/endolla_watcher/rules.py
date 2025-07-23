from dataclasses import dataclass

@dataclass
class Rules:
    """Configuration for detecting problematic chargers."""

    # Days with no port usage to consider a charger unused
    unused_days: int = 4
    # Past window to look for long sessions
    long_session_days: int = 2
    # Minimum duration of a session to count as long (minutes)
    long_session_min: int = 5
    # Continuous hours with all ports unavailable
    unavailable_hours: int = 24
