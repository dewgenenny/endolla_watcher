from typing import Iterable, Dict, Any

UNAVAILABLE_STATUSES = {"OUT_OF_ORDER", "UNAVAILABLE"}


def from_records(records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    """Compute statistics from a list of port records."""
    chargers = 0
    unavailable = 0
    charging = 0
    sessions = 0
    for r in records:
        chargers += 1
        status = r.get("status")
        if status in UNAVAILABLE_STATUSES:
            unavailable += 1
        if status == "IN_USE":
            charging += 1
        sessions += len(r.get("sessions", []))
    return {
        "chargers": chargers,
        "unavailable": unavailable,
        "charging": charging,
        "sessions": sessions,
    }
