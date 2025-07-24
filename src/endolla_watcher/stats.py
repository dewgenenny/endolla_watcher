from typing import Iterable, Dict, Any

UNAVAILABLE_STATUSES = {"OUT_OF_ORDER", "UNAVAILABLE"}


def from_records(records: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    """Compute statistics from a list of port records."""
    chargers = 0
    unavailable = 0
    charging = 0
    sessions = 0
    duration_total = 0.0
    duration_count = 0
    for r in records:
        chargers += 1
        status = r.get("status")
        if status in UNAVAILABLE_STATUSES:
            unavailable += 1
        if status == "IN_USE":
            charging += 1
        rec_sessions = r.get("sessions", [])
        sessions += len(rec_sessions)
        for s in rec_sessions:
            if "duration" in s:
                duration_total += float(s["duration"])
                duration_count += 1
    avg = duration_total / duration_count if duration_count else 0.0
    return {
        "chargers": chargers,
        "unavailable": unavailable,
        "charging": charging,
        "sessions": sessions,
        "avg_session_min": avg,
    }
