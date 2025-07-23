from datetime import datetime, timedelta
from typing import Dict, List


SHORT_SESSION_THRESHOLD_MIN = 3
INACTIVE_DAYS_THRESHOLD = 7


def analyze(records: List[Dict[str, any]]) -> List[Dict[str, any]]:
    """Return ports with no sessions or sessions below the threshold."""
    problematic: List[Dict[str, any]] = []
    now = datetime.now().astimezone()

    for r in records:
        sessions = r.get("sessions", [])
        if sessions:
            short_sessions = [s for s in sessions if s.get("duration", 0) < SHORT_SESSION_THRESHOLD_MIN]
            if short_sessions:
                r["reason"] = f"short sessions: {len(short_sessions)}"
                problematic.append(r)
                continue
        else:
            last = r.get("last_updated")
            if last:
                try:
                    last_time = datetime.fromisoformat(last)
                except ValueError:
                    last_time = None
                if last_time and now - last_time > timedelta(days=INACTIVE_DAYS_THRESHOLD):
                    r["reason"] = "no sessions"
                    problematic.append(r)
        
    return problematic
