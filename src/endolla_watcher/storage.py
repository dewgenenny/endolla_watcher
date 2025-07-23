import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
import logging

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS port_status (
    ts TEXT NOT NULL,
    location_id TEXT,
    station_id TEXT,
    port_id TEXT,
    status TEXT,
    last_updated TEXT
);
CREATE INDEX IF NOT EXISTS idx_port_ts ON port_status(location_id, station_id, port_id, ts);
"""

PortKey = Tuple[str | None, str | None, str | None]


def connect(path: Path) -> sqlite3.Connection:
    """Open connection and ensure schema exists."""
    logger.debug("Connecting to database %s", path)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def save_snapshot(conn: sqlite3.Connection, records: Iterable[Dict[str, Any]], ts: datetime | None = None) -> None:
    """Persist a snapshot of all port statuses."""
    if ts is None:
        ts = datetime.now().astimezone()
    logger.debug("Saving snapshot at %s", ts)
    rows = [
        (
            ts.isoformat(),
            r.get("location_id"),
            r.get("station_id"),
            r.get("port_id"),
            r.get("status"),
            r.get("last_updated"),
        )
        for r in records
    ]
    conn.executemany(
        "INSERT INTO port_status (ts, location_id, station_id, port_id, status, last_updated) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    logger.debug("Saved snapshot with %d rows", len(rows))


def _session_durations(statuses: List[Tuple[datetime, str]]) -> List[float]:
    """Return session durations in minutes from a status timeline."""
    sessions: List[float] = []
    start: datetime | None = None
    for ts, status in statuses:
        if status == "IN_USE":
            if start is None:
                start = ts
        else:
            if start is not None:
                sessions.append((ts - start).total_seconds() / 60)
                start = None
    logger.debug("Computed %d session durations", len(sessions))
    return sessions


def recent_sessions(conn: sqlite3.Connection, since: datetime) -> Dict[PortKey, List[float]]:
    """Get session durations for each port since a given time."""
    logger.debug("Fetching sessions since %s", since)
    cur = conn.execute(
        "SELECT location_id, station_id, port_id, ts, status FROM port_status WHERE ts >= ? ORDER BY location_id, station_id, port_id, ts",
        (since.isoformat(),),
    )
    history: Dict[PortKey, List[Tuple[datetime, str]]] = {}
    for loc, sta, port, ts, status in cur:
        key = (loc, sta, port)
        history.setdefault(key, []).append((datetime.fromisoformat(ts), status))
    result = {k: _session_durations(v) for k, v in history.items()}
    logger.debug("Loaded history for %d ports", len(result))
    return result


def analyze_recent(conn: sqlite3.Connection, days: int = 7, short_threshold: int = 3) -> List[Dict[str, Any]]:
    """Return problematic chargers based on recent history."""
    since = datetime.now().astimezone() - timedelta(days=days)
    logger.debug("Analyzing recent data since %s", since)
    sessions = recent_sessions(conn, since)
    problematic: List[Dict[str, Any]] = []
    for (loc, sta, port), durs in sessions.items():
        if not durs:
            problematic.append(
                {
                    "location_id": loc,
                    "station_id": sta,
                    "port_id": port,
                    "status": None,
                    "reason": "no sessions",
                }
            )
            logger.debug("Port %s has no sessions", port)
            continue
        short = [d for d in durs if d < short_threshold]
        if short:
            problematic.append(
                {
                    "location_id": loc,
                    "station_id": sta,
                    "port_id": port,
                    "status": None,
                    "reason": f"short sessions: {len(short)}",
                }
            )
            logger.debug("Port %s has %d short sessions", port, len(short))
    logger.debug("Identified %d problematic ports", len(problematic))
    return problematic
