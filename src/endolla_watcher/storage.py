import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from .rules import Rules
from . import stats as stats_mod
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

# Current schema version for migrations
CURRENT_SCHEMA_VERSION = 1

MIGRATIONS = {
    1: SCHEMA,
}

# Delete records older than this many days
MAX_DATA_AGE_DAYS = 28

UNAVAILABLE_STATUSES = {"OUT_OF_ORDER", "UNAVAILABLE"}

PortKey = Tuple[str | None, str | None, str | None]


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Apply pending schema migrations to the database."""
    cur = conn.execute("PRAGMA user_version")
    version = cur.fetchone()[0]
    logger.debug("Current schema version: %d", version)
    for v in range(version + 1, CURRENT_SCHEMA_VERSION + 1):
        script = MIGRATIONS.get(v)
        if script:
            logger.info("Applying migration %d", v)
            conn.executescript(script)
            conn.execute(f"PRAGMA user_version = {v}")
            conn.commit()
    logger.debug("Migrations complete")


def prune_old_data(conn: sqlite3.Connection, max_age_days: int = MAX_DATA_AGE_DAYS) -> None:
    """Remove data older than the configured retention window."""
    cutoff = datetime.now().astimezone() - timedelta(days=max_age_days)
    logger.debug("Pruning data older than %s", cutoff)
    conn.execute("DELETE FROM port_status WHERE ts < ?", (cutoff.isoformat(),))
    conn.commit()


def connect(path: Path) -> sqlite3.Connection:
    """Open connection and ensure schema and retention policy."""
    logger.debug("Connecting to database %s", path)
    conn = sqlite3.connect(path)
    _apply_migrations(conn)
    prune_old_data(conn)
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
    prune_old_data(conn)
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


def _recent_status_history(
    conn: sqlite3.Connection, since: datetime
) -> Dict[PortKey, List[Tuple[datetime, str]]]:
    """Return status history for each port since a given time."""
    logger.debug("Fetching status history since %s", since)
    cur = conn.execute(
        "SELECT location_id, station_id, port_id, ts, status FROM port_status WHERE ts >= ? ORDER BY location_id, station_id, port_id, ts",
        (since.isoformat(),),
    )
    history: Dict[PortKey, List[Tuple[datetime, str]]] = {}
    for loc, sta, port, ts, status in cur:
        key = (loc, sta, port)
        history.setdefault(key, []).append((datetime.fromisoformat(ts), status))
    logger.debug("Loaded status history for %d ports", len(history))
    return history


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
            continue

        logger.debug("Port %s is healthy", port)

    logger.debug("Identified %d problematic ports", len(problematic))
    return problematic


def analyze_chargers(conn: sqlite3.Connection, rules: Rules | None = None) -> List[Dict[str, Any]]:
    """Classify chargers as problematic based on configurable rules."""
    if rules is None:
        rules = Rules()

    now = datetime.now().astimezone()
    earliest = now - timedelta(
        days=max(rules.unused_days, rules.long_session_days, rules.unavailable_hours / 24)
    )
    history = _recent_status_history(conn, earliest)

    # Organize data per station
    stations: Dict[Tuple[str | None, str | None], Dict[str | None, List[Tuple[datetime, str]]]] = {}
    for (loc, sta, port), events in history.items():
        stations.setdefault((loc, sta), {})[port] = events

    problematic: List[Dict[str, Any]] = []
    for (loc, sta), ports in stations.items():
        reasons: List[str] = []

        # Determine how much history we have for this charger
        earliest_ts = min(ts for events in ports.values() for ts, _ in events)
        history_span = now - earliest_ts

        # Rule 1: no usage for more than N days
        if history_span >= timedelta(days=rules.unused_days):
            since_unused = now - timedelta(days=rules.unused_days)
            used_recently = any(
                any(status == "IN_USE" and ts >= since_unused for ts, status in events)
                for events in ports.values()
            )
            if not used_recently:
                reasons.append(f"unused > {rules.unused_days}d")
        else:
            logger.debug(
                "Skipping unused rule for %s/%s due to insufficient history", loc, sta
            )

        # Rule 2: no long sessions in past window
        if history_span >= timedelta(days=rules.long_session_days):
            since_long = now - timedelta(days=rules.long_session_days)
            has_long = any(
                any(
                    d >= rules.long_session_min
                    for d in _session_durations([(ts, st) for ts, st in events if ts >= since_long])
                )
                for events in ports.values()
            )
            if not has_long:
                reasons.append(
                    f"no session >= {rules.long_session_min}min in {rules.long_session_days}d"
                )
        else:
            logger.debug(
                "Skipping long session rule for %s/%s due to insufficient history", loc, sta
            )

        # Rule 3: all ports unavailable for continuous hours
        if history_span >= timedelta(hours=rules.unavailable_hours):
            since_unavail = now - timedelta(hours=rules.unavailable_hours)
            all_unavail = all(
                all(
                    status in UNAVAILABLE_STATUSES for ts, status in events if ts >= since_unavail
                )
                and any(ts >= since_unavail for ts, _ in events)
                for events in ports.values()
            )
            if all_unavail and ports:
                reasons.append(f"unavailable > {rules.unavailable_hours}h")
        else:
            logger.debug(
                "Skipping unavailable rule for %s/%s due to insufficient history", loc, sta
            )

        if reasons:
            problematic.append(
                {
                    "location_id": loc,
                    "station_id": sta,
                    "port_id": None,
                    "status": None,
                    "reason": ", ".join(reasons),
                }
            )
        else:
            logger.debug("Charger %s/%s is healthy", loc, sta)

    logger.debug("Identified %d problematic chargers", len(problematic))
    return problematic


def _latest_records(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return the most recent status entry for each port."""
    cur = conn.execute(
        """
        SELECT ps.location_id, ps.station_id, ps.port_id, ps.status, ps.last_updated
        FROM port_status ps
        JOIN (
            SELECT location_id, station_id, port_id, MAX(ts) AS max_ts
            FROM port_status
            GROUP BY location_id, station_id, port_id
        ) latest
        ON ps.location_id = latest.location_id
        AND ps.station_id = latest.station_id
        AND ps.port_id = latest.port_id
        AND ps.ts = latest.max_ts
        """
    )
    return [
        {
            "location_id": loc,
            "station_id": sta,
            "port_id": port,
            "status": status,
            "last_updated": last,
        }
        for loc, sta, port, status, last in cur
    ]


def _all_history(conn: sqlite3.Connection) -> Dict[PortKey, List[Tuple[datetime, str]]]:
    """Return full status history grouped by port."""
    cur = conn.execute(
        "SELECT location_id, station_id, port_id, ts, status FROM port_status ORDER BY location_id, station_id, port_id, ts"
    )
    history: Dict[PortKey, List[Tuple[datetime, str]]] = {}
    for loc, sta, port, ts, status in cur:
        history.setdefault((loc, sta, port), []).append((datetime.fromisoformat(ts), status))
    return history


def stats_from_db(conn: sqlite3.Connection) -> Dict[str, int]:
    """Compute statistics based on data stored in the database."""
    latest = _latest_records(conn)
    stats = stats_mod.from_records(latest)
    history = _all_history(conn)
    stats["sessions"] = sum(len(_session_durations(v)) for v in history.values())
    return stats


def timeline_stats(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return aggregated statistics for each stored snapshot."""
    placeholders = ",".join("?" for _ in UNAVAILABLE_STATUSES)
    cur = conn.execute(
        f"""
        SELECT ts,
               COUNT(*) AS chargers,
               SUM(CASE WHEN status IN ({placeholders}) THEN 1 ELSE 0 END) AS unavailable,
               SUM(CASE WHEN status = 'IN_USE' THEN 1 ELSE 0 END) AS charging
        FROM port_status
        GROUP BY ts
        ORDER BY ts
        """,
        tuple(UNAVAILABLE_STATUSES),
    )
    result = [
        {
            "ts": ts,
            "chargers": chargers,
            "unavailable": unavailable,
            "charging": charging,
        }
        for ts, chargers, unavailable, charging in cur
    ]
    logger.debug("Loaded timeline with %d points", len(result))
    return result
