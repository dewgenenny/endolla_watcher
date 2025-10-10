import sqlite3
from datetime import datetime, timedelta, timezone
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
CREATE INDEX IF NOT EXISTS idx_port_ts
    ON port_status(location_id, station_id, port_id, ts);
"""

# Current schema version for migrations
CURRENT_SCHEMA_VERSION = 2

MIGRATIONS = {
    1: SCHEMA,
    2: "CREATE INDEX IF NOT EXISTS idx_ts ON port_status(ts);",
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
    # Reclaim space from deleted rows
    conn.execute("PRAGMA incremental_vacuum")


def connect(path: Path) -> sqlite3.Connection:
    """Open connection and ensure schema and retention policy."""
    logger.debug("Connecting to database %s", path)
    conn = sqlite3.connect(path)
    # Enable incremental auto_vacuum so deleted rows free space over time
    conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
    _apply_migrations(conn)
    prune_old_data(conn)
    return conn


def db_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    """Return basic statistics about the database."""
    rows = conn.execute("SELECT COUNT(*) FROM port_status").fetchone()[0]
    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
    stats = {
        "rows": rows,
        "page_count": page_count,
        "page_size": page_size,
        "freelist": freelist,
        "size_bytes": page_count * page_size,
    }
    logger.debug("Database stats: %s", stats)
    return stats


def compress_db(conn: sqlite3.Connection) -> None:
    """Run VACUUM to reclaim unused space and defragment the database."""
    logger.info("Compressing database")
    conn.execute("VACUUM")
    conn.execute("PRAGMA optimize")


def save_snapshot(conn: sqlite3.Connection, records: Iterable[Dict[str, Any]], ts: datetime | None = None) -> None:
    """Persist only status changes for each port."""
    if ts is None:
        ts = datetime.now().astimezone()
    logger.debug("Saving snapshot at %s", ts)
    rows = []
    for r in records:
        loc = r.get("location_id")
        sta = r.get("station_id")
        port = r.get("port_id")
        status = r.get("status")
        last = conn.execute(
            "SELECT status FROM port_status WHERE location_id=? AND station_id=? AND port_id=? ORDER BY ts DESC LIMIT 1",
            (loc, sta, port),
        ).fetchone()
        if last and last[0] == status:
            continue
        rows.append(
            (
                ts.isoformat(),
                loc,
                sta,
                port,
                status,
                r.get("last_updated"),
            )
        )
    if rows:
        conn.executemany(
            "INSERT INTO port_status (ts, location_id, station_id, port_id, status, last_updated) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
        logger.debug("Saved snapshot with %d rows", len(rows))
    else:
        logger.debug("No status changes to save")
    prune_old_data(conn)


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


def _session_records(
    statuses: List[Tuple[datetime, str]]
) -> List[Tuple[datetime, datetime, float]]:
    """Return session start/end and duration in minutes."""
    sessions: List[Tuple[datetime, datetime, float]] = []
    start: datetime | None = None
    for ts, status in statuses:
        if status == "IN_USE":
            if start is None:
                start = ts
        else:
            if start is not None:
                dur = (ts - start).total_seconds() / 60
                sessions.append((start, ts, dur))
                start = None
    logger.debug("Computed %d session records", len(sessions))
    return sessions


def _recent_status_history(
    conn: sqlite3.Connection,
    since: datetime,
    until: datetime | None = None,
) -> Dict[PortKey, List[Tuple[datetime, str]]]:
    """Return status history for each port since a given time."""
    logger.debug("Fetching status history since %s until %s", since, until)
    params = [since.isoformat()]
    query = (
        "SELECT location_id, station_id, port_id, ts, status FROM port_status "
        "WHERE ts >= ?"
    )
    if until is not None:
        query += " AND ts <= ?"
        params.append(until.isoformat())
    query += " ORDER BY location_id, station_id, port_id, ts"
    cur = conn.execute(query, params)
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
    rule_counts: Dict[str, int] = {"unused": 0, "no_long": 0, "unavailable": 0}
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


def analyze_chargers(
    conn: sqlite3.Connection,
    rules: Rules | None = None,
    *,
    now: datetime | None = None,
    history: Dict[PortKey, List[Tuple[datetime, str]]] | None = None,
) -> tuple[list[Dict[str, Any]], Dict[str, int]]:
    """Classify chargers as problematic based on configurable rules."""
    if rules is None:
        rules = Rules()

    if now is None:
        now = datetime.now().astimezone()
    earliest = now - timedelta(
        days=max(rules.unused_days, rules.long_session_days, rules.unavailable_hours / 24)
    )
    if history is None:
        history = _recent_status_history(conn, earliest, now)
    else:
        filtered: Dict[PortKey, List[Tuple[datetime, str]]] = {}
        for key, events in history.items():
            ev = [
                (ts, st)
                for ts, st in events
                if earliest <= ts <= now
            ]
            if ev:
                filtered[key] = ev
        history = filtered

    # Organize data per station
    stations: Dict[Tuple[str | None, str | None], Dict[str | None, List[Tuple[datetime, str]]]] = {}
    for (loc, sta, port), events in history.items():
        stations.setdefault((loc, sta), {})[port] = events

    problematic: List[Dict[str, Any]] = []
    rule_counts: Dict[str, int] = {"unused": 0, "no_long": 0, "unavailable": 0}
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
                rule_counts["unused"] += 1
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
                rule_counts["no_long"] += 1
        else:
            logger.debug(
                "Skipping long session rule for %s/%s due to insufficient history", loc, sta
            )

        # Rule 3: all ports unavailable for continuous hours
        if history_span >= timedelta(hours=rules.unavailable_hours):
            since_unavail = now - timedelta(hours=rules.unavailable_hours)
            all_unavail = True
            for events in ports.values():
                if not events:
                    all_unavail = False
                    break
                last_status = events[-1][1]
                if last_status not in UNAVAILABLE_STATUSES:
                    all_unavail = False
                    break
                if any(ts >= since_unavail and st not in UNAVAILABLE_STATUSES for ts, st in events):
                    all_unavail = False
                    break
            if all_unavail and ports:
                reasons.append(f"unavailable > {rules.unavailable_hours}h")
                rule_counts["unavailable"] += 1
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
    return problematic, rule_counts


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


def _count_unused_chargers(
    conn: sqlite3.Connection,
    days: int,
    now: datetime,
    history: Dict[PortKey, List[Tuple[datetime, str]]] | None = None,
) -> int:
    """Return the number of chargers unused for more than ``days``."""
    if history is None:
        # Load all available history so chargers with no recent events are
        # included in the count. ``_all_history`` is bounded by the data
        # retention policy so this remains inexpensive.
        history = _all_history(conn)
    else:
        # Trim any events in the future but keep older ones so we can detect
        # chargers with no records in the desired window.
        history = {
            k: [(ts, st) for ts, st in v if ts <= now]
            for k, v in history.items()
            if any(ts <= now for ts, _ in v)
        }

    stations: Dict[Tuple[str | None, str | None], Dict[str | None, List[Tuple[datetime, str]]]] = {}
    for (loc, sta, port), events in history.items():
        stations.setdefault((loc, sta), {})[port] = events

    count = 0
    for ports in stations.values():
        earliest_ts = min(ts for events in ports.values() for ts, _ in events)
        history_span = now - earliest_ts
        if history_span >= timedelta(days=days):
            since_unused = now - timedelta(days=days)
            used_recently = any(
                any(status == "IN_USE" and ts >= since_unused for ts, status in events)
                for events in ports.values()
            )
            if not used_recently:
                count += 1
    return count


def stats_from_db(conn: sqlite3.Connection) -> Dict[str, float]:
    """Compute statistics based on data stored in the database."""
    latest = _latest_records(conn)
    stats = stats_mod.from_records(latest)
    history = _all_history(conn)
    stats["sessions"] = sum(len(_session_durations(v)) for v in history.values())
    stats["short_sessions"] = sum(
        len([d for d in _session_durations(v) if d < stats_mod.SHORT_SESSION_MAX_MIN])
        for v in history.values()
    )

    since = datetime.now().astimezone() - timedelta(hours=24)
    durations: List[float] = []
    for events in history.values():
        for start, end, dur in _session_records(events):
            if start >= since:
                durations.append(dur)
    stats["avg_session_min"] = sum(durations) / len(durations) if durations else 0.0

    # Count charging sessions that started since midnight today
    today = datetime.now().astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
    charges_today = 0
    for events in history.values():
        for start, end, _ in _session_records(events):
            if start >= today:
                charges_today += 1
    stats["charges_today"] = charges_today
    return stats


def timeline_stats(
    conn: sqlite3.Connection, rules: Rules | None = None
) -> List[Dict[str, Any]]:
    """Return aggregated statistics every 15 minutes for the past week."""
    since = datetime.now().astimezone() - timedelta(days=7)
    if rules is None:
        rules = Rules()
    history_span = max(
        rules.unused_days,
        rules.long_session_days,
        rules.unavailable_hours / 24,
    )
    full_history = _recent_status_history(
        conn,
        since - timedelta(days=history_span),
    )
    slot_set = {
        datetime.fromtimestamp(
            (int(ts.timestamp()) // 900) * 900, tz=timezone.utc
        )
        for events in full_history.values()
        for ts, _ in events
        if ts >= since
    }
    slots = sorted(slot_set)
    result: List[Dict[str, Any]] = []
    for slot_ts in slots:
        slot_end = slot_ts + timedelta(minutes=15)
        chargers = 0
        unavailable = 0
        charging = 0
        for events in full_history.values():
            status = None
            for ts, st in events:
                if ts <= slot_end:
                    status = st
                else:
                    break
            if status is None:
                continue
            chargers += 1
            if status in UNAVAILABLE_STATUSES:
                unavailable += 1
            if status == "IN_USE":
                charging += 1
        problematic, _ = analyze_chargers(
            conn,
            rules,
            now=slot_end,
            history=full_history,
        )
        result.append(
            {
                "ts": slot_ts.isoformat(),
                "chargers": chargers,
                "unavailable": unavailable,
                "charging": charging,
                "problematic": len(problematic),
                "unused_1": _count_unused_chargers(
                    conn,
                    1,
                    slot_end,
                    history=full_history,
                ),
                "unused_2": _count_unused_chargers(
                    conn,
                    2,
                    slot_end,
                    history=full_history,
                ),
                "unused_7": _count_unused_chargers(
                    conn,
                    7,
                    slot_end,
                    history=full_history,
                ),
            }
        )
    logger.debug("Loaded timeline with %d points", len(result))
    return result


def charger_sessions(
    conn: sqlite3.Connection,
    location_id: str | None,
    station_id: str | None,
    limit: int = 10,
) -> Dict[str | None, List[Dict[str, Any]]]:
    """Return recent charging sessions for all ports of a charger."""
    since = datetime.now().astimezone() - timedelta(days=MAX_DATA_AGE_DAYS)
    cur = conn.execute(
        """
        SELECT port_id, ts, status
        FROM port_status
        WHERE location_id IS ? AND station_id IS ? AND ts >= ?
        ORDER BY port_id, ts
        """,
        (location_id, station_id, since.isoformat()),
    )
    history: Dict[str | None, List[Tuple[datetime, str]]] = {}
    for port, ts, status in cur:
        history.setdefault(port, []).append((datetime.fromisoformat(ts), status))

    result: Dict[str | None, List[Dict[str, Any]]] = {}
    for port, events in history.items():
        sessions = _session_records(events)
        sessions.sort(key=lambda r: r[0], reverse=True)
        trimmed = sessions[:limit]
        result[port] = [
            {
                "start": s.isoformat(timespec="seconds"),
                "end": e.isoformat(timespec="seconds"),
                "duration": dur,
            }
            for s, e, dur in trimmed
        ]
    logger.debug(
        "Loaded %d session lists for charger %s/%s", len(result), location_id, station_id
    )
    return result


def sessions_per_day(
    conn: sqlite3.Connection, days: int = 7
) -> List[Dict[str, Any]]:
    """Return number of charging sessions started each day."""
    since = datetime.now().astimezone() - timedelta(days=days)
    cur = conn.execute(
        "SELECT location_id, station_id, port_id, ts, status FROM port_status WHERE ts >= ? ORDER BY location_id, station_id, port_id, ts",
        (since.isoformat(),),
    )
    history: Dict[PortKey, List[Tuple[datetime, str]]] = {}
    for loc, sta, port, ts, status in cur:
        history.setdefault((loc, sta, port), []).append((datetime.fromisoformat(ts), status))

    counts: Dict[str, int] = {}
    for events in history.values():
        last_status: str | None = None
        for ts, status in events:
            if status == "IN_USE" and last_status != "IN_USE":
                if ts >= since:
                    day = ts.date().isoformat()
                    counts[day] = counts.get(day, 0) + 1
            last_status = status
    result = []
    for i in range(days - 1, -1, -1):
        day = (datetime.now().astimezone() - timedelta(days=i)).date().isoformat()
        result.append({"day": day, "sessions": counts.get(day, 0)})
    return result
