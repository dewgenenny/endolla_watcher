"""Persistence helpers backed by a MySQL database."""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import groupby
from typing import Any, Callable, Dict, Iterable, Iterator, List, Sequence, Tuple
from urllib.parse import urlparse, unquote

import pymysql
from pymysql.connections import Connection

from . import stats as stats_mod
from .rules import Rules

logger = logging.getLogger(__name__)

# Data retention policy
HIGH_DETAIL_DAYS = 7
MEDIUM_DETAIL_DAYS = 30

UNAVAILABLE_STATUSES = {"OUT_OF_ORDER", "UNAVAILABLE"}
OCCUPIED_STATUSES = {"IN_USE", "FINISHED", "COMPLETED", "OCCUPIED", "CHARGING"}
ACTIVE_CHARGING_STATUSES = {"IN_USE", "CHARGING"}

PortKey = Tuple[str | None, str | None, str | None]


@dataclass
class MySQLConfig:
    """Connection details for the Endolla Watcher database."""

    host: str
    port: int
    user: str
    password: str | None
    database: str

    @classmethod
    def from_env(cls) -> "MySQLConfig":
        url = os.getenv("ENDOLLA_DB_URL")
        if not url:
            raise RuntimeError("ENDOLLA_DB_URL environment variable is required")
        return cls.from_url(url)

    @classmethod
    def from_url(cls, url: str) -> "MySQLConfig":
        parsed = urlparse(url)
        if parsed.scheme not in {"mysql", "mysql+pymysql"}:
            raise ValueError(f"Unsupported MySQL URL scheme: {parsed.scheme}")
        if parsed.username is None:
            raise ValueError("MySQL URL must include a username")
        if parsed.hostname is None:
            raise ValueError("MySQL URL must include a hostname")
        database = parsed.path.lstrip("/")
        if not database:
            raise ValueError("MySQL URL must include a database name")
        password = unquote(parsed.password) if parsed.password else None
        return cls(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=parsed.username,
            password=password,
            database=database,
        )


SCHEMA_STATEMENTS: Sequence[str] = (
    """
    CREATE TABLE IF NOT EXISTS schema_version (
        id TINYINT PRIMARY KEY,
        version INT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS port_status (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        ts VARCHAR(64) NOT NULL,
        location_id VARCHAR(64) NULL,
        station_id VARCHAR(64) NULL,
        port_id VARCHAR(64) NULL,
        status VARCHAR(32) NULL,
        last_updated VARCHAR(64) NULL,
        INDEX idx_port_ts (location_id, station_id, port_id, ts),
        INDEX idx_ts (ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)

CURRENT_SCHEMA_VERSION = 1


@contextmanager
def _with_cursor(conn: Connection) -> Iterator[pymysql.cursors.Cursor]:
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


def _ensure_schema(conn: Connection) -> None:
    for statement in SCHEMA_STATEMENTS:
        with _with_cursor(conn) as cur:
            cur.execute(statement)
    with _with_cursor(conn) as cur:
        cur.execute("SELECT version FROM schema_version WHERE id = 1")
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO schema_version (id, version) VALUES (1, %s)",
                (CURRENT_SCHEMA_VERSION,),
            )
            conn.commit()
        elif row[0] != CURRENT_SCHEMA_VERSION:
            cur.execute(
                "UPDATE schema_version SET version = %s WHERE id = 1",
                (CURRENT_SCHEMA_VERSION,),
            )
            conn.commit()


def _truncate_to_hour(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def _truncate_to_day(ts: datetime) -> datetime:
    return ts.replace(hour=0, minute=0, second=0, microsecond=0)


def _downsample_range(
    conn: Connection,
    *,
    bucket: Callable[[datetime], datetime],
    newer_than: datetime | None = None,
    older_than: datetime | None = None,
) -> List[int]:
    query = [
        "SELECT id, location_id, station_id, port_id, ts",
        "FROM port_status",
        "WHERE 1 = 1",
    ]
    params: List[str] = []
    if newer_than is not None:
        query.append("AND ts >= %s")
        params.append(newer_than.isoformat())
    if older_than is not None:
        query.append("AND ts < %s")
        params.append(older_than.isoformat())
    query.append("ORDER BY location_id, station_id, port_id, ts")
    sql = " ".join(query)

    seen: Dict[Tuple[PortKey, Any], int] = {}
    to_delete: List[int] = []
    with _with_cursor(conn) as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            row_id, loc, sta, port, ts_str = row
            try:
                ts = datetime.fromisoformat(ts_str)
            except (TypeError, ValueError):
                logger.debug("Unable to parse timestamp '%s' for row %s", ts_str, row_id)
                continue
            key = ((loc, sta, port), bucket(ts))
            if key in seen:
                to_delete.append(row_id)
            else:
                seen[key] = row_id
    return to_delete


def _delete_rows(conn: Connection, row_ids: Sequence[int], chunk_size: int = 1000) -> None:
    if not row_ids:
        return
    with _with_cursor(conn) as cur:
        for start in range(0, len(row_ids), chunk_size):
            chunk = row_ids[start : start + chunk_size]
            placeholders = ", ".join(["%s"] * len(chunk))
            cur.execute(
                f"DELETE FROM port_status WHERE id IN ({placeholders})",
                tuple(chunk),
            )


def prune_old_data(conn: Connection) -> None:
    now = datetime.now().astimezone()
    high_detail_cutoff = now - timedelta(days=HIGH_DETAIL_DAYS)
    medium_detail_cutoff = now - timedelta(days=MEDIUM_DETAIL_DAYS)

    to_delete: List[int] = []
    # Keep at most one record per day for very old data (low detail)
    to_delete.extend(
        _downsample_range(
            conn,
            bucket=_truncate_to_day,
            older_than=medium_detail_cutoff,
        )
    )
    # Keep at most one record per hour for medium-aged data
    to_delete.extend(
        _downsample_range(
            conn,
            bucket=_truncate_to_hour,
            newer_than=medium_detail_cutoff,
            older_than=high_detail_cutoff,
        )
    )

    if to_delete:
        _delete_rows(conn, to_delete)
        logger.debug("Pruned %d historical rows", len(to_delete))
    conn.commit()


def connect(config: MySQLConfig | str | None = None) -> Connection:
    if config is None:
        config = MySQLConfig.from_env()
    if isinstance(config, str):
        config = MySQLConfig.from_url(config)
    conn = pymysql.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
        autocommit=False,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )
    _ensure_schema(conn)
    prune_old_data(conn)
    return conn


def db_stats(conn: Connection) -> Dict[str, int]:
    with _with_cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) FROM port_status")
        rows = int(cur.fetchone()[0])
    with _with_cursor(conn) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(data_length + index_length), 0)
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = 'port_status'
            """
        )
        size_bytes = int(cur.fetchone()[0] or 0)
    with _with_cursor(conn) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(data_free), 0)
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = 'port_status'
            """
        )
        free_bytes = int(cur.fetchone()[0] or 0)
    stats = {
        "rows": rows,
        "size_bytes": size_bytes,
        "free_bytes": free_bytes,
    }
    logger.debug("Database stats: %s", stats)
    return stats


def compress_db(conn: Connection) -> None:
    with _with_cursor(conn) as cur:
        cur.execute("OPTIMIZE TABLE port_status")
    conn.commit()


def save_snapshot(
    conn: Connection,
    records: Iterable[Dict[str, Any]],
    ts: datetime | None = None,
) -> bool:
    if ts is None:
        ts = datetime.now().astimezone()
    ts_iso = ts.isoformat()
    new_rows: List[Tuple[str, str | None, str | None, str | None, str | None, str | None]] = []
    for r in records:
        loc = r.get("location_id")
        sta = r.get("station_id")
        port = r.get("port_id")
        status = r.get("status")
        with _with_cursor(conn) as cur:
            cur.execute(
                """
                SELECT status FROM port_status
                WHERE location_id <=> %s AND station_id <=> %s AND port_id <=> %s
                ORDER BY ts DESC LIMIT 1
                """,
                (loc, sta, port),
            )
            row = cur.fetchone()
        if row and row[0] == status:
            continue
        new_rows.append(
            (
                ts_iso,
                loc,
                sta,
                port,
                status,
                r.get("last_updated"),
            )
        )
    if new_rows:
        with _with_cursor(conn) as cur:
            cur.executemany(
                """
                INSERT INTO port_status (ts, location_id, station_id, port_id, status, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                new_rows,
            )
        conn.commit()
    prune_old_data(conn)
    return bool(new_rows)


def _session_durations(
    statuses: List[Tuple[datetime, str]],
    *,
    now: datetime | None = None,
) -> List[float]:
    if now is None:
        now = datetime.now().astimezone()
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
    if start is not None:
        sessions.append((now - start).total_seconds() / 60)
    return sessions


def _session_records(
    statuses: List[Tuple[datetime, str]]
) -> List[Tuple[datetime, datetime, float]]:
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
    return sessions


def _recent_status_history(
    conn: Connection,
    since: datetime,
    until: datetime | None = None,
) -> Dict[PortKey, List[Tuple[datetime, str]]]:
    params: List[Any] = [since.isoformat()]
    query = [
        "SELECT location_id, station_id, port_id, ts, status",
        "FROM port_status",
        "WHERE ts >= %s",
    ]
    if until is not None:
        query.append("AND ts <= %s")
        params.append(until.isoformat())
    query.append("ORDER BY location_id, station_id, port_id, ts")
    sql = " ".join(query)
    history: Dict[PortKey, List[Tuple[datetime, str]]] = {}
    with _with_cursor(conn) as cur:
        cur.execute(sql, params)
        for loc, sta, port, ts_str, status in cur.fetchall():
            key = (loc, sta, port)
            history.setdefault(key, []).append((datetime.fromisoformat(ts_str), status))
    return history


def _recent_location_history(
    conn: Connection,
    location_id: str | None,
    since: datetime,
    until: datetime | None = None,
) -> Dict[Tuple[str | None, str | None], List[Tuple[datetime, str]]]:
    params: List[Any] = [location_id, since.isoformat()]
    query = [
        "SELECT station_id, port_id, ts, status",
        "FROM port_status",
        "WHERE location_id <=> %s AND ts >= %s",
    ]
    if until is not None:
        query.append("AND ts <= %s")
        params.append(until.isoformat())
    query.append("ORDER BY station_id, port_id, ts")
    sql = " ".join(query)
    history: Dict[Tuple[str | None, str | None], List[Tuple[datetime, str]]] = {}
    with _with_cursor(conn) as cur:
        cur.execute(sql, params)
        for station_id, port_id, ts_str, status in cur.fetchall():
            key = (station_id, port_id)
            history.setdefault(key, []).append((datetime.fromisoformat(ts_str), status))
    return history


def recent_sessions(conn: Connection, since: datetime) -> Dict[PortKey, List[float]]:
    history = _recent_status_history(conn, since)
    return {k: _session_durations(v) for k, v in history.items()}


def analyze_recent(conn: Connection, days: int = 7, short_threshold: int = 3) -> List[Dict[str, Any]]:
    since = datetime.now().astimezone() - timedelta(days=days)
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
    return problematic


def analyze_chargers(
    conn: Connection,
    rules: Rules | None = None,
    *,
    now: datetime | None = None,
    history: Dict[PortKey, List[Tuple[datetime, str]]] | None = None,
) -> tuple[list[Dict[str, Any]], Dict[str, int]]:
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
            ev = [(ts, st) for ts, st in events if earliest <= ts <= now]
            if ev:
                filtered[key] = ev
        history = filtered

    stations: Dict[Tuple[str | None, str | None], Dict[str | None, List[Tuple[datetime, str]]]] = {}
    for (loc, sta, port), events in history.items():
        stations.setdefault((loc, sta), {})[port] = events

    problematic: List[Dict[str, Any]] = []
    rule_counts: Dict[str, int] = {"unused": 0, "no_long": 0, "unavailable": 0}
    for (loc, sta), ports in stations.items():
        reasons: List[str] = []
        earliest_ts = min(ts for events in ports.values() for ts, _ in events)
        history_span = now - earliest_ts

        if history_span >= timedelta(days=rules.unused_days):
            since_unused = now - timedelta(days=rules.unused_days)
            used_recently = any(
                any(status == "IN_USE" and ts >= since_unused for ts, status in events)
                for events in ports.values()
            )
            if not used_recently:
                reasons.append(f"unused > {rules.unused_days}d")
                rule_counts["unused"] += 1

        if history_span >= timedelta(days=rules.long_session_days):
            since_long = now - timedelta(days=rules.long_session_days)
            has_long = any(
                any(
                    d >= rules.long_session_min
                    for d in _session_durations(
                        [(ts, st) for ts, st in events if ts >= since_long], now=now
                    )
                )
                for events in ports.values()
            )
            if not has_long:
                reasons.append(
                    f"no session >= {rules.long_session_min}min in {rules.long_session_days}d"
                )
                rule_counts["no_long"] += 1

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
    return problematic, rule_counts


def _latest_records(conn: Connection) -> List[Dict[str, Any]]:
    query = """
        SELECT ps.location_id, ps.station_id, ps.port_id, ps.status, ps.last_updated
        FROM port_status ps
        JOIN (
            SELECT location_id, station_id, port_id, MAX(ts) AS max_ts
            FROM port_status
            GROUP BY location_id, station_id, port_id
        ) latest
        ON ps.location_id <=> latest.location_id
        AND ps.station_id <=> latest.station_id
        AND ps.port_id <=> latest.port_id
        AND ps.ts = latest.max_ts
    """
    results: List[Dict[str, Any]] = []
    with _with_cursor(conn) as cur:
        cur.execute(query)
        for loc, sta, port, status, last in cur.fetchall():
            results.append(
                {
                    "location_id": loc,
                    "station_id": sta,
                    "port_id": port,
                    "status": status,
                    "last_updated": last,
                }
            )
    return results


def latest_status_by_locations(
    conn: Connection, location_ids: Sequence[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """Return the latest port status entries grouped by location."""

    filtered_ids = [str(loc) for loc in location_ids if loc is not None]
    if not filtered_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(filtered_ids))
    query = f"""
        SELECT ps.location_id, ps.station_id, ps.port_id, ps.status, ps.last_updated
        FROM port_status ps
        JOIN (
            SELECT location_id, station_id, port_id, MAX(ts) AS max_ts
            FROM port_status
            WHERE location_id IN ({placeholders})
            GROUP BY location_id, station_id, port_id
        ) latest
        ON ps.location_id <=> latest.location_id
        AND ps.station_id <=> latest.station_id
        AND ps.port_id <=> latest.port_id
        AND ps.ts = latest.max_ts
        WHERE ps.location_id IN ({placeholders})
    """

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    params = tuple(filtered_ids) * 2
    with _with_cursor(conn) as cur:
        cur.execute(query, params)
        for loc, sta, port, status, last_updated in cur.fetchall():
            grouped.setdefault(str(loc), []).append(
                {
                    "location_id": loc,
                    "station_id": sta,
                    "port_id": port,
                    "status": status,
                    "last_updated": last_updated,
                }
            )
    return grouped


def _all_history(conn: Connection) -> Dict[PortKey, List[Tuple[datetime, str]]]:
    history: Dict[PortKey, List[Tuple[datetime, str]]] = {}
    with _with_cursor(conn) as cur:
        cur.execute(
            "SELECT location_id, station_id, port_id, ts, status FROM port_status ORDER BY location_id, station_id, port_id, ts"
        )
        for loc, sta, port, ts_str, status in cur.fetchall():
            history.setdefault((loc, sta, port), []).append((datetime.fromisoformat(ts_str), status))
    return history


def _station_outage_durations(
    station_events: Dict[str | None, List[Tuple[datetime, str]]],
    *,
    now: datetime,
) -> List[float]:
    if not station_events:
        return []
    timeline: List[Tuple[datetime, str | None, str]] = []
    for port_id, events in station_events.items():
        for ts, status in events:
            if ts <= now:
                timeline.append((ts, port_id, status))
    if not timeline:
        return []
    timeline.sort(key=lambda item: item[0])

    statuses: Dict[str | None, str | None] = {
        port_id: None for port_id, events in station_events.items() if events
    }
    if not statuses:
        return []

    def station_down() -> bool:
        if any(status is None for status in statuses.values()):
            return False
        return bool(statuses) and all(status in UNAVAILABLE_STATUSES for status in statuses.values())

    durations: List[float] = []
    current_down = station_down()
    down_start: datetime | None = None

    for ts, group in groupby(timeline, key=lambda item: item[0]):
        prev_down = current_down
        for _, port_id, status in group:
            statuses[port_id] = status
        current_down = station_down()
        if prev_down and not current_down and down_start is not None:
            durations.append((ts - down_start).total_seconds() / 60)
            down_start = None
        elif not prev_down and current_down:
            down_start = ts

    if current_down and down_start is not None:
        durations.append((now - down_start).total_seconds() / 60)

    return durations


def _count_unused_chargers(
    conn: Connection,
    days: int,
    now: datetime,
    history: Dict[PortKey, List[Tuple[datetime, str]]] | None = None,
) -> int:
    if history is None:
        history = _all_history(conn)
    else:
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


def _status_intervals(
    events: List[Tuple[datetime, str | None]],
    *,
    end: datetime,
) -> List[Tuple[datetime, datetime, str | None]]:
    if not events:
        return []
    ordered = sorted(events, key=lambda item: item[0])
    intervals: List[Tuple[datetime, datetime, str | None]] = []
    prev_ts, prev_status = ordered[0]
    if prev_ts >= end:
        return []
    for ts, status in ordered[1:]:
        if ts <= prev_ts:
            prev_ts, prev_status = ts, status
            continue
        segment_end = min(ts, end)
        if segment_end > prev_ts:
            intervals.append((prev_ts, segment_end, prev_status))
        prev_ts, prev_status = ts, status
        if prev_ts >= end:
            break
    if prev_ts < end:
        intervals.append((prev_ts, end, prev_status))
    return [(start, stop, status) for start, stop, status in intervals if stop > start]


def _empty_totals() -> Dict[str, float]:
    return {
        "sessions": 0.0,
        "monitored_seconds": 0.0,
        "available_seconds": 0.0,
        "occupied_seconds": 0.0,
        "active_seconds": 0.0,
        "port_count": 0.0,
    }


def _accumulate_totals(target: Dict[str, float], source: Dict[str, float]) -> None:
    target["sessions"] = target.get("sessions", 0.0) + source.get("sessions", 0.0)
    target["monitored_seconds"] = target.get("monitored_seconds", 0.0) + source.get(
        "monitored_seconds", 0.0
    )
    target["available_seconds"] = target.get("available_seconds", 0.0) + source.get(
        "available_seconds", 0.0
    )
    target["occupied_seconds"] = target.get("occupied_seconds", 0.0) + source.get(
        "occupied_seconds", 0.0
    )
    target["active_seconds"] = target.get("active_seconds", 0.0) + source.get(
        "active_seconds", 0.0
    )
    target["port_count"] = target.get("port_count", 0.0) + source.get("port_count", 0.0)


def _compute_port_utilization(
    events: List[Tuple[datetime, str]],
    *,
    now: datetime,
) -> Dict[str, float] | None:
    intervals = _status_intervals(events, end=now)
    if not intervals:
        return None
    total_seconds = 0.0
    available_seconds = 0.0
    occupied_seconds = 0.0
    active_seconds = 0.0
    for start, end, status in intervals:
        if end <= start:
            continue
        duration = (end - start).total_seconds()
        if duration <= 0:
            continue
        total_seconds += duration
        if status is not None and status not in UNAVAILABLE_STATUSES:
            available_seconds += duration
        if status in OCCUPIED_STATUSES:
            occupied_seconds += duration
        if status in ACTIVE_CHARGING_STATUSES:
            active_seconds += duration
    if total_seconds <= 0:
        return None
    sessions = len(_session_durations(events, now=now))
    return {
        "sessions": float(sessions),
        "monitored_seconds": total_seconds,
        "available_seconds": available_seconds,
        "occupied_seconds": occupied_seconds,
        "active_seconds": active_seconds,
        "port_count": 1.0,
    }


def _compute_port_usage_between(
    events: List[Tuple[datetime, str]],
    start: datetime,
    end: datetime,
) -> Dict[str, float] | None:
    if not events or end <= start:
        return None
    intervals = _status_intervals(events, end=end)
    total_seconds = 0.0
    available_seconds = 0.0
    occupied_seconds = 0.0
    active_seconds = 0.0
    for interval_start, interval_end, status in intervals:
        if interval_end <= start or interval_start >= end:
            continue
        seg_start = max(interval_start, start)
        seg_end = min(interval_end, end)
        if seg_end <= seg_start:
            continue
        duration = (seg_end - seg_start).total_seconds()
        if duration <= 0:
            continue
        total_seconds += duration
        if status is not None and status not in UNAVAILABLE_STATUSES:
            available_seconds += duration
        if status in OCCUPIED_STATUSES:
            occupied_seconds += duration
        if status in ACTIVE_CHARGING_STATUSES:
            active_seconds += duration
    if total_seconds <= 0:
        return None

    ordered = sorted(events, key=lambda item: item[0])
    in_session = False
    session_count = 0
    for ts, status in ordered:
        if ts < start:
            if status == "IN_USE":
                in_session = True
            elif in_session and status != "IN_USE":
                session_count += 1
                in_session = False
            continue
        if status == "IN_USE":
            if not in_session:
                in_session = True
        else:
            if in_session:
                session_count += 1
                in_session = False
    if in_session:
        session_count += 1

    return {
        "sessions": float(session_count),
        "monitored_seconds": total_seconds,
        "available_seconds": available_seconds,
        "occupied_seconds": occupied_seconds,
        "active_seconds": active_seconds,
        "port_count": 1.0,
    }


def _format_utilization_metrics(totals: Dict[str, float]) -> Dict[str, float]:
    monitored_seconds = totals.get("monitored_seconds", 0.0)
    available_seconds = totals.get("available_seconds", 0.0)
    occupied_seconds = totals.get("occupied_seconds", 0.0)
    active_seconds = totals.get("active_seconds", 0.0)
    sessions_raw = totals.get("sessions", 0.0)
    if isinstance(sessions_raw, float) and sessions_raw.is_integer():
        sessions_value: float | int = int(sessions_raw)
    else:
        sessions_value = sessions_raw
    hours = monitored_seconds / 3600 if monitored_seconds else 0.0
    days = monitored_seconds / 86400 if monitored_seconds else 0.0
    return {
        "sessions": sessions_value,
        "monitored_seconds": monitored_seconds,
        "monitored_hours": hours,
        "monitored_days": days,
        "available_seconds": available_seconds,
        "occupied_seconds": occupied_seconds,
        "active_seconds": active_seconds,
        "session_count_per_day": sessions_raw / days if days else 0.0,
        "session_count_per_hour": sessions_raw / hours if hours else 0.0,
        "occupation_utilization_pct": (occupied_seconds / available_seconds) * 100
        if available_seconds
        else 0.0,
        "active_charging_utilization_pct": (active_seconds / available_seconds) * 100
        if available_seconds
        else 0.0,
        "availability_ratio": (available_seconds / monitored_seconds)
        if monitored_seconds
        else 0.0,
    }


def _utilization_summary(
    history: Dict[PortKey, List[Tuple[datetime, str]]],
    *,
    now: datetime,
) -> Dict[str, Any]:
    port_rows: List[Dict[str, Any]] = []
    station_totals: Dict[Tuple[str | None, str | None], Dict[str, Any]] = {}
    location_totals: Dict[str | None, Dict[str, Any]] = {}
    network_totals = _empty_totals()

    for (loc, sta, port), events in history.items():
        totals = _compute_port_utilization(events, now=now)
        if totals is None:
            continue
        metrics = _format_utilization_metrics(totals)
        port_rows.append(
            {
                "location_id": loc,
                "station_id": sta,
                "port_id": port,
                **metrics,
            }
        )

        station_key = (loc, sta)
        station_acc = station_totals.setdefault(station_key, _empty_totals())
        _accumulate_totals(station_acc, totals)

        location_acc = location_totals.setdefault(
            loc,
            {
                **_empty_totals(),
                "station_ids": set(),
            },
        )
        _accumulate_totals(location_acc, totals)
        location_acc["station_ids"].add(sta)

        _accumulate_totals(network_totals, totals)

    port_rows.sort(
        key=lambda row: (
            row.get("location_id") or "",
            row.get("station_id") or "",
            row.get("port_id") or "",
        )
    )

    station_rows: List[Dict[str, Any]] = []
    for (loc, sta), totals in station_totals.items():
        metrics = _format_utilization_metrics(totals)
        metrics.update(
            {
                "location_id": loc,
                "station_id": sta,
                "port_count": int(totals.get("port_count", 0)),
            }
        )
        station_rows.append(metrics)
    station_rows.sort(
        key=lambda row: (row.get("location_id") or "", row.get("station_id") or "")
    )

    location_rows: List[Dict[str, Any]] = []
    for loc, totals in location_totals.items():
        station_ids = totals.pop("station_ids", set())
        metrics = _format_utilization_metrics(totals)
        metrics.update(
            {
                "location_id": loc,
                "station_count": len({sid for sid in station_ids if sid is not None}),
                "port_count": int(totals.get("port_count", 0)),
            }
        )
        location_rows.append(metrics)
    location_rows.sort(key=lambda row: row.get("location_id") or "")

    network_metrics = _format_utilization_metrics(network_totals)
    network_metrics.update(
        {
            "port_count": int(network_totals.get("port_count", 0)),
            "station_count": len(station_totals),
            "location_count": len({loc for loc in location_totals.keys() if loc is not None}),
        }
    )

    return {
        "ports": port_rows,
        "stations": station_rows,
        "locations": location_rows,
        "network": network_metrics,
    }


def _location_usage_timeline(
    history: Dict[Tuple[str | None, str | None], List[Tuple[datetime, str]]],
    start: datetime,
    end: datetime,
    step: timedelta,
) -> List[Dict[str, Any]]:
    timeline: List[Dict[str, Any]] = []
    current = start
    while current < end:
        bucket_end = min(current + step, end)
        bucket_totals = _empty_totals()
        for events in history.values():
            totals = _compute_port_usage_between(events, current, bucket_end)
            if totals is None:
                continue
            _accumulate_totals(bucket_totals, totals)
        entry: Dict[str, Any] = {
            "start": current.isoformat(),
            "end": bucket_end.isoformat(),
            "port_count": int(bucket_totals.get("port_count", 0)),
            "monitored_seconds": bucket_totals.get("monitored_seconds", 0.0),
            "available_seconds": bucket_totals.get("available_seconds", 0.0),
            "occupied_seconds": bucket_totals.get("occupied_seconds", 0.0),
            "active_seconds": bucket_totals.get("active_seconds", 0.0),
            "sessions": bucket_totals.get("sessions", 0.0),
        }
        if bucket_totals.get("monitored_seconds", 0.0) > 0:
            entry.update(_format_utilization_metrics(bucket_totals))
        else:
            entry.update(
                {
                    "monitored_hours": 0.0,
                    "monitored_days": 0.0,
                    "session_count_per_day": 0.0,
                    "session_count_per_hour": 0.0,
                    "occupation_utilization_pct": 0.0,
                    "active_charging_utilization_pct": 0.0,
                    "availability_ratio": 0.0,
                }
            )
        timeline.append(entry)
        current = bucket_end
    return timeline


def location_usage(
    conn: Connection,
    location_id: str | None,
    *,
    now: datetime | None = None,
) -> Dict[str, Any] | None:
    if now is None:
        now = datetime.now().astimezone()
    lookback_start = now - timedelta(days=8)
    history = _recent_location_history(conn, location_id, lookback_start, now)
    if not history:
        return None

    day_start = now - timedelta(hours=24)
    week_start = now - timedelta(days=7)

    day_totals = _empty_totals()
    week_totals = _empty_totals()
    station_ids: set[str | None] = set()
    port_ids: set[Tuple[str | None, str | None]] = set()

    for (station_id, port_id), events in history.items():
        port_ids.add((station_id, port_id))
        station_ids.add(station_id)
        week_metrics = _compute_port_usage_between(events, week_start, now)
        if week_metrics is not None:
            _accumulate_totals(week_totals, week_metrics)
        day_metrics = _compute_port_usage_between(events, day_start, now)
        if day_metrics is not None:
            _accumulate_totals(day_totals, day_metrics)

    day_timeline = _location_usage_timeline(history, day_start, now, timedelta(hours=1))
    week_timeline = _location_usage_timeline(history, week_start, now, timedelta(days=1))

    summary = {
        "day": _format_utilization_metrics(day_totals),
        "week": _format_utilization_metrics(week_totals),
    }

    return {
        "location_id": location_id,
        "station_count": len({sid for sid in station_ids if sid is not None}),
        "port_count": len(port_ids),
        "summary": summary,
        "usage_day": {
            "start": day_start.isoformat(),
            "end": now.isoformat(),
            "bucket_minutes": 60,
            "timeline": day_timeline,
        },
        "usage_week": {
            "start": week_start.isoformat(),
            "end": now.isoformat(),
            "bucket_days": 1,
            "timeline": week_timeline,
        },
        "updated": now.isoformat(),
    }


def stats_from_db(conn: Connection, *, now: datetime | None = None) -> Dict[str, Any]:
    if now is None:
        now = datetime.now().astimezone()
    latest = _latest_records(conn)
    stats = stats_mod.from_records(latest)
    history = _all_history(conn)
    stats["sessions"] = sum(len(_session_durations(v, now=now)) for v in history.values())
    stats["short_sessions"] = sum(
        len(
            [
                d
                for d in _session_durations(v, now=now)
                if d < stats_mod.SHORT_SESSION_MAX_MIN
            ]
        )
        for v in history.values()
    )

    since = now - timedelta(hours=24)
    durations: List[float] = []
    for events in history.values():
        for start, end, dur in _session_records(events):
            if start >= since:
                durations.append(dur)
    stats["avg_session_min"] = sum(durations) / len(durations) if durations else 0.0

    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    charges_today = 0
    for events in history.values():
        for start, _, _ in _session_records(events):
            if start >= today:
                charges_today += 1
    stats["charges_today"] = charges_today
    station_histories: Dict[Tuple[str | None, str | None], Dict[str | None, List[Tuple[datetime, str]]]] = {}
    for (loc, sta, port), events in history.items():
        station_histories.setdefault((loc, sta), {})[port] = events
    outage_durations: List[float] = []
    for events in station_histories.values():
        outage_durations.extend(_station_outage_durations(events, now=now))
    stats["mttr_minutes"] = (
        sum(outage_durations) / len(outage_durations) if outage_durations else 0.0
    )
    stats["utilization"] = _utilization_summary(history, now=now)
    return stats


def timeline_stats(conn: Connection, rules: Rules | None = None) -> List[Dict[str, Any]]:
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
    return result


def charger_sessions(
    conn: Connection,
    location_id: str | None,
    station_id: str | None,
    limit: int = 10,
) -> Dict[str | None, List[Dict[str, Any]]]:
    since = datetime.now().astimezone() - timedelta(days=MEDIUM_DETAIL_DAYS)
    history: Dict[str | None, List[Tuple[datetime, str]]] = {}
    with _with_cursor(conn) as cur:
        cur.execute(
            """
            SELECT port_id, ts, status
            FROM port_status
            WHERE location_id <=> %s AND station_id <=> %s AND ts >= %s
            ORDER BY port_id, ts
            """,
            (location_id, station_id, since.isoformat()),
        )
        for port, ts_str, status in cur.fetchall():
            history.setdefault(port, []).append((datetime.fromisoformat(ts_str), status))
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
    return result


def sessions_time_series(
    conn: Connection,
    days: int = 7,
    granularity: str = "day",
) -> List[Dict[str, Any]]:
    granularity = granularity.lower()
    if granularity not in {"day", "hour"}:
        raise ValueError(f"Unsupported granularity '{granularity}'")

    now = datetime.now().astimezone()
    if granularity == "hour":
        since = (now - timedelta(days=days)).replace(minute=0, second=0, microsecond=0)
    else:
        since = now - timedelta(days=days)

    history: Dict[PortKey, List[Tuple[datetime, str]]] = {}
    with _with_cursor(conn) as cur:
        cur.execute(
            """
            SELECT location_id, station_id, port_id, ts, status
            FROM port_status
            WHERE ts >= %s
            ORDER BY location_id, station_id, port_id, ts
            """,
            (since.isoformat(),),
        )
        for loc, sta, port, ts_str, status in cur.fetchall():
            history.setdefault((loc, sta, port), []).append((datetime.fromisoformat(ts_str), status))

    counts: Dict[str, int] = {}

    def _bucket_start(ts: datetime) -> datetime:
        ts_local = ts.astimezone()
        if granularity == "hour":
            return ts_local.replace(minute=0, second=0, microsecond=0)
        return ts_local.replace(hour=0, minute=0, second=0, microsecond=0)

    for events in history.values():
        last_status: str | None = None
        for ts, status in events:
            if status == "IN_USE" and last_status != "IN_USE" and ts >= since:
                bucket = _bucket_start(ts)
                key = bucket.isoformat()
                counts[key] = counts.get(key, 0) + 1
            last_status = status

    result: List[Dict[str, Any]] = []
    if granularity == "hour":
        start = (now - timedelta(days=days)).replace(minute=0, second=0, microsecond=0)
        end = now.replace(minute=0, second=0, microsecond=0)
        current = start
        while current <= end:
            key = current.isoformat()
            result.append(
                {
                    "start": key,
                    "end": (current + timedelta(hours=1)).isoformat(),
                    "sessions": counts.get(key, 0),
                }
            )
            current += timedelta(hours=1)
    else:
        for i in range(days - 1, -1, -1):
            day_start = (now - timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
            key = day_start.isoformat()
            result.append(
                {
                    "start": key,
                    "end": (day_start + timedelta(days=1)).isoformat(),
                    "sessions": counts.get(key, 0),
                }
            )
    return result


def sessions_per_day(conn: Connection, days: int = 7) -> List[Dict[str, Any]]:
    series = sessions_time_series(conn, days=days, granularity="day")
    return [
        {
            "day": datetime.fromisoformat(entry["start"]).date().isoformat(),
            "sessions": entry["sessions"],
        }
        for entry in series
    ]
