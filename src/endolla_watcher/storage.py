"""Persistence helpers backed by a MySQL database."""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Sequence, Tuple
from urllib.parse import urlparse, unquote

import pymysql
from pymysql.connections import Connection

from . import stats as stats_mod
from .rules import Rules

logger = logging.getLogger(__name__)

# Delete records older than this many days
MAX_DATA_AGE_DAYS = 28

UNAVAILABLE_STATUSES = {"OUT_OF_ORDER", "UNAVAILABLE"}

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


def prune_old_data(conn: Connection, max_age_days: int = MAX_DATA_AGE_DAYS) -> None:
    cutoff = datetime.now().astimezone() - timedelta(days=max_age_days)
    cutoff_iso = cutoff.isoformat()
    with _with_cursor(conn) as cur:
        cur.execute("DELETE FROM port_status WHERE ts < %s", (cutoff_iso,))
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
) -> None:
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


def _session_durations(statuses: List[Tuple[datetime, str]]) -> List[float]:
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
        sessions.append((datetime.now().astimezone() - start).total_seconds() / 60)
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
                    for d in _session_durations([(ts, st) for ts, st in events if ts >= since_long])
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


def _all_history(conn: Connection) -> Dict[PortKey, List[Tuple[datetime, str]]]:
    history: Dict[PortKey, List[Tuple[datetime, str]]] = {}
    with _with_cursor(conn) as cur:
        cur.execute(
            "SELECT location_id, station_id, port_id, ts, status FROM port_status ORDER BY location_id, station_id, port_id, ts"
        )
        for loc, sta, port, ts_str, status in cur.fetchall():
            history.setdefault((loc, sta, port), []).append((datetime.fromisoformat(ts_str), status))
    return history


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


def stats_from_db(conn: Connection) -> Dict[str, float]:
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

    today = datetime.now().astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
    charges_today = 0
    for events in history.values():
        for start, _, _ in _session_records(events):
            if start >= today:
                charges_today += 1
    stats["charges_today"] = charges_today
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
    since = datetime.now().astimezone() - timedelta(days=MAX_DATA_AGE_DAYS)
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
