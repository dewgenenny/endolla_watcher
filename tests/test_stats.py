import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import endolla_watcher.storage as storage


def test_average_session_last_day():
    conn = storage.connect(Path(":memory:"))
    now = datetime.now(timezone.utc)

    old_start = now - timedelta(days=2)
    old_end = old_start + timedelta(minutes=30)
    new_start = now - timedelta(hours=2)
    new_end = new_start + timedelta(hours=1)

    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "IN_USE", "last_updated": old_start.isoformat()}],
        ts=old_start,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": old_end.isoformat()}],
        ts=old_end,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "IN_USE", "last_updated": new_start.isoformat()}],
        ts=new_start,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": new_end.isoformat()}],
        ts=new_end,
    )

    stats = storage.stats_from_db(conn)
    assert stats["avg_session_min"] == 60
    assert stats["short_sessions"] == 0
    assert stats["charges_today"] == 1
    conn.close()


def test_count_short_sessions():
    conn = storage.connect(Path(":memory:"))
    now = datetime.now(timezone.utc)

    start = now - timedelta(hours=1)
    end = start + timedelta(minutes=2)

    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "IN_USE", "last_updated": start.isoformat()}],
        ts=start,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": end.isoformat()}],
        ts=end,
    )

    stats = storage.stats_from_db(conn)
    assert stats["short_sessions"] == 1
    assert stats["charges_today"] == 1
    conn.close()


def test_sessions_per_day_counts_active_sessions():
    conn = storage.connect(Path(":memory:"))
    now = datetime.now(timezone.utc)

    base = now - timedelta(hours=2)
    active = now - timedelta(hours=1)

    storage.save_snapshot(
        conn,
        [
            {
                "location_id": "L1",
                "station_id": "S1",
                "port_id": "P1",
                "status": "AVAILABLE",
                "last_updated": base.isoformat(),
            }
        ],
        ts=base,
    )
    storage.save_snapshot(
        conn,
        [
            {
                "location_id": "L1",
                "station_id": "S1",
                "port_id": "P1",
                "status": "IN_USE",
                "last_updated": active.isoformat(),
            }
        ],
        ts=active,
    )

    sessions = storage.sessions_per_day(conn, days=2)
    assert sessions[-1]["sessions"] == 1
    conn.close()
