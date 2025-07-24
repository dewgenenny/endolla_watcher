import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import endolla_watcher.storage as storage


def test_charger_sessions():
    conn = storage.connect(Path(":memory:"))
    now = datetime.now(timezone.utc)

    start1 = now - timedelta(minutes=30)
    end1 = start1 + timedelta(minutes=10)
    start2 = now - timedelta(minutes=10)
    end2 = now

    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "IN_USE", "last_updated": start1.isoformat()}],
        ts=start1,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": end1.isoformat()}],
        ts=end1,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "IN_USE", "last_updated": start2.isoformat()}],
        ts=start2,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": end2.isoformat()}],
        ts=end2,
    )

    result = storage.charger_sessions(conn, "L1", "S1", limit=5)
    assert "P1" in result
    sessions = result["P1"]
    assert len(sessions) == 2
    assert sessions[0]["duration"] == 10
    assert sessions[1]["duration"] == 10
    conn.close()
