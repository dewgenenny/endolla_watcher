import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import endolla_watcher.storage as storage


def test_count_unused_chargers():
    conn = storage.connect(Path(":memory:"))
    now = datetime.now(timezone.utc)

    start_a = now - timedelta(days=2, hours=12)
    end_a = start_a + timedelta(hours=1)
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "IN_USE", "last_updated": start_a.isoformat()}],
        ts=start_a,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": end_a.isoformat()}],
        ts=end_a,
    )

    start_b = now - timedelta(days=1, hours=1)
    end_b = start_b + timedelta(hours=1)
    storage.save_snapshot(
        conn,
        [{"location_id": "L2", "station_id": "S2", "port_id": "P1", "status": "IN_USE", "last_updated": start_b.isoformat()}],
        ts=start_b,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L2", "station_id": "S2", "port_id": "P1", "status": "AVAILABLE", "last_updated": end_b.isoformat()}],
        ts=end_b,
    )

    start_c = now - timedelta(hours=10)
    end_c = start_c + timedelta(hours=1)
    storage.save_snapshot(
        conn,
        [{"location_id": "L3", "station_id": "S3", "port_id": "P1", "status": "IN_USE", "last_updated": start_c.isoformat()}],
        ts=start_c,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L3", "station_id": "S3", "port_id": "P1", "status": "AVAILABLE", "last_updated": end_c.isoformat()}],
        ts=end_c,
    )

    count_1 = storage._count_unused_chargers(conn, 1, now)
    count_2 = storage._count_unused_chargers(conn, 2, now)
    assert count_1 == 2
    assert count_2 == 1
    conn.close()
