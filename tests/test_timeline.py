import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import endolla_watcher.storage as storage


def test_timeline_stats():
    conn = storage.connect(Path(":memory:"))
    now = datetime.now(timezone.utc)
    day1 = now - timedelta(days=1)
    day2 = now

    # Morning snapshots
    storage.save_snapshot(
        conn,
        [
            {"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": day1.isoformat()},
            {"location_id": "L2", "station_id": "S2", "port_id": "P1", "status": "AVAILABLE", "last_updated": day1.isoformat()},
        ],
        ts=day1,
    )
    # Evening snapshots (latest for day1)
    storage.save_snapshot(
        conn,
        [
            {"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "IN_USE", "last_updated": day1.isoformat()},
            {"location_id": "L2", "station_id": "S2", "port_id": "P1", "status": "OUT_OF_ORDER", "last_updated": day1.isoformat()},
        ],
        ts=day1 + timedelta(hours=12),
    )
    # Day 2 morning snapshots
    storage.save_snapshot(
        conn,
        [
            {"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": day2.isoformat()},
            {"location_id": "L2", "station_id": "S2", "port_id": "P1", "status": "AVAILABLE", "last_updated": day2.isoformat()},
        ],
        ts=day2,
    )
    # Day 2 evening snapshots (latest for day2)
    storage.save_snapshot(
        conn,
        [
            {"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": day2.isoformat()},
            {"location_id": "L2", "station_id": "S2", "port_id": "P1", "status": "IN_USE", "last_updated": day2.isoformat()},
        ],
        ts=day2 + timedelta(hours=12),
    )

    result = storage.timeline_stats(conn)
    assert len(result) == 4

    first = result[1]
    second = result[3]

    assert first["chargers"] == 2
    assert first["charging"] == 1
    assert first["unavailable"] == 1

    assert second["chargers"] == 2
    assert second["charging"] == 1
    assert second["unavailable"] == 0

    conn.close()
