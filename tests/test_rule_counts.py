import endolla_watcher.storage as storage
from endolla_watcher.rules import Rules
from datetime import datetime, timedelta, timezone
from pathlib import Path


def test_rule_counts():
    conn = storage.connect(Path(":memory:"))
    now = datetime.now(timezone.utc)

    # Port 1 - unused
    old = now - timedelta(days=1)
    storage.save_snapshot(
        conn,
        [{"location_id": "L1", "station_id": "S1", "port_id": "P1", "status": "AVAILABLE", "last_updated": old.isoformat()}],
        ts=old,
    )

    # Port 2 - only short session
    start = now - timedelta(days=1)
    end = start + timedelta(minutes=3)
    storage.save_snapshot(
        conn,
        [{"location_id": "L2", "station_id": "S2", "port_id": "P1", "status": "IN_USE", "last_updated": start.isoformat()}],
        ts=start,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L2", "station_id": "S2", "port_id": "P1", "status": "AVAILABLE", "last_updated": end.isoformat()}],
        ts=end,
    )

    # Port 3 - unavailable for long time
    start3 = now - timedelta(hours=8)
    storage.save_snapshot(
        conn,
        [{"location_id": "L3", "station_id": "S3", "port_id": "P1", "status": "OUT_OF_ORDER", "last_updated": start3.isoformat()}],
        ts=start3,
    )
    storage.save_snapshot(
        conn,
        [{"location_id": "L3", "station_id": "S3", "port_id": "P1", "status": "OUT_OF_ORDER", "last_updated": now.isoformat()}],
        ts=now,
    )

    rules = Rules(unused_days=1, long_session_days=1, long_session_min=5, unavailable_hours=6)
    problematic, counts = storage.analyze_chargers(conn, rules, now=now)
    assert counts["unused"] == 1
    assert counts["no_long"] == 2
    assert counts["unavailable"] == 1
    assert len(problematic) == 3
    conn.close()
