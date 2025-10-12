from datetime import datetime, timedelta, timezone

from endolla_watcher import storage


def _record(conn, ts, status):
    storage.save_snapshot(
        conn,
        [
            {
                "location_id": "L1",
                "station_id": "S1",
                "port_id": "P1",
                "status": status,
                "last_updated": ts.isoformat(),
            }
        ],
        ts=ts,
    )


def test_location_usage_summary(conn):
    now = datetime.now(timezone.utc)
    timeline = [
        (now - timedelta(days=3), "AVAILABLE"),
        (now - timedelta(days=2, hours=20), "IN_USE"),
        (now - timedelta(days=2, hours=18), "AVAILABLE"),
        (now - timedelta(hours=12), "IN_USE"),
        (now - timedelta(hours=11, minutes=30), "AVAILABLE"),
        (now - timedelta(hours=2), "IN_USE"),
        (now - timedelta(hours=1), "AVAILABLE"),
    ]
    for ts, status in timeline:
        _record(conn, ts, status)

    details = storage.location_usage(conn, "L1", now=now)
    assert details is not None
    assert details["location_id"] == "L1"
    assert details["station_count"] == 1
    assert details["port_count"] == 1
    assert len(details["usage_day"]["timeline"]) == 24
    assert len(details["usage_week"]["timeline"]) == 7
    assert details["summary"]["day"]["occupation_utilization_pct"] > 0
    assert details["summary"]["week"]["availability_ratio"] >= 0


def test_location_usage_unknown_location(conn):
    now = datetime.now(timezone.utc)
    result = storage.location_usage(conn, "does-not-exist", now=now)
    assert result is None
