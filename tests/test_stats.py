from datetime import datetime, timedelta, timezone

import endolla_watcher.storage as storage


def test_average_session_last_day(conn):
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
    stats = storage.stats_from_db(conn)
    assert stats["avg_session_min"] == 60
    assert stats["short_sessions"] == 0
    assert stats["charges_today"] == 1


def test_count_short_sessions(conn):
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


def test_sessions_per_day_counts_active_sessions(conn):
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


def test_sessions_time_series_hourly(conn):
    now = datetime.now(timezone.utc)

    baseline = now - timedelta(hours=6, minutes=15)
    first_start = now - timedelta(hours=6)
    first_end = first_start + timedelta(minutes=30)
    second_start = now - timedelta(hours=1, minutes=30)
    second_end = second_start + timedelta(minutes=45)

    snapshots = [
        (baseline, "AVAILABLE"),
        (first_start, "IN_USE"),
        (first_end, "AVAILABLE"),
        (second_start, "IN_USE"),
        (second_end, "AVAILABLE"),
    ]

    for ts, status in snapshots:
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

    series = storage.sessions_time_series(conn, days=1, granularity="hour")
    assert series, "expected hourly series to include at least one bucket"

    total_sessions = sum(entry.get("sessions", 0) for entry in series)
    assert total_sessions == 2

    target_hour_key = second_start.astimezone().replace(minute=0, second=0, microsecond=0).isoformat()
    target_bucket = next((entry for entry in series if entry.get("start") == target_hour_key), None)
    assert target_bucket is not None
    assert target_bucket["sessions"] >= 1
