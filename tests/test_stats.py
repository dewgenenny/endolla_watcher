from datetime import datetime, timedelta, timezone

import pytest

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


def test_mttr_station(conn):
    now = datetime.now(timezone.utc)

    base = now - timedelta(hours=3)

    def _snapshot(ts: datetime, port: str, status: str) -> None:
        storage.save_snapshot(
            conn,
            [
                {
                    "location_id": "L1",
                    "station_id": "S1",
                    "port_id": port,
                    "status": status,
                    "last_updated": ts.isoformat(),
                }
            ],
            ts=ts,
        )

    _snapshot(base, "P1", "AVAILABLE")
    _snapshot(base, "P2", "AVAILABLE")

    first_outage_start = base + timedelta(minutes=30)
    _snapshot(first_outage_start, "P1", "OUT_OF_ORDER")
    _snapshot(first_outage_start, "P2", "OUT_OF_ORDER")

    first_outage_end = first_outage_start + timedelta(minutes=30)
    _snapshot(first_outage_end, "P1", "AVAILABLE")
    _snapshot(first_outage_end, "P2", "AVAILABLE")

    second_outage_start = first_outage_end + timedelta(minutes=30)
    _snapshot(second_outage_start, "P1", "UNAVAILABLE")
    _snapshot(second_outage_start, "P2", "UNAVAILABLE")

    second_outage_end = second_outage_start + timedelta(minutes=60)
    _snapshot(second_outage_end, "P1", "AVAILABLE")
    _snapshot(second_outage_end, "P2", "AVAILABLE")

    partial_start = second_outage_end + timedelta(minutes=10)
    _snapshot(partial_start, "P1", "OUT_OF_ORDER")

    partial_end = partial_start + timedelta(minutes=20)
    _snapshot(partial_end, "P1", "AVAILABLE")

    stats = storage.stats_from_db(conn, now=partial_end + timedelta(minutes=10))

    assert stats["mttr_minutes"] == pytest.approx(45.0)


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


def test_utilization_metrics(conn):
    now = datetime.now(timezone.utc)

    def _record(port: str, ts: datetime, status: str) -> None:
        storage.save_snapshot(
            conn,
            [
                {
                    "location_id": "L1",
                    "station_id": "S1",
                    "port_id": port,
                    "status": status,
                    "last_updated": ts.isoformat(),
                }
            ],
            ts=ts,
        )

    # Port P1 timeline (2 hours monitored)
    _record("P1", now - timedelta(hours=2), "AVAILABLE")
    _record("P1", now - timedelta(hours=1, minutes=30), "IN_USE")
    _record("P1", now - timedelta(minutes=30), "AVAILABLE")
    _record("P1", now - timedelta(minutes=10), "OUT_OF_ORDER")
    _record("P1", now - timedelta(minutes=5), "AVAILABLE")

    # Port P2 timeline (3 hours monitored)
    _record("P2", now - timedelta(hours=3), "AVAILABLE")
    _record("P2", now - timedelta(hours=2), "IN_USE")
    _record("P2", now - timedelta(hours=1), "AVAILABLE")
    _record("P2", now - timedelta(minutes=30), "UNAVAILABLE")
    _record("P2", now - timedelta(minutes=10), "AVAILABLE")

    stats = storage.stats_from_db(conn, now=now)
    util = stats["utilization"]

    p1 = next(item for item in util["ports"] if item["port_id"] == "P1")
    assert p1["sessions"] == 1
    assert p1["monitored_hours"] == pytest.approx(2.0)
    assert p1["session_count_per_day"] == pytest.approx(12.0, rel=1e-6)
    assert p1["session_count_per_hour"] == pytest.approx(0.5, rel=1e-6)
    assert p1["occupation_utilization_pct"] == pytest.approx(52.173913, rel=1e-6)
    assert p1["active_charging_utilization_pct"] == pytest.approx(52.173913, rel=1e-6)
    assert p1["availability_ratio"] == pytest.approx(0.958333, rel=1e-6)

    station = next(item for item in util["stations"] if item["station_id"] == "S1")
    assert station["port_count"] == 2
    assert station["session_count_per_hour"] == pytest.approx(0.4, rel=1e-6)
    assert station["occupation_utilization_pct"] == pytest.approx(43.636364, rel=1e-6)
    assert station["availability_ratio"] == pytest.approx(0.916666, rel=1e-6)

    location = util["locations"][0]
    assert location["location_id"] == "L1"
    assert location["station_count"] == 1
    assert location["port_count"] == 2
    assert location["availability_ratio"] == pytest.approx(0.916666, rel=1e-6)

    network = util["network"]
    assert network["port_count"] == 2
    assert network["station_count"] == 1
    assert network["location_count"] == 1
    assert network["session_count_per_day"] == pytest.approx(9.6, rel=1e-6)
