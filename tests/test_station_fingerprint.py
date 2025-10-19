from datetime import datetime, timedelta, timezone

from endolla_watcher import storage


def _record(conn, ts, status, port):
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


def test_station_fingerprint_heatmap(conn):
    reference = datetime(2025, 1, 8, 2, tzinfo=timezone.utc)
    midnight = reference.replace(hour=0, minute=0, second=0, microsecond=0)
    start = midnight - timedelta(days=7)

    # Seed baseline availability prior to the aggregation window.
    baseline = start - timedelta(hours=2)
    for port in ("P1", "P2"):
        _record(conn, baseline, "AVAILABLE", port)

    busy_start = start + timedelta(days=4, hours=10)
    busy_mid = busy_start + timedelta(hours=1)
    busy_end = busy_start + timedelta(hours=2)

    _record(conn, busy_start, "IN_USE", "P1")
    _record(conn, busy_end, "AVAILABLE", "P1")

    _record(conn, busy_mid, "IN_USE", "P2")
    _record(conn, busy_end + timedelta(hours=1), "AVAILABLE", "P2")

    fingerprint = storage.station_fingerprint(conn, "L1", "S1", reference=reference)
    assert fingerprint is not None
    assert fingerprint["location_id"] == "L1"
    assert fingerprint["station_id"] == "S1"
    assert fingerprint["port_count"] == 2
    assert len(fingerprint["cells"]) == 7 * 24

    target_start = busy_start.isoformat()
    matching = [cell for cell in fingerprint["cells"] if cell["start"] == target_start]
    assert matching, "expected fingerprint cell for busy interval"
    target_cell = matching[0]
    assert target_cell["metrics"]["occupation_utilization_pct"] > 0
    assert 0 <= target_cell["coverage_ratio"] <= 1
    assert fingerprint["busiest"], "expected busiest summary entries"
    hottest = fingerprint["busiest"][0]
    assert hottest["label"] == target_cell["label"]
    assert hottest["occupation_utilization_pct"] >= target_cell["metrics"]["occupation_utilization_pct"]

    storage.save_station_fingerprint(conn, fingerprint)
    cached = storage.latest_station_fingerprint(conn, "L1", "S1")
    assert cached is not None
    assert cached["start"] == fingerprint["start"]
    assert cached["end"] == fingerprint["end"]


def test_station_fingerprint_queue(conn):
    now = datetime(2025, 1, 8, 3, tzinfo=timezone.utc)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Ensure the station exists in the dataset.
    _record(conn, midnight - timedelta(days=1), "AVAILABLE", "P1")

    queued = storage.schedule_station_fingerprints(conn, midnight)
    assert queued >= 1

    job = storage.dequeue_station_fingerprint_job(conn, now=now)
    assert job is not None
    assert job["location_id"] == "L1"
    assert job["station_id"] == "S1"

    storage.complete_station_fingerprint_job(conn, job["id"], "completed")
    follow_up = storage.dequeue_station_fingerprint_job(conn, now=now)
    assert follow_up is None
