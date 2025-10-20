import os
from datetime import datetime, timedelta, timezone

import pytest

TEST_DB_URL = os.getenv("ENDOLLA_TEST_DB_URL")
if not TEST_DB_URL:
    pytest.skip("ENDOLLA_TEST_DB_URL not configured", allow_module_level=True)

os.environ.setdefault("ENDOLLA_DB_URL", TEST_DB_URL)

from endolla_watcher import storage
from endolla_watcher.api import (
    Settings,
    _fingerprint_reference_for,
    _generate_missing_fingerprints,
)
from endolla_watcher.rules import Rules


def _make_settings(db_url: str) -> Settings:
    return Settings(
        db_url=db_url,
        dataset_file=None,
        fetch_interval=300,
        auto_fetch=False,
        location_file=None,
        rules=Rules(),
        cors_origins=["*"],
        debug=False,
        dashboard_cache_ttl=60,
        dashboard_cache_presets=[(5, "hour"), (5, "day")],
    )


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
    start = midnight - timedelta(days=28)

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
    span = datetime.fromisoformat(fingerprint["end"]) - datetime.fromisoformat(
        fingerprint["start"]
    )
    assert len(fingerprint["cells"]) == int(span.total_seconds() // 3600)

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


def test_station_fingerprint_rolls_forward(conn):
    reference = datetime(2025, 1, 8, 2, tzinfo=timezone.utc)
    midnight = reference.replace(hour=0, minute=0, second=0, microsecond=0)
    start = midnight - timedelta(days=28)

    baseline = start - timedelta(hours=2)
    for port in ("P1", "P2"):
        _record(conn, baseline, "AVAILABLE", port)

    busy_start = start + timedelta(days=5, hours=9)
    busy_mid = busy_start + timedelta(hours=1)
    busy_end = busy_start + timedelta(hours=2)

    _record(conn, busy_start, "IN_USE", "P1")
    _record(conn, busy_end, "AVAILABLE", "P1")

    _record(conn, busy_mid, "IN_USE", "P2")
    _record(conn, busy_end + timedelta(hours=1), "AVAILABLE", "P2")

    fingerprint = storage.station_fingerprint(conn, "L1", "S1", reference=reference)
    assert fingerprint is not None
    storage.save_station_fingerprint(conn, fingerprint)

    next_reference = reference + timedelta(days=1)
    next_midnight = midnight + timedelta(days=1)
    next_busy_start = next_midnight + timedelta(hours=9)
    next_busy_mid = next_busy_start + timedelta(hours=1)
    next_busy_end = next_busy_start + timedelta(hours=2)

    _record(conn, next_busy_start - timedelta(hours=1), "AVAILABLE", "P1")
    _record(conn, next_busy_start, "IN_USE", "P1")
    _record(conn, next_busy_end, "AVAILABLE", "P1")

    _record(conn, next_busy_mid, "IN_USE", "P2")
    _record(conn, next_busy_end + timedelta(hours=1), "AVAILABLE", "P2")

    updated = storage.station_fingerprint(
        conn, "L1", "S1", reference=next_reference
    )
    assert updated is not None

    first_start = datetime.fromisoformat(fingerprint["start"])
    first_end = datetime.fromisoformat(fingerprint["end"])
    second_start = datetime.fromisoformat(updated["start"])
    second_end = datetime.fromisoformat(updated["end"])

    assert second_start == first_start + timedelta(days=1)
    assert second_end == first_end + timedelta(days=1)

    assert any(cell["start"] == busy_start.isoformat() for cell in updated["cells"])
    assert any(
        cell["start"] == next_busy_start.isoformat() for cell in updated["cells"]
    )


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


def test_generate_missing_fingerprints_on_startup(conn, db_url):
    now = datetime(2025, 1, 8, 5, tzinfo=timezone.utc)
    reference = _fingerprint_reference_for(now)
    settings = _make_settings(db_url)

    baseline = reference - timedelta(days=1)
    _record(conn, baseline, "AVAILABLE", "P1")

    missing_before = storage.stations_missing_fingerprints(conn)
    assert ("L1", "S1") in missing_before

    created = _generate_missing_fingerprints(settings, reference)
    assert created == 1

    cached = storage.latest_station_fingerprint(conn, "L1", "S1")
    assert cached is not None

    missing_after = storage.stations_missing_fingerprints(conn)
    assert ("L1", "S1") not in missing_after
