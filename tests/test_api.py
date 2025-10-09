import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from endolla_watcher import storage


def _seed_database(db_path):
    conn = storage.connect(db_path)
    try:
        base = datetime.now(timezone.utc) - timedelta(hours=2)
        snapshots = [
            (
                [
                    {
                        "location_id": "L1",
                        "station_id": "S1",
                        "port_id": "P1",
                        "status": "AVAILABLE",
                        "last_updated": base.isoformat(),
                    }
                ],
                base,
            ),
            (
                [
                    {
                        "location_id": "L1",
                        "station_id": "S1",
                        "port_id": "P1",
                        "status": "IN_USE",
                        "last_updated": (base + timedelta(minutes=10)).isoformat(),
                    }
                ],
                base + timedelta(minutes=10),
            ),
            (
                [
                    {
                        "location_id": "L1",
                        "station_id": "S1",
                        "port_id": "P1",
                        "status": "AVAILABLE",
                        "last_updated": (base + timedelta(minutes=70)).isoformat(),
                    }
                ],
                base + timedelta(minutes=70),
            ),
        ]
        for records, ts in snapshots:
            storage.save_snapshot(conn, records, ts=ts)
    finally:
        conn.close()


@pytest.mark.parametrize("auto_fetch", ["0"])
def test_dashboard_endpoint(tmp_path, monkeypatch, auto_fetch):
    db_path = tmp_path / "endolla.db"
    locations = tmp_path / "locations.json"
    locations.write_text(
        json.dumps({"locations": [{"id": "L1", "latitude": 41.0, "longitude": 2.0}]}),
        encoding="utf-8",
    )

    monkeypatch.setenv("ENDOLLA_DB_PATH", str(db_path))
    monkeypatch.setenv("ENDOLLA_AUTO_FETCH", auto_fetch)
    monkeypatch.setenv("ENDOLLA_LOCATIONS_FILE", str(locations))

    _seed_database(db_path)

    from endolla_watcher.api import app  # Import after environment variables are set

    with TestClient(app) as client:
        response = client.get("/api/dashboard")
        assert response.status_code == 200
        payload = response.json()
        assert payload["stats"]["chargers"] >= 1
        assert payload["rule_counts"]["unused"] >= 0
        assert payload["locations"]["L1"]["lat"] == 41.0
        assert payload["last_fetch"] is None
