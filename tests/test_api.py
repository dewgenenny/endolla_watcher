import importlib
import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from endolla_watcher import storage


def _seed_database(db_url: str) -> None:
    conn = storage.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM port_status")
        conn.commit()
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
def test_dashboard_endpoint(tmp_path, monkeypatch, auto_fetch, db_url):
    locations = tmp_path / "locations.json"
    locations.write_text(
        json.dumps({"locations": [{"id": "L1", "latitude": 41.0, "longitude": 2.0}]}),
        encoding="utf-8",
    )

    monkeypatch.setenv("ENDOLLA_DB_URL", db_url)
    monkeypatch.setenv("ENDOLLA_AUTO_FETCH", auto_fetch)
    monkeypatch.setenv("ENDOLLA_LOCATIONS_FILE", str(locations))

    _seed_database(db_url)

    from endolla_watcher.api import app  # Import after environment variables are set

    with TestClient(app) as client:
        response = client.get("/api/dashboard")
        assert response.status_code == 200
        payload = response.json()
        assert payload["stats"]["chargers"] >= 1
        assert payload["rule_counts"]["unused"] >= 0
        assert payload["locations"]["L1"]["lat"] == 41.0
        assert payload["last_fetch"] is None


def test_dashboard_cache(monkeypatch, tmp_path, db_url):
    locations = tmp_path / "locations.json"
    locations.write_text(
        json.dumps({"locations": [{"id": "L1", "latitude": 41.0, "longitude": 2.0}]}),
        encoding="utf-8",
    )

    monkeypatch.setenv("ENDOLLA_DB_URL", db_url)
    monkeypatch.setenv("ENDOLLA_AUTO_FETCH", "0")
    monkeypatch.setenv("ENDOLLA_LOCATIONS_FILE", str(locations))
    monkeypatch.setenv("ENDOLLA_DASHBOARD_CACHE_TTL", "3600")

    module = importlib.import_module("endolla_watcher.api")
    api = importlib.reload(module)

    calls = {"count": 0}

    def fake_build(settings, locations_map, days, granularity):
        calls["count"] += 1
        return {"calls": calls["count"]}

    def fake_fetch_once(db_url_value, dataset_file):
        return None

    monkeypatch.setattr(api, "_build_dashboard", fake_build)
    monkeypatch.setattr(api, "fetch_once", fake_fetch_once)

    with TestClient(api.app) as client:
        first = client.get("/api/dashboard")
        assert first.status_code == 200
        assert first.json() == {"calls": 1}

        second = client.get("/api/dashboard")
        assert second.status_code == 200
        assert second.json() == {"calls": 1}

        refresh = client.post("/api/refresh")
        assert refresh.status_code == 202

        third = client.get("/api/dashboard")
        assert third.status_code == 200
        assert third.json() == {"calls": 2}
