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


@pytest.mark.parametrize("auto_fetch", ["0"])
def test_location_details_endpoint(tmp_path, monkeypatch, auto_fetch, db_url):
    locations = tmp_path / "locations.json"
    locations.write_text(
        json.dumps({"locations": [{"id": "L1", "latitude": 41.0, "longitude": 2.0}]}),
        encoding="utf-8",
    )

    monkeypatch.setenv("ENDOLLA_DB_URL", db_url)
    monkeypatch.setenv("ENDOLLA_AUTO_FETCH", auto_fetch)
    monkeypatch.setenv("ENDOLLA_LOCATIONS_FILE", str(locations))

    _seed_database(db_url)

    module = importlib.import_module("endolla_watcher.api")
    api = importlib.reload(module)

    with TestClient(api.app) as client:
        response = client.get("/api/locations/L1")
        assert response.status_code == 200
        payload = response.json()
        assert payload["location_id"] == "L1"
        assert payload["coordinates"]["lat"] == 41.0
        assert payload["summary"]["week"]["availability_ratio"] >= 0
        assert len(payload["usage_day"]["timeline"]) == 24
        assert len(payload["usage_week"]["timeline"]) == 7


def test_nearby_endpoint(tmp_path, monkeypatch, db_url):
    locations = tmp_path / "locations.json"
    locations.write_text(
        json.dumps(
            {
                "locations": [
                    {
                        "id": "L1",
                        "latitude": 41.0,
                        "longitude": 2.0,
                        "address": "Plaça 1",
                        "stations": [
                            {
                                "id": "S1",
                                "ports": [
                                    {"id": "P1", "power_kw": 22},
                                    {"id": "P2", "power_kw": 7.2, "notes": "MOTORCYCLE_ONLY"},
                                ],
                            }
                        ],
                    },
                    {
                        "id": "L2",
                        "latitude": 41.01,
                        "longitude": 2.01,
                        "stations": [
                            {
                                "id": "S2",
                                "ports": [
                                    {"id": "P1", "power_kw": 3.6, "notes": "MOTORCYCLE_ONLY"},
                                ],
                            }
                        ],
                    },
                    {"id": "L3", "latitude": 41.2, "longitude": 2.2},
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("ENDOLLA_DB_URL", db_url)
    monkeypatch.setenv("ENDOLLA_AUTO_FETCH", "0")
    monkeypatch.setenv("ENDOLLA_LOCATIONS_FILE", str(locations))

    _seed_database(db_url)

    conn = storage.connect(db_url)
    try:
        now = datetime.now(timezone.utc)
        storage.save_snapshot(
            conn,
            [
                {
                    "location_id": "L2",
                    "station_id": "S2",
                    "port_id": "P2",
                    "status": "AVAILABLE",
                    "last_updated": now.isoformat(),
                },
                {
                    "location_id": "L2",
                    "station_id": "S2",
                    "port_id": "P3",
                    "status": "OUT_OF_SERVICE",
                    "last_updated": now.isoformat(),
                },
            ],
            ts=now,
        )
    finally:
        conn.close()

    module = importlib.import_module("endolla_watcher.api")
    api = importlib.reload(module)

    with TestClient(api.app) as client:
        response = client.get("/api/nearby", params={"lat": 41.0, "lon": 2.0, "limit": 2})
        assert response.status_code == 200
        payload = response.json()
        assert payload["limit"] == 2
        assert payload["coordinates"] == {"lat": 41.0, "lon": 2.0}
        assert [entry["location_id"] for entry in payload["locations"]] == ["L1"]
        first = payload["locations"][0]
        assert any(port["status"] == "IN_USE" for port in first["ports"])
        assert first["charger_type"] == "both"
        assert first["max_power_kw"] == 22.0

        response_all = client.get(
            "/api/nearby",
            params={"lat": 41.0, "lon": 2.0, "limit": 2, "include_motorcycle": True},
        )
        assert response_all.status_code == 200
        payload_all = response_all.json()
        assert [entry["location_id"] for entry in payload_all["locations"]] == ["L1", "L2"]
        assert payload_all["locations"][0]["distance_m"] <= payload_all["locations"][1]["distance_m"]
        second = payload_all["locations"][1]
        assert second["status_counts"]["AVAILABLE"] == 1
        assert second["port_count"] == 2
        assert second["charger_type"] == "motorcycle"
        assert second["max_power_kw"] == 3.6


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
        return True

    monkeypatch.setattr(api, "_build_dashboard", fake_build)
    monkeypatch.setattr(api, "fetch_once", fake_fetch_once)

    with TestClient(api.app) as client:
        assert calls["count"] == 2
        first = client.get("/api/dashboard")
        assert first.status_code == 200
        assert first.json() == {"calls": 2}
        assert calls["count"] == 2

        second = client.get("/api/dashboard")
        assert second.status_code == 200
        assert second.json() == {"calls": 2}
        assert calls["count"] == 2

        refresh = client.post("/api/refresh")
        assert refresh.status_code == 202
        assert calls["count"] == 4

        third = client.get("/api/dashboard")
        assert third.status_code == 200
        assert third.json() == {"calls": 4}
        assert calls["count"] == 4


def test_refresh_without_changes_keeps_cached_dashboard(monkeypatch, tmp_path, db_url):
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
        return False

    monkeypatch.setattr(api, "_build_dashboard", fake_build)
    monkeypatch.setattr(api, "fetch_once", fake_fetch_once)

    with TestClient(api.app) as client:
        assert calls["count"] == 2
        first = client.get("/api/dashboard")
        assert first.status_code == 200
        assert first.json() == {"calls": 2}
        assert calls["count"] == 2

        refresh = client.post("/api/refresh")
        assert refresh.status_code == 202
        assert calls["count"] == 2

        second = client.get("/api/dashboard")
        assert second.status_code == 200
        assert second.json() == {"calls": 2}
        assert calls["count"] == 2


def test_dashboard_cache_presets(monkeypatch, tmp_path, db_url):
    locations = tmp_path / "locations.json"
    locations.write_text(
        json.dumps({"locations": [{"id": "L1", "latitude": 41.0, "longitude": 2.0}]}),
        encoding="utf-8",
    )

    monkeypatch.setenv("ENDOLLA_DB_URL", db_url)
    monkeypatch.setenv("ENDOLLA_AUTO_FETCH", "0")
    monkeypatch.setenv("ENDOLLA_LOCATIONS_FILE", str(locations))
    monkeypatch.setenv("ENDOLLA_DASHBOARD_CACHE_TTL", "3600")
    monkeypatch.setenv("ENDOLLA_DASHBOARD_CACHE_PRESETS", "10:hour")

    module = importlib.import_module("endolla_watcher.api")
    api = importlib.reload(module)

    calls: list[tuple[int, str]] = []

    def fake_build(settings, locations_map, days, granularity):
        calls.append((days, granularity))
        return {"days": days, "granularity": granularity}

    def fake_fetch_once(db_url_value, dataset_file):
        return True

    monkeypatch.setattr(api, "_build_dashboard", fake_build)
    monkeypatch.setattr(api, "fetch_once", fake_fetch_once)

    with TestClient(api.app) as client:
        assert calls == [(10, "hour"), (5, "hour"), (5, "day")]
        first = client.get("/api/dashboard", params={"days": 10, "granularity": "hour"})
        assert first.status_code == 200
        assert first.json() == {"days": 10, "granularity": "hour"}
        assert calls == [(10, "hour"), (5, "hour"), (5, "day")]
