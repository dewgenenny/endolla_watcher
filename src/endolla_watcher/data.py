import json
import logging
from pathlib import Path
from typing import Any, Dict, List
import requests

logger = logging.getLogger(__name__)

# Public download endpoint for the Endolla dataset
ENDOLLA_URL = (
    "https://opendata-ajuntament.barcelona.cat/data/dataset/"
    "a2bd4c83-d024-4d78-8436-040ef996cf7f/resource/"
    "ada4c823-9566-477d-9362-7b15e7d38189/download"
)

# Public download endpoint for station location information
LOCATION_URL = (
    "https://opendata-ajuntament.barcelona.cat/data/dataset/"
    "8cdafa08-d378-4bf1-aad4-fafffe815940/resource/"
    "9febc26f-d6a7-45f2-8f73-f529ba4da930/download"
)


def fetch_data(path: Path | None = None) -> Dict[str, Any]:
    """Fetch dataset either from local file or remote endpoint."""
    if path:
        logger.debug("Loading dataset from %s", path)
        with path.open() as f:
            data = json.load(f)
        logger.debug("Loaded %d bytes from file", len(json.dumps(data)))
        return data
    logger.debug("Fetching dataset from %s", ENDOLLA_URL)
    resp = requests.get(ENDOLLA_URL, timeout=30)
    resp.raise_for_status()
    logger.debug("Fetched %d bytes from remote", len(resp.content))
    return resp.json()


def fetch_locations(path: Path | None = None) -> Dict[str, Dict[str, float]]:
    """Fetch charger location data from file or remote."""
    if path:
        logger.debug("Loading location data from %s", path)
        with path.open() as f:
            data = json.load(f)
    else:
        logger.debug("Fetching location data from %s", LOCATION_URL)
        resp = requests.get(LOCATION_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    return parse_locations(data)


def parse_locations(data: Any) -> Dict[str, Dict[str, float]]:
    """Return a mapping of location_id -> {'lat': float, 'lon': float}."""
    items: List[Dict[str, Any]]
    if isinstance(data, dict):
        items = data.get("data") or data.get("locations") or data.get("records") or []
    elif isinstance(data, list):
        items = data
    else:
        items = []
    result: Dict[str, Dict[str, float]] = {}
    for it in items:
        loc_id = (
            it.get("location_id")
            or it.get("id")
            or it.get("ID")
            or it.get("codi")
            or it.get("CODI")
        )
        lat = (
            it.get("latitude")
            or it.get("lat")
            or it.get("LATITUD")
            or it.get("latitud")
        )
        lon = (
            it.get("longitude")
            or it.get("lon")
            or it.get("LONGITUD")
            or it.get("longitud")
        )
        if lat is None or lon is None:
            coords = it.get("coordinates") or {}
            lat = lat or coords.get("latitude") or coords.get("lat")
            lon = lon or coords.get("longitude") or coords.get("lon")
        if lat is None or lon is None:
            address = it.get("address") or {}
            coords = address.get("coordinates") or {}
            lat = lat or coords.get("latitude") or coords.get("lat")
            lon = lon or coords.get("longitude") or coords.get("lon")
        if loc_id is None or lat is None or lon is None:
            continue
        try:
            result[str(loc_id)] = {"lat": float(lat), "lon": float(lon)}
        except (TypeError, ValueError):
            logger.debug("Skipping invalid location entry: %s", it)
    logger.debug("Parsed %d location coordinates", len(result))
    return result


def parse_usage(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten dataset into a list of port entries with usage info."""
    results = []
    for loc in data.get("locations", []):
        for station in loc.get("stations", []):
            for port in station.get("ports", []):
                item = {
                    "location_id": loc.get("id"),
                    "station_id": station.get("id"),
                    "port_id": port.get("id"),
                    "status": port.get("port_status", [{}])[0].get("status"),
                    "last_updated": port.get("last_updated"),
                }
                # Optional session data
                if "sessions" in port:
                    item["sessions"] = port["sessions"]
                results.append(item)
    logger.debug("Parsed %d port records", len(results))
    return results
