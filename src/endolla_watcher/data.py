import json
import logging
from pathlib import Path
from typing import Any, Dict, List
import requests

_ADDRESS_KEYWORDS = (
    "address",
    "street",
    "road",
    "via",
    "carrer",
    "avenue",
    "avinguda",
    "calle",
    "numero",
    "number",
    "postal",
    "postcode",
    "zip",
    "city",
    "municip",
    "distric",
    "barri",
    "barrio",
    "neigh",
    "locality",
    "provinc",
    "pobl",
    "ciutat",
)


def _should_collect_address(key: str) -> bool:
    key_lower = str(key).lower()
    return any(keyword in key_lower for keyword in _ADDRESS_KEYWORDS)


def _collect_address_components(value: Any, components: List[str], *, allow_all: bool = False) -> None:
    if value is None:
        return
    if isinstance(value, dict):
        for key, val in value.items():
            if allow_all or _should_collect_address(key):
                _collect_address_components(val, components, allow_all=False)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_address_components(item, components, allow_all=allow_all)
        return
    if isinstance(value, (int, float)):
        text = str(value).strip()
    elif isinstance(value, str):
        text = value.strip()
    else:
        return
    if text and text not in components:
        components.append(text)


def _extract_location_address(entry: Dict[str, Any]) -> str | None:
    components: List[str] = []
    address_field = entry.get("address")
    if address_field is not None:
        _collect_address_components(
            address_field,
            components,
            allow_all=isinstance(address_field, str),
        )
    for key, value in entry.items():
        if key == "address":
            continue
        if _should_collect_address(key):
            _collect_address_components(value, components, allow_all=True)
    if components:
        return ", ".join(components)
    return None

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
            record = {"lat": float(lat), "lon": float(lon)}
            address = _extract_location_address(it)
            if address:
                record["address"] = address
            result[str(loc_id)] = record
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
