import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set
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


_MOTORCYCLE_KEYWORDS: Sequence[str] = (
    "motorcycle",
    "motorbike",
    "motocic",
    "moped",
    "scooter",
    "moto",
)

_CAR_KEYWORDS: Sequence[str] = (
    "car",
    "cotxe",
    "coche",
    "automobil",
    "vehicle",
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


def fetch_locations(path: Path | None = None) -> Dict[str, Dict[str, Any]]:
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


def parse_locations(data: Any) -> Dict[str, Dict[str, Any]]:
    """Return a mapping of location_id -> metadata including coordinates."""
    items: List[Dict[str, Any]]
    if isinstance(data, dict):
        items = data.get("data") or data.get("locations") or data.get("records") or []
    elif isinstance(data, list):
        items = data
    else:
        items = []
    result: Dict[str, Dict[str, Any]] = {}
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
            record: Dict[str, Any] = {"lat": float(lat), "lon": float(lon)}
            address = _extract_location_address(it)
            if address:
                record["address"] = address

            vehicle_types = _summarise_vehicle_types(it)
            if vehicle_types:
                record["charger_type"] = _summarise_location_vehicle_type(vehicle_types)

            max_power = _max_port_power_kw(it)
            if max_power is not None:
                record["max_power_kw"] = max_power
            result[str(loc_id)] = record
        except (TypeError, ValueError):
            logger.debug("Skipping invalid location entry: %s", it)
    logger.debug("Parsed %d location coordinates", len(result))
    return result


def _iter_text_values(entry: Dict[str, Any], keys: Iterable[str]) -> Iterable[str]:
    for key in keys:
        value = entry.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            yield value
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                if item is None:
                    continue
                yield str(item)


def _classify_vehicle_mentions(texts: Iterable[str]) -> Set[str]:
    mentions: Set[str] = set()
    for text in texts:
        cleaned = text.strip().lower()
        if not cleaned:
            continue
        if any(keyword in cleaned for keyword in _MOTORCYCLE_KEYWORDS):
            mentions.add("motorcycle")
        if any(keyword in cleaned for keyword in _CAR_KEYWORDS):
            mentions.add("car")
    return mentions


def _summarise_vehicle_types(location: Dict[str, Any]) -> Set[str]:
    vehicle_types: Set[str] = set()
    stations = location.get("stations")
    if not isinstance(stations, list):
        return vehicle_types

    for station in stations:
        if not isinstance(station, dict):
            continue
        ports = station.get("ports")
        if not isinstance(ports, list):
            continue
        for port in ports:
            if not isinstance(port, dict):
                continue
            texts = list(_iter_text_values(port, ("notes", "label", "description")))
            mentions = _classify_vehicle_mentions(texts)
            if not mentions:
                # Default to car chargers if no explicit classification is provided.
                mentions = {"car"}
            vehicle_types.update(mentions)
    return vehicle_types


def _summarise_location_vehicle_type(vehicle_types: Set[str]) -> str:
    if not vehicle_types:
        return "unknown"
    if vehicle_types == {"car"}:
        return "car"
    if vehicle_types == {"motorcycle"}:
        return "motorcycle"
    return "both"


def _extract_power_kw(port: Dict[str, Any]) -> float | None:
    power_candidates = (
        port.get("power_kw"),
        port.get("powerKW"),
        port.get("power"),
        port.get("POWER_KW"),
    )
    for raw_value in power_candidates:
        if raw_value is None:
            continue
        if isinstance(raw_value, (int, float)):
            value = float(raw_value)
        elif isinstance(raw_value, str):
            cleaned = raw_value.strip().replace(",", ".")
            if not cleaned:
                continue
            try:
                value = float(cleaned)
            except ValueError:
                continue
        else:
            continue
        if value <= 0:
            continue
        return value
    return None


def _max_port_power_kw(location: Dict[str, Any]) -> float | None:
    stations = location.get("stations")
    if not isinstance(stations, list):
        return None

    max_power: float | None = None
    for station in stations:
        if not isinstance(station, dict):
            continue
        ports = station.get("ports")
        if not isinstance(ports, list):
            continue
        for port in ports:
            if not isinstance(port, dict):
                continue
            value = _extract_power_kw(port)
            if value is None:
                continue
            if max_power is None or value > max_power:
                max_power = value
    return max_power


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
