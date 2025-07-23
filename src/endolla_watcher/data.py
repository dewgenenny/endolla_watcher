import json
from pathlib import Path
from typing import Any, Dict, List
import requests

ENDOLLA_URL = "https://opendata.barcelona.cat/resource/endolla.json"


def fetch_data(path: Path | None = None) -> Dict[str, Any]:
    """Fetch dataset either from local file or remote endpoint."""
    if path:
        with path.open() as f:
            return json.load(f)
    resp = requests.get(ENDOLLA_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


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
    return results
