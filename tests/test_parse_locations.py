import endolla_watcher.data as data


def test_parse_locations_nested_coords():
    sample = {
        "locations": [
            {"id": "L1", "coordinates": {"latitude": 1.1, "longitude": 2.2}},
            {"id": "L2", "address": {"coordinates": {"latitude": 3.3, "longitude": 4.4}}},
        ]
    }
    result = data.parse_locations(sample)
    assert result == {
        "L1": {"lat": 1.1, "lon": 2.2},
        "L2": {"lat": 3.3, "lon": 4.4},
    }
