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


def test_parse_locations_includes_address_components():
    sample = [
        {
            "id": "L3",
            "latitude": 41.1,
            "longitude": 2.1,
            "address": {
                "street_name": "Carrer de Mallorca",
                "street_number": "401",
                "postal_code": "08013",
                "city": "Barcelona",
            },
        }
    ]
    result = data.parse_locations(sample)
    assert result["L3"]["address"] == "Carrer de Mallorca, 401, 08013, Barcelona"
