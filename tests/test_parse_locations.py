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


def test_parse_locations_includes_vehicle_types_and_power():
    sample = {
        "locations": [
            {
                "id": "L4",
                "latitude": 41.3,
                "longitude": 2.2,
                "stations": [
                    {
                        "id": "S1",
                        "ports": [
                            {"id": "P1", "power_kw": 22},
                            {"id": "P2", "power_kw": "7.2", "notes": "MOTORCYCLE_ONLY"},
                        ],
                    }
                ],
            }
        ]
    }

    result = data.parse_locations(sample)
    entry = result["L4"]
    assert entry["charger_type"] == "both"
    assert entry["max_power_kw"] == 22.0
