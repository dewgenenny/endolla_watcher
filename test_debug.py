
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

PortKey = Tuple[str | None, str | None, str | None]

def _count_unused_chargers(
    days: int,
    now: datetime,
    history: Dict[PortKey, List[Tuple[datetime, str]]] | None = None,
) -> int:
    stations: Dict[Tuple[str | None, str | None], Dict[str | None, List[Tuple[datetime, str]]]] = {}
    
    # Simulate the logic in storage.py
    for (loc, sta, port), events in history.items():
        stations.setdefault((loc, sta), {})[port] = events
        
    count = 0
    for ports in stations.values():
        # This is line 829
        try:
            earliest_ts = min(ts for events in ports.values() for ts, _ in events)
        except ValueError as e:
            print(f"Error caught: {e}")
            raise

    return count

def test():
    now = datetime.now()
    # Case 1: normal data
    history = {
        ("loc1", "sta1", "port1"): [(now, "AVAILABLE"), (now, "IN_USE")]
    }
    _count_unused_chargers(1, now, history)
    print("Case 1 passed")

    # Case 2: what if events is weird?
    # history = { ("loc1", "sta1", "port1"): [] } # empty list -> min raises empty sequence
    
    # Case 3: what if unpack fails?
    try:
        history_bad = {
            ("loc1", "sta1", "port1"): ["not_a_tuple"] 
        }
        # This iterates "not_a_tuple". key, val in history.items(). events="not_a_tuple" (str)
        # loop iterates string chars? No events is the value.
        # events is "not_a_tuple".
        # for ts, _ in "not_a_tuple".
        # "n", "o" -> unpacks? No, "not_a_tuple" yields strings of length 1.
        # unpack fails: expected 2, got 1.
        _count_unused_chargers(1, now, history_bad) # type: ignore
    except ValueError as e:
        print(f"Case 3 error: {e}")

if __name__ == "__main__":
    test()
