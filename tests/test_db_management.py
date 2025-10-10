from datetime import datetime, timedelta

import endolla_watcher.storage as storage


def test_db_stats_and_compress(conn):
    now = datetime.now().astimezone()
    rows = [
        (
            now.isoformat(),
            "loc",
            "sta",
            str(i),
            "IN_USE",
            None,
        )
        for i in range(500)
    ]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO port_status (ts, location_id, station_id, port_id, status, last_updated) VALUES (%s, %s, %s, %s, %s, %s)",
            rows,
        )
    conn.commit()

    stats = storage.db_stats(conn)
    assert stats["rows"] == 500
    size_before = stats["size_bytes"]

    with conn.cursor() as cur:
        cur.execute("DELETE FROM port_status")
    conn.commit()
    size_after_delete = storage.db_stats(conn)["size_bytes"]
    assert size_after_delete >= size_before

    storage.compress_db(conn)
    size_after_compress = storage.db_stats(conn)["size_bytes"]
    assert size_after_compress < size_after_delete


def test_prune_old_data_aging_levels(conn):
    now = datetime.now().astimezone()

    high_base = now - timedelta(days=1)
    medium_base = now - timedelta(days=10)
    low_base = now - timedelta(days=40)
    very_low_base = now - timedelta(days=60)

    rows = [
        # High detail window (keep all)
        (high_base.isoformat(), "loc", "sta", "high", "IN_USE", None),
        ((high_base + timedelta(minutes=30)).isoformat(), "loc", "sta", "high", "AVAILABLE", None),
        # Medium detail window (hourly)
        (medium_base.isoformat(), "loc", "sta", "medium", "IN_USE", None),
        ((medium_base + timedelta(minutes=15)).isoformat(), "loc", "sta", "medium", "AVAILABLE", None),
        ((medium_base + timedelta(hours=2)).isoformat(), "loc", "sta", "medium", "IN_USE", None),
        # Low detail window (daily)
        (low_base.isoformat(), "loc", "sta", "low", "IN_USE", None),
        ((low_base + timedelta(hours=2)).isoformat(), "loc", "sta", "low", "AVAILABLE", None),
        (very_low_base.isoformat(), "loc", "sta", "low", "IN_USE", None),
        ((very_low_base + timedelta(hours=3)).isoformat(), "loc", "sta", "low", "AVAILABLE", None),
    ]

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO port_status (ts, location_id, station_id, port_id, status, last_updated)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            rows,
        )
    conn.commit()

    storage.prune_old_data(conn)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT port_id, COUNT(*) FROM port_status GROUP BY port_id"
        )
        counts = dict(cur.fetchall())

    assert counts["high"] == 2
    assert counts["medium"] == 2  # one per hour bucket older than 7 days
    assert counts["low"] == 2  # one per day bucket older than 30 days
