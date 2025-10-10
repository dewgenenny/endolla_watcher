from datetime import datetime

from datetime import datetime

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
