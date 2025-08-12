from datetime import datetime

import endolla_watcher.storage as storage


def test_db_stats_and_compress(tmp_path):
    db = tmp_path / "test.db"
    conn = storage.connect(db)

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
    conn.executemany(
        "INSERT INTO port_status (ts, location_id, station_id, port_id, status, last_updated) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()

    stats = storage.db_stats(conn)
    assert stats["rows"] == 500
    size_before = stats["size_bytes"]

    conn.execute("DELETE FROM port_status")
    conn.commit()
    size_after_delete = storage.db_stats(conn)["size_bytes"]
    assert size_after_delete >= size_before

    storage.compress_db(conn)
    size_after_compress = storage.db_stats(conn)["size_bytes"]
    assert size_after_compress < size_after_delete

    conn.close()
