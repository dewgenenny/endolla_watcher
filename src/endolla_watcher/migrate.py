import argparse
import logging
from pathlib import Path
from typing import Iterable, Tuple

import sqlite3

from . import storage
from .logging_utils import setup_logging

logger = logging.getLogger(__name__)


def _iter_sqlite_rows(conn: sqlite3.Connection, batch_size: int) -> Iterable[Tuple]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT ts, location_id, station_id, port_id, status, last_updated FROM port_status ORDER BY ts"
    )
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield from rows


def migrate(sqlite_path: Path, db_url: str, truncate: bool, batch_size: int) -> int:
    logger.info("Starting migration from %s to %s", sqlite_path, db_url)
    sqlite_conn = sqlite3.connect(sqlite_path)
    mysql_conn = storage.connect(db_url)
    try:
        if truncate:
            with mysql_conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE port_status")
            mysql_conn.commit()
        else:
            with mysql_conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM port_status")
                count = cur.fetchone()[0]
            if count:
                raise RuntimeError(
                    "Destination database already contains data. Use --truncate to overwrite."
                )

        migrated = 0
        with mysql_conn.cursor() as cur:
            for row in _iter_sqlite_rows(sqlite_conn, batch_size):
                cur.execute(
                    """
                    INSERT INTO port_status (ts, location_id, station_id, port_id, status, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    row,
                )
                migrated += 1
                if migrated % batch_size == 0:
                    mysql_conn.commit()
        mysql_conn.commit()
        logger.info("Migrated %d rows", migrated)
        return migrated
    finally:
        mysql_conn.close()
        sqlite_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate data from SQLite to MySQL")
    parser.add_argument("--sqlite", type=Path, required=True, help="Path to existing SQLite database")
    parser.add_argument("--db-url", required=True, help="Destination MySQL connection URL")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Clear existing MySQL data before importing",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows per transaction batch",
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    setup_logging(args.debug)

    if not args.sqlite.exists():
        raise SystemExit(f"SQLite database {args.sqlite} not found")

    migrated = migrate(args.sqlite, args.db_url, args.truncate, args.batch_size)
    logger.info("Migration complete (%d rows)", migrated)


if __name__ == "__main__":
    main()
