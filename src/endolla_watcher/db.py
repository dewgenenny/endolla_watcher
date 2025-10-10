import argparse
import logging
import os

from . import storage
from .logging_utils import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect or optimize the database")
    parser.add_argument(
        "--db-url",
        default=os.getenv("ENDOLLA_DB_URL"),
        help="MySQL connection URL (default: ENDOLLA_DB_URL)",
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Run OPTIMIZE TABLE to reclaim free space",
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    setup_logging(args.debug)

    if not args.db_url:
        raise SystemExit("--db-url or ENDOLLA_DB_URL must be provided")

    conn = storage.connect(args.db_url)
    stats = storage.db_stats(conn)
    logger.info(
        "rows=%d size=%.1fMB free=%.1fMB",
        stats["rows"],
        stats["size_bytes"] / (1024 * 1024),
        stats["free_bytes"] / (1024 * 1024),
    )
    if args.compress:
        before = stats["size_bytes"]
        storage.compress_db(conn)
        stats = storage.db_stats(conn)
        logger.info(
            "compressed from %.1fMB to %.1fMB",
            before / (1024 * 1024),
            stats["size_bytes"] / (1024 * 1024),
        )
    conn.close()


if __name__ == "__main__":
    main()
