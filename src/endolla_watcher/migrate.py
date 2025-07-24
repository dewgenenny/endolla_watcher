import argparse
from pathlib import Path
import logging

from . import storage
from .logging_utils import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("endolla.db"))
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    setup_logging(args.debug)

    conn = storage.connect(args.db)
    conn.close()
    logger.info("Database migrated to version %d", storage.CURRENT_SCHEMA_VERSION)


if __name__ == "__main__":
    main()
