import argparse
import time
import logging
from pathlib import Path
from .data import fetch_data, parse_usage
from .analyze import analyze
from .render import render
from . import storage
from .rules import Rules
from .logging_utils import setup_logging

logger = logging.getLogger(__name__)


def run_once(
    output: Path,
    file: Path | None = None,
    db: Path | None = None,
    rules: Rules | None = None,
) -> None:
    logger.debug("Running update with file=%s db=%s", file, db)
    data = fetch_data(file)
    records = parse_usage(data)
    logger.debug("Fetched %d records", len(records))
    if db:
        conn = storage.connect(db)
        storage.save_snapshot(conn, records)
        problematic = storage.analyze_chargers(conn, rules)
        conn.close()
    else:
        problematic = analyze(records)
    html = render(problematic)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    logger.debug("Wrote output to %s", output)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path)
    parser.add_argument("--output", type=Path, default=Path("site/index.html"))
    parser.add_argument("--db", type=Path, default=Path("endolla.db"))
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Seconds between updates",
    )
    parser.add_argument("--unused-days", type=int, default=4)
    parser.add_argument("--long-session-days", type=int, default=2)
    parser.add_argument("--long-session-min", type=int, default=5)
    parser.add_argument("--unavailable-hours", type=int, default=24)
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    setup_logging(args.debug)

    rules = Rules(
        unused_days=args.unused_days,
        long_session_days=args.long_session_days,
        long_session_min=args.long_session_min,
        unavailable_hours=args.unavailable_hours,
    )

    while True:
        logger.info("Starting update cycle")
        run_once(args.output, args.file, args.db, rules)
        logger.info("Sleeping for %s seconds", args.interval)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
