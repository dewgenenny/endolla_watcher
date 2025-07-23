import argparse
import logging
from pathlib import Path

from .data import fetch_data, parse_usage
from .analyze import analyze
from .render import render
from .logging_utils import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, help="Local JSON file to parse")
    parser.add_argument("--output", type=Path, default=Path("site/index.html"))
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    setup_logging(args.debug)

    logger.info("Reading data")
    data = fetch_data(args.file)
    records = parse_usage(data)
    problematic = analyze(records)

    html = render(problematic)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    logger.info("Wrote report to %s", args.output)


if __name__ == "__main__":
    main()
