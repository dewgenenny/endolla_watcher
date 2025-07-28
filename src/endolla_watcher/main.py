import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

from .data import fetch_data, fetch_locations, parse_usage
from .analyze import analyze
from .render import render, render_about, render_problematic
from .stats import from_records
from .logging_utils import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, help="Local JSON file to parse")
    parser.add_argument(
        "--locations",
        type=Path,
        help="Local JSON file with charger locations (default: fetch online)",
    )
    parser.add_argument("--output", type=Path, default=Path("site/index.html"))
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    setup_logging(args.debug)

    start = time.monotonic()
    logger.info("Reading data")
    data = fetch_data(args.file)
    records = parse_usage(data)
    locations = fetch_locations(args.locations)
    problematic = analyze(records)
    stats = from_records(records)

    html = render(
        problematic,
        stats,
        history=None,
        daily=None,
        rule_counts={},
        rules=None,
        updated=datetime.now().astimezone().isoformat(timespec="seconds"),
        elapsed=time.monotonic() - start,
        locations=locations,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    # Write the about page alongside the main report
    about_path = args.output.parent / "about.html"
    about_path.write_text(render_about(), encoding="utf-8")
    prob_path = args.output.parent / "problematic.html"
    prob_page = render_problematic(
        problematic,
        updated=datetime.now().astimezone().isoformat(timespec="seconds"),
        elapsed=time.monotonic() - start,
        locations=locations,
    )
    prob_path.write_text(prob_page, encoding="utf-8")
    logger.info("Wrote report to %s", args.output)


if __name__ == "__main__":
    main()
