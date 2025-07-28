import argparse
import os
import subprocess
import time
from datetime import datetime
import logging
from pathlib import Path
from typing import Dict
from .data import fetch_data, fetch_locations, parse_usage
from .render import render, render_about, render_charger
from . import storage
from .rules import Rules
from .logging_utils import setup_logging

logger = logging.getLogger(__name__)


def fetch_once(db: Path, file: Path | None = None) -> None:
    """Fetch the dataset and store a snapshot in the database."""
    logger.debug("Fetching data with file=%s db=%s", file, db)
    data = fetch_data(file)
    records = parse_usage(data)
    logger.debug("Fetched %d records", len(records))
    conn = storage.connect(db)
    storage.save_snapshot(conn, records)
    conn.close()


def update_once(
    output: Path,
    db: Path,
    rules: Rules | None = None,
    locations: Dict[str, Dict[str, float]] | None = None,
) -> None:
    """Generate the HTML report from stored snapshots."""
    logger.debug("Updating report from db=%s", db)
    start = time.monotonic()
    conn = storage.connect(db)
    problematic, rule_counts = storage.analyze_chargers(conn, rules)
    stats = storage.stats_from_db(conn)
    history = storage.timeline_stats(conn, rules)
    db_size = db.stat().st_size / (1024 * 1024)
    html = render(
        problematic,
        stats,
        history,
        rule_counts,
        rules,
        updated=datetime.now().astimezone().isoformat(timespec="seconds"),
        db_size=db_size,
        elapsed=time.monotonic() - start,
        locations=locations,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    about_path = output.parent / "about.html"
    about_path.write_text(render_about(), encoding="utf-8")

    # Generate charger detail pages for problematic entries
    for r in problematic:
        sessions = storage.charger_sessions(
            conn,
            r.get("location_id"),
            r.get("station_id"),
            limit=10,
        )
        page = render_charger(r.get("location_id"), r.get("station_id"), sessions)
        fname = f"charger_{r.get('location_id')}_{r.get('station_id')}.html"
        (output.parent / fname).write_text(page, encoding="utf-8")

    conn.close()
    logger.debug("Wrote output to %s", output)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path)
    parser.add_argument("--output", type=Path, default=Path("site/index.html"))
    parser.add_argument("--db", type=Path, default=Path("endolla.db"))
    parser.add_argument(
        "--locations",
        type=Path,
        help="Local JSON file with charger locations (default: fetch online)",
    )
    parser.add_argument(
        "--fetch-interval",
        type=int,
        default=60,
        help="Seconds between data fetches",
    )
    parser.add_argument(
        "--update-interval",
        type=int,
        default=3600,
        help="Seconds between report updates",
    )
    parser.add_argument("--unused-days", type=int, default=7)
    parser.add_argument("--long-session-days", type=int, default=2)
    parser.add_argument("--long-session-min", type=int, default=5)
    parser.add_argument("--unavailable-hours", type=int, default=24)
    parser.add_argument(
        "--push-site",
        action="store_true",
        help="Push the generated site to the repository after each update",
    )
    parser.add_argument(
        "--push-repo",
        default=os.getenv("REPO_URL"),
        help="Repository URL for push_site.py",
    )
    parser.add_argument(
        "--push-branch",
        default="gh-pages",
        help="Branch to push the site to",
    )
    parser.add_argument(
        "--push-remote",
        default="origin",
        help="Remote name for push_site.py",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    setup_logging(args.debug)

    locations = fetch_locations(args.locations)

    rules = Rules(
        unused_days=args.unused_days,
        long_session_days=args.long_session_days,
        long_session_min=args.long_session_min,
        unavailable_hours=args.unavailable_hours,
    )

    # Track the next scheduled time for each task. The intervals should
    # remain consistent regardless of how long each action takes to run.
    next_fetch = time.monotonic()
    next_update = time.monotonic()

    while True:
        now = time.monotonic()
        if now >= next_fetch:
            logger.info("Fetching data")
            fetch_once(args.db, args.file)
            next_fetch += args.fetch_interval
            if next_fetch <= now:
                # Catch up if the fetch took longer than the interval
                next_fetch = now + args.fetch_interval

        if now >= next_update:
            logger.info("Updating report")
            update_once(args.output, args.db, rules, locations)
            if args.push_site:
                cmd = [
                    "push_site.py",
                    "--site",
                    str(args.output.parent),
                    "--branch",
                    args.push_branch,
                    "--remote",
                    args.push_remote,
                ]
                if args.push_repo:
                    cmd.extend(["--repo", args.push_repo])
                try:
                    subprocess.check_call(cmd)
                except subprocess.CalledProcessError as exc:
                    logger.error("push_site failed: %s", exc)
            next_update += args.update_interval
            if next_update <= now:
                next_update = now + args.update_interval

        sleep_for = min(next_fetch, next_update) - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
