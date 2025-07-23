import argparse
import time
from pathlib import Path
from .data import fetch_data, parse_usage
from .analyze import analyze
from .render import render
from . import storage


def run_once(output: Path, file: Path | None = None, db: Path | None = None) -> None:
    data = fetch_data(file)
    records = parse_usage(data)
    if db:
        conn = storage.connect(db)
        storage.save_snapshot(conn, records)
        problematic = storage.analyze_recent(conn)
        conn.close()
    else:
        problematic = analyze(records)
    html = render(problematic)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path)
    parser.add_argument("--output", type=Path, default=Path("site/index.html"))
    parser.add_argument("--db", type=Path, default=Path("endolla.db"))
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Seconds between updates"
    )
    args = parser.parse_args()

    while True:
        run_once(args.output, args.file, args.db)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
