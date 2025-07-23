import argparse
from pathlib import Path

from .data import fetch_data, parse_usage
from .analyze import analyze
from .render import render


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, help="Local JSON file to parse")
    parser.add_argument("--output", type=Path, default=Path("site/index.html"))
    args = parser.parse_args()

    data = fetch_data(args.file)
    records = parse_usage(data)
    problematic = analyze(records)

    html = render(problematic)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")


if __name__ == "__main__":
    main()
