"""CLI script for ingesting trade/position files.

Usage:
    python cli.py sample_data/trades_format1.csv [sample_data/positions.yaml ...]
    python cli.py --permissive sample_data/trades_format1.csv
"""
import argparse
import json
import sys

from app import create_app
from app.services import ingest_file, IngestionError


def main():
    parser = argparse.ArgumentParser(description="Ingest trade/position files")
    parser.add_argument("files", nargs="+", help="File paths to ingest")
    parser.add_argument("--permissive", action="store_true", help="Skip malformed rows instead of aborting")
    args = parser.parse_args()

    app = create_app()
    strict = not args.permissive

    with app.app_context():
        for path in args.files:
            try:
                with open(path, "r") as f:
                    content = f.read()
                filename = path.split("/")[-1]
                report = ingest_file(filename, content, strict=strict)
                print(json.dumps(report, indent=2))
            except FileNotFoundError:
                print(f"Error: file not found: {path}", file=sys.stderr)
                sys.exit(1)
            except IngestionError as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)


if __name__ == "__main__":
    main()
