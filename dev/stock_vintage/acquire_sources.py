"""Verify or download pinned public pilot sources from the source manifest."""

import argparse
import csv
import hashlib
import os
from pathlib import Path
import tempfile
from urllib.request import urlopen


def verify(path, record):
    raw = path.read_bytes()
    return (
        len(raw) == int(record["bytes"])
        and hashlib.sha256(raw).hexdigest() == record["sha256"]
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download missing files; never replace a changed local file",
    )
    parser.add_argument("--source-ids", nargs="+", default=["S02", "S05", "S14"])
    args = parser.parse_args()
    manifest = (
        Path(__file__).resolve().parents[2]
        / "docs/development/stock-asset-temporal/acquisition-manifest.csv"
    )
    with manifest.open(newline="") as stream:
        records = [
            row for row in csv.DictReader(stream) if row["source_id"] in args.source_ids
        ]
    if set(args.source_ids) - {r["source_id"] for r in records}:
        raise ValueError("Unknown requested source ID")
    args.input_dir.mkdir(parents=True, exist_ok=True)
    for record in records:
        filename = record["local_research_filename"]
        if Path(filename).name != filename or not record["url"].startswith("https://"):
            raise ValueError("Manifest requires a basename and HTTPS URL")
        path = args.input_dir / filename
        if path.exists():
            if not verify(path, record):
                raise ValueError(f"Changed local source, review explicitly: {path}")
        elif args.download:
            fd, temporary = tempfile.mkstemp(prefix=".source-", dir=args.input_dir)
            try:
                with (
                    os.fdopen(fd, "wb") as target,
                    urlopen(record["url"], timeout=60) as response,
                ):
                    while block := response.read(1024 * 1024):
                        target.write(block)
                if not verify(Path(temporary), record):
                    raise ValueError(
                        f"Downloaded source has changed; review a new version: {filename}"
                    )
                os.replace(temporary, path)
            finally:
                Path(temporary).unlink(missing_ok=True)
        else:
            raise FileNotFoundError(
                f"Missing source: {path}; use --download to retrieve it"
            )
        print(f"Verified {record['source_id']}: {filename}")


if __name__ == "__main__":
    main()
