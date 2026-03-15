"""Worker entrypoint for isolated Premise UI runs."""

from __future__ import annotations

import argparse
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from premise_ui.worker.runner import run_manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a Premise UI worker job.")
    parser.add_argument("manifest_path", help="Path to the run manifest JSON file.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the scaffold worker without executing Premise workflows.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    manifest_path = Path(args.manifest_path).expanduser().resolve()
    run_dir = manifest_path.parent

    stdout_path = run_dir / "stdout.log"
    stderr_path = run_dir / "stderr.log"

    with (
        open(stdout_path, "a", encoding="utf-8") as stdout_handle,
        open(stderr_path, "a", encoding="utf-8") as stderr_handle,
        redirect_stdout(stdout_handle),
        redirect_stderr(stderr_handle),
    ):
        return run_manifest(manifest_path, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
