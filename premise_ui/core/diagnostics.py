"""Helpers for loading and redacting per-run diagnostics."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from premise_ui.core.redaction import redact_absolute_paths, redact_value
from premise_ui.core.storage import read_json
from premise_ui.worker.events import read_events


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return read_json(path)
    except Exception:
        return None


def _read_text_tail(path: Path, *, max_lines: int = 120) -> str:
    if not path.exists():
        return ""

    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        lines = handle.readlines()

    return "".join(lines[-max_lines:])


def load_run_diagnostics(run_dir: str | Path) -> dict[str, Any]:
    base_dir = Path(run_dir).expanduser().resolve()
    events = read_events(base_dir / "events.jsonl")
    diagnostics = _read_json_if_exists(base_dir / "diagnostics.json")
    metadata = _read_json_if_exists(base_dir / "metadata.json")
    manifest = _read_json_if_exists(base_dir / "run_manifest.json")
    result = _read_json_if_exists(base_dir / "result.json")
    stdout_tail = redact_absolute_paths(_read_text_tail(base_dir / "stdout.log"))
    stderr_tail = redact_absolute_paths(_read_text_tail(base_dir / "stderr.log"))

    if diagnostics is not None:
        diagnostics = redact_value(diagnostics)

    redacted_events = []
    for event in events[-40:]:
        item = dict(event)
        item = redact_value(item)
        redacted_events.append(item)

    return {
        "run_dir": str(base_dir),
        "available_files": sorted(
            path.name for path in base_dir.iterdir() if path.is_file()
        )
        if base_dir.exists()
        else [],
        "metadata": metadata,
        "manifest": manifest,
        "result": result,
        "diagnostics": diagnostics,
        "events": redacted_events,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }
