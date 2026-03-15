"""Structured event writing for worker runs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventWriter:
    def __init__(self, run_dir: str | Path, run_id: str) -> None:
        self.run_dir = Path(run_dir).expanduser().resolve()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self.events_path = self.run_dir / "events.jsonl"

    def emit(
        self,
        event_type: str,
        *,
        level: str = "info",
        phase: str | None = None,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "timestamp": utc_now_iso(),
            "run_id": self.run_id,
            "event_type": event_type,
            "level": level,
            "phase": phase,
            "message": message,
            "details": details or {},
        }
        with open(self.events_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")
        return payload


def read_events(path: str | Path) -> list[dict[str, Any]]:
    events_path = Path(path).expanduser().resolve()
    if not events_path.exists():
        return []

    events: list[dict[str, Any]] = []
    with open(events_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    return events
