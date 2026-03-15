"""Helpers for redacting sensitive values from exported diagnostics."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def redact_absolute_paths(text: str) -> str:
    redacted = text.replace(str(Path.home()), "~")
    redacted = re.sub(r"(?<!\w)(/[^\s]+)", "<path>", redacted)
    redacted = re.sub(r"(?<!\w)([A-Za-z]:\\[^\s]+)", "<path>", redacted)
    return redacted


def redact_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_absolute_paths(value)
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, dict):
        return {key: redact_value(item) for key, item in value.items()}
    return value
