"""Packaged plain-language storyline descriptions for known IAM scenarios."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def storyline_catalog_path() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "iam_storylines.json"


def load_iam_storyline_catalog() -> dict[str, Any]:
    path = storyline_catalog_path()
    return json.loads(path.read_text(encoding="utf-8"))
