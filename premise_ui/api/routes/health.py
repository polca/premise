"""Basic health endpoints for the local Premise UI API."""

from __future__ import annotations

from fastapi import APIRouter

from premise_ui import __version__
from premise_ui.core.premise_metadata import load_premise_version

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "service": "premise-ui",
        "ui_version": __version__,
        "premise_version": load_premise_version(),
    }
