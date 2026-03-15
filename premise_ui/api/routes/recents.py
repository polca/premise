"""Recent project and path routes for the local Premise UI."""

from __future__ import annotations

from fastapi import APIRouter

from premise_ui.api.models import RecentRememberRequest
from premise_ui.core.recents import (
    clear_recent_state,
    recents_payload,
    remember_recent_path,
    remember_recent_project,
)

router = APIRouter(prefix="/api/recents", tags=["recents"])


@router.get("")
def get_recents() -> dict:
    return recents_payload()


@router.post("/remember")
def remember_recent(request: RecentRememberRequest) -> dict:
    if request.kind == "project":
        remember_recent_project(request.path, label=request.label)
    else:
        remember_recent_path(
            request.path,
            kind=request.kind,
            label=request.label,
            base_path=request.base_path,
        )
    return recents_payload()


@router.delete("")
def clear_recents() -> dict:
    clear_recent_state()
    return recents_payload()
