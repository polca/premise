"""Native path dialog routes for the local Premise UI."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from premise_ui.api.models import PathDialogRequest
from premise_ui.core.dialogs import DialogUnavailableError, open_path_dialog

router = APIRouter(prefix="/api/dialogs", tags=["dialogs"])


@router.post("/path")
async def choose_path(request: PathDialogRequest) -> dict:
    try:
        selected_path = open_path_dialog(
            mode=request.mode,
            title=request.title,
            initial_path=request.initial_path,
            default_extension=request.default_extension,
            must_exist=request.must_exist,
            filters=[(item.label, item.pattern) for item in request.filters],
        )
    except DialogUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "mode": request.mode,
        "selected_path": selected_path,
        "cancelled": selected_path is None,
    }
