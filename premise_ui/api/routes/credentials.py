"""Credential routes for the local Premise UI."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from premise_ui.api.models import IamKeyUpdateRequest
from premise_ui.core.credentials import clear_iam_key, iam_key_state, store_iam_key

router = APIRouter(prefix="/api/credentials", tags=["credentials"])


@router.get("/iam-key")
def get_iam_key() -> dict:
    return iam_key_state()


@router.post("/iam-key")
def set_iam_key(request: IamKeyUpdateRequest) -> dict:
    try:
        return store_iam_key(request.value, remember=request.remember)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/iam-key")
def delete_iam_key() -> dict:
    return clear_iam_key()
