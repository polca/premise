"""Capability endpoints describing the current UI scaffold."""

from __future__ import annotations

import os
import platform
import sys

from fastapi import APIRouter

from premise_ui.core.capabilities import get_capabilities
from premise_ui.core.credentials import iam_key_state
from premise_ui.core.dialogs import native_dialog_state

router = APIRouter(prefix="/api", tags=["capabilities"])


@router.get("/capabilities")
def capabilities() -> dict:
    return get_capabilities()


@router.get("/environment")
def environment() -> dict:
    iam_key = iam_key_state()
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "credentials": {
            "EI_USERNAME": bool(os.environ.get("EI_USERNAME")),
            "EI_PASSWORD": bool(os.environ.get("EI_PASSWORD")),
            "IAM_FILES_KEY": bool(iam_key["has_value"]),
        },
        "dialogs": {
            "native_path_dialogs": native_dialog_state(),
        },
    }
