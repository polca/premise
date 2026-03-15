"""Native filesystem dialog helpers for the local Premise UI."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any


class DialogUnavailableError(RuntimeError):
    """Raised when a native path dialog cannot be displayed."""


def _load_tkinter():
    import tkinter
    from tkinter import filedialog

    return tkinter, filedialog


def native_dialog_state() -> dict[str, Any]:
    """Return native path-dialog availability and fallback information."""

    try:
        tkinter, _filedialog = _load_tkinter()
    except ImportError:
        return {
            "available": False,
            "backend": "none",
            "detail": "Tkinter is not available in this Python environment.",
            "manual_path_entry": True,
        }

    if sys.platform.startswith("linux") and not (
        os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")
    ):
        return {
            "available": False,
            "backend": "tkinter",
            "detail": "No graphical desktop session was detected for native file dialogs.",
            "manual_path_entry": True,
        }

    return {
        "available": True,
        "backend": "tkinter",
        "detail": (
            "Native file dialogs are available through Tkinter."
            if getattr(tkinter, "TkVersion", None) is not None
            else "Native file dialogs appear to be available."
        ),
        "manual_path_entry": True,
    }


def _initial_directory(initial_path: str | None, *, mode: str) -> str:
    if not initial_path:
        return str(Path.home())

    candidate = Path(initial_path).expanduser()
    if mode == "open_directory":
        if candidate.exists() and candidate.is_dir():
            return str(candidate.resolve())
        parent = candidate.parent if candidate.name else candidate
        if parent.exists():
            return str(parent.resolve())
        return str(Path.home())

    if candidate.exists() and candidate.is_dir():
        return str(candidate.resolve())

    parent = candidate.parent if candidate.name else candidate
    if parent.exists():
        return str(parent.resolve())
    return str(Path.home())


def _initial_file(initial_path: str | None) -> str:
    if not initial_path:
        return ""

    candidate = Path(initial_path).expanduser()
    if candidate.name and not candidate.is_dir():
        return candidate.name
    return ""


def open_path_dialog(
    *,
    mode: str,
    title: str | None = None,
    initial_path: str | None = None,
    default_extension: str | None = None,
    must_exist: bool = True,
    filters: list[tuple[str, str]] | None = None,
) -> str | None:
    """Open a local native dialog and return the selected path."""

    try:
        tkinter, filedialog = _load_tkinter()
    except ImportError as exc:
        raise DialogUnavailableError(
            "Tkinter is not available in this Python environment."
        ) from exc

    root = None
    try:
        root = tkinter.Tk()
        root.withdraw()
        try:
            root.attributes("-topmost", True)
        except tkinter.TclError:
            pass
        root.update()

        dialog_kwargs = {
            "title": title or "Select a path",
            "initialdir": _initial_directory(initial_path, mode=mode),
        }

        initial_file = _initial_file(initial_path)
        if initial_file and mode in {"open_file", "save_file"}:
            dialog_kwargs["initialfile"] = initial_file
        if filters and mode in {"open_file", "save_file"}:
            dialog_kwargs["filetypes"] = filters
        if default_extension and mode == "save_file":
            dialog_kwargs["defaultextension"] = default_extension

        if mode == "open_file":
            selected = filedialog.askopenfilename(**dialog_kwargs)
        elif mode == "save_file":
            selected = filedialog.asksaveasfilename(**dialog_kwargs)
        elif mode == "open_directory":
            selected = filedialog.askdirectory(
                title=dialog_kwargs["title"],
                initialdir=dialog_kwargs["initialdir"],
                mustexist=must_exist,
            )
        else:
            raise ValueError(f"Unsupported dialog mode: {mode}")
    except tkinter.TclError as exc:
        raise DialogUnavailableError(
            "A native file dialog is unavailable in this environment."
        ) from exc
    finally:
        if root is not None:
            try:
                root.grab_release()
            except tkinter.TclError:
                pass
            try:
                root.update_idletasks()
            except tkinter.TclError:
                pass
            try:
                root.quit()
            except tkinter.TclError:
                pass
            try:
                root.destroy()
            except tkinter.TclError:
                pass

    if not selected:
        return None

    return str(Path(selected).expanduser().resolve())
