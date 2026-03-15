"""Local credential helpers for the Premise UI."""

from __future__ import annotations

import json
import os
from typing import Any

from premise_ui.core.paths import USER_UI_CREDENTIALS_FILE

IAM_FILES_KEY_NAME = "IAM_FILES_KEY"
KEYRING_SERVICE_NAME = "premise-ui"
CREDENTIALS_FILE = USER_UI_CREDENTIALS_FILE


def _load_keyring():
    try:
        import keyring
    except ImportError:
        return None
    return keyring


def _read_file_store() -> dict[str, str]:
    if not CREDENTIALS_FILE.exists():
        return {}

    try:
        payload = json.loads(CREDENTIALS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

    if not isinstance(payload, dict):
        return {}

    return {
        str(key): str(value)
        for key, value in payload.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def _write_file_store(payload: dict[str, str]) -> None:
    CREDENTIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CREDENTIALS_FILE.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    try:
        os.chmod(CREDENTIALS_FILE, 0o600)
    except PermissionError:
        pass


def _delete_file_store_value(name: str) -> None:
    payload = _read_file_store()
    if name in payload:
        del payload[name]
        if payload:
            _write_file_store(payload)
        else:
            try:
                CREDENTIALS_FILE.unlink()
            except FileNotFoundError:
                pass


def _set_runtime_value(name: str, value: str) -> None:
    os.environ[name] = value


def _clear_runtime_value(name: str) -> None:
    os.environ.pop(name, None)


def _stored_file_value(name: str) -> str | None:
    return _read_file_store().get(name)


def _stored_keyring_value(name: str) -> str | None:
    keyring = _load_keyring()
    if keyring is None:
        return None

    try:
        return keyring.get_password(KEYRING_SERVICE_NAME, name)
    except Exception:
        return None


def _stored_value(name: str) -> tuple[str | None, str | None]:
    keyring_value = _stored_keyring_value(name)
    if keyring_value:
        return keyring_value, "keyring"

    file_value = _stored_file_value(name)
    if file_value:
        return file_value, "file"

    return None, None


def credential_value(name: str) -> str | None:
    runtime_value = os.environ.get(name)
    if runtime_value:
        return runtime_value

    stored_value, _backend = _stored_value(name)
    if stored_value:
        _set_runtime_value(name, stored_value)
    return stored_value


def credential_state(name: str) -> dict[str, Any]:
    runtime_value = os.environ.get(name)
    stored_value, backend = _stored_value(name)

    if runtime_value:
        return {
            "name": name,
            "has_value": True,
            "value": runtime_value,
            "source": "environment" if not stored_value else backend,
            "persisted": bool(stored_value),
            "backend": backend or "environment",
        }

    if stored_value:
        _set_runtime_value(name, stored_value)
        return {
            "name": name,
            "has_value": True,
            "value": stored_value,
            "source": backend,
            "persisted": True,
            "backend": backend,
        }

    return {
        "name": name,
        "has_value": False,
        "value": "",
        "source": "none",
        "persisted": False,
        "backend": "none",
    }


def store_credential(name: str, value: str, *, remember: bool = True) -> dict[str, Any]:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("Credential value cannot be empty.")

    _set_runtime_value(name, normalized)

    if remember:
        keyring = _load_keyring()
        if keyring is not None:
            try:
                keyring.set_password(KEYRING_SERVICE_NAME, name, normalized)
                _delete_file_store_value(name)
                return credential_state(name)
            except Exception:
                pass

        payload = _read_file_store()
        payload[name] = normalized
        _write_file_store(payload)
        return credential_state(name)

    keyring = _load_keyring()
    if keyring is not None:
        try:
            keyring.delete_password(KEYRING_SERVICE_NAME, name)
        except Exception:
            pass
    _delete_file_store_value(name)
    return credential_state(name)


def clear_credential(name: str) -> dict[str, Any]:
    stored_value, _backend = _stored_value(name)
    keyring = _load_keyring()
    if keyring is not None:
        try:
            keyring.delete_password(KEYRING_SERVICE_NAME, name)
        except Exception:
            pass
    _delete_file_store_value(name)
    if stored_value:
        _clear_runtime_value(name)
    return credential_state(name)


def iam_key_state() -> dict[str, Any]:
    return credential_state(IAM_FILES_KEY_NAME)


def iam_key_value() -> str | None:
    return credential_value(IAM_FILES_KEY_NAME)


def store_iam_key(value: str, *, remember: bool = True) -> dict[str, Any]:
    return store_credential(IAM_FILES_KEY_NAME, value, remember=remember)


def clear_iam_key() -> dict[str, Any]:
    return clear_credential(IAM_FILES_KEY_NAME)
