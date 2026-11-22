"""Find a local stock-vintage ZIP or download it from a Zenodo record."""

import json
import re
from functools import partial
from hashlib import file_digest
from pathlib import Path
from urllib.parse import quote
from zipfile import BadZipFile, ZipFile

from ..core.filesystem_constants import DIR_CACHED_FILES, IAM_VINTAGE_DIR
from .scenario_downloader import download_file

# A published, version-specific record can become the default once available.
ZENODO_VINTAGE_RECORD_ID = None


def _validate_package(path: Path, *, model: str, pathway: str) -> None:
    """Check the archive identity and the checksums of both declared data files."""
    try:
        with ZipFile(path) as archive:
            names = archive.namelist()
            for name in ("datapackage.json", "vintages.parquet", "validation.json"):
                if names.count(name) != 1:
                    raise ValueError(f"Expected one {name} at the root of the ZIP.")

            manifest = json.loads(archive.read("datapackage.json"))
            if (
                manifest.get("profile") != "iam-preprocessor-stock-vintages"
                or manifest.get("schema_version") != 2
            ):
                raise ValueError(
                    "Expected an iam-preprocessor stock-vintage package v2."
                )
            if (
                manifest["model"].casefold() != model.casefold()
                or manifest["scenario"] != pathway
            ):
                raise ValueError(
                    f"Expected {model}/{pathway}, found "
                    f"{manifest['model']}/{manifest['scenario']}."
                )

            checksums = {item["path"]: item["sha256"] for item in manifest["resources"]}
            for name in ("vintages.parquet", "validation.json"):
                with archive.open(name) as resource:
                    if file_digest(resource, "sha256").hexdigest() != checksums[name]:
                        raise ValueError(f"Checksum mismatch for {name}.")
    except (BadZipFile, AttributeError, KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid stock-vintage package {path}. {exc}") from exc


def _find_vintage_package(
    model: str,
    pathway: str,
    *,
    local_dir: str | Path | None = None,
    zenodo_record_id: str | int | None = None,
) -> Path | None:
    """Find a verified archive for the IAM scenario selected in TRAILS.

    Look in the supplied directory or in premise's internal IAM vintage
    folder first. On a miss, use the cache for the specified Zenodo record or
    download the archive into that cache. A missing package is optional when
    neither a local folder nor a Zenodo record was specified.
    """
    for label in (model, pathway):
        if not label or any(character in label for character in ("/", "\\", "\x00")):
            raise ValueError("Model and pathway must be valid filename labels.")

    file_name = f"{model.lower()}_{pathway}_stock_vintages_v2.zip"
    directory = IAM_VINTAGE_DIR if local_dir is None else Path(local_dir)
    local_path = directory / file_name
    validate = partial(_validate_package, model=model, pathway=pathway)
    if local_path.is_file():
        validate(local_path)
        return local_path

    record_id = (
        ZENODO_VINTAGE_RECORD_ID if zenodo_record_id is None else zenodo_record_id
    )
    if record_id is None:
        if local_dir is None:
            return None
        raise FileNotFoundError(
            f"No stock-vintage package at {local_path}. "
            "Place the ZIP there or provide a Zenodo record ID."
        )
    record = str(record_id)
    if not re.fullmatch(r"[0-9]+", record):
        raise ValueError("The Zenodo record ID must contain only digits.")

    url = f"https://zenodo.org/records/{record}/files/{quote(file_name)}"
    cache = DIR_CACHED_FILES / "iam_vintage_files" / record
    return download_file(file_name, url, cache, validate=validate)
