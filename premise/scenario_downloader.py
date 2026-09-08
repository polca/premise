"""Utilities to download scenario data files used for the application examples."""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import requests
from tqdm import tqdm

from premise import __version__

ZENODO_IAM_SCENARIO_RECORD_ID = "21790981"
ZENODO_IAM_SCENARIO_BASE_URL = (
    f"https://zenodo.org/records/{ZENODO_IAM_SCENARIO_RECORD_ID}/files"
)


def get_scenario_file_stems(model: str, pathway: str) -> tuple[str, ...]:
    """Return accepted local file stems, with the canonical name first.

    premise exposes IMAGE pathways with hyphens, whereas the current Zenodo
    archive uses underscores. Supporting both forms lets users reuse downloaded
    archive files without renaming them while retaining the existing canonical
    cache filename.

    :param model: IAM model name used by premise.
    :param pathway: IAM pathway name used by premise.
    :return: Accepted file stems in lookup order.
    """

    canonical_stem = f"{model}_{pathway}"
    if model != "image":
        return (canonical_stem,)

    archive_stem = f"{model}_{pathway.replace('-', '_')}"
    if archive_stem == canonical_stem:
        return (canonical_stem,)

    return canonical_stem, archive_stem


def get_scenario_url(model: str, pathway: str) -> str:
    """Return the Zenodo download URL for an IAM scenario.

    IMAGE files in the current archive use underscores in pathway names, while
    premise exposes those pathway names with hyphens (for example,
    ``SSP2-VLHO``). Other IAM filenames already match the public model and
    pathway names.

    :param model: IAM model name used by premise.
    :param pathway: IAM pathway name used by premise.
    :return: Direct Zenodo URL for the encrypted scenario CSV file.
    """

    archive_stem = get_scenario_file_stems(model, pathway)[-1]
    return f"{ZENODO_IAM_SCENARIO_BASE_URL}/{archive_stem}.csv"


def download_csv(file_name: str, url: str, download_folder: Path) -> Path:
    """Download a CSV file from Zenodo if it is not present locally.

    A progress bar is displayed using :mod:`tqdm` while the file is being
    downloaded. When the destination directory does not yet exist it is created
    automatically. Temporary network failures are retried up to three times.
    The destination is published only after a complete download; failures raise
    a requests exception without leaving a partial cached scenario.

    :param file_name: Name of the file to save the downloaded content as.
    :type file_name: str
    :param url: Direct download URL of the target CSV file.
    :type url: str
    :param download_folder: Directory where the file should be stored.
    :type download_folder: pathlib.Path
    :return: Path to the downloaded file on disk.
    :rtype: pathlib.Path
    """

    if not download_folder.exists():
        download_folder.mkdir(parents=True, exist_ok=True)

    file_path = download_folder / file_name

    if not file_path.exists():
        print(f"{file_name} not found locally. Downloading...")

        version_str = ".".join(map(str, __version__))
        headers = {
            "User-Agent": f"premise-lca/{version_str} (https://github.com/polca/premise)"
        }
        for attempt in range(4):
            temporary_path = None
            try:
                with requests.get(
                    url, stream=True, timeout=60, headers=headers
                ) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get("Content-Length", 0))
                    with (
                        tempfile.NamedTemporaryFile(
                            dir=download_folder, suffix=".part", delete=False
                        ) as file_handle,
                        tqdm(
                            total=total_size, unit="B", unit_scale=True, desc=file_name
                        ) as progress,
                    ):
                        temporary_path = Path(file_handle.name)
                        for chunk in response.iter_content(chunk_size=1024):
                            if chunk:
                                file_handle.write(chunk)
                                progress.update(len(chunk))
                    os.replace(temporary_path, file_path)
                print(f"{file_name} downloaded successfully.")
                break
            except requests.RequestException as error:
                status = (
                    error.response.status_code if error.response is not None else None
                )
                retryable = isinstance(
                    error,
                    (
                        requests.ConnectionError,
                        requests.Timeout,
                        requests.exceptions.ChunkedEncodingError,
                    ),
                ) or status in {429, 500, 502, 503, 504}
                if not retryable or attempt == 3:
                    raise
                time.sleep(2**attempt)
            finally:
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
    else:
        print(f"{file_name} already exists locally.")

    return file_path
