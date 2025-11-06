"""Utilities to download scenario data files used for the application examples."""

from __future__ import annotations

import os
from pathlib import Path

import requests
from tqdm import tqdm


def download_csv(file_name: str, url: str, download_folder: Path) -> Path:
    """Download a CSV file from Zenodo if it is not present locally.

    A progress bar is displayed using :mod:`tqdm` while the file is being
    downloaded. When the destination directory does not yet exist it is created
    automatically.

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

        response = requests.get(url, stream=True, timeout=60)

        if response.status_code == 200:
            total_size = int(response.headers.get("Content-Length", 0))
            with (
                open(file_path, "wb") as file_handle,
                tqdm(
                    total=total_size, unit="B", unit_scale=True, desc=file_name
                ) as progress,
            ):
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file_handle.write(chunk)
                        progress.update(len(chunk))
            print(f"{file_name} downloaded successfully.")
        else:
            print(
                f"Failed to download {file_name}. Status code: {response.status_code}"
            )
    else:
        print(f"{file_name} already exists locally.")

    return file_path
