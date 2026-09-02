"""Utilities to download scenario data files used for the application examples."""

from __future__ import annotations

import os
from pathlib import Path

import requests
from tqdm import tqdm

from premise import __version__

ZENODO_IAM_SCENARIO_RECORD_ID = "22227290"
ZENODO_IAM_SCENARIO_BASE_URL = (
    f"https://zenodo.org/records/{ZENODO_IAM_SCENARIO_RECORD_ID}/files"
)

ZENODO_IAM_SCENARIOS = {
    "remind": frozenset(
        {
            "SSP1-NPi",
            "SSP1-PkBudg650",
            "SSP1-PkBudg1000",
            "SSP2-NDC",
            "SSP2-NPi",
            "SSP2-PkBudg650",
            "SSP2-PkBudg1000",
            "SSP3-rollBack",
        }
    ),
    "remind-eu": frozenset(
        {
            "SSP2-NDC",
            "SSP2-NPi",
            "SSP2-PkBudg650",
            "SSP2-PkBudg1000",
        }
    ),
    "image": frozenset(
        {
            "SSP1-L",
            "SSP1-M",
            "SSP1-VLLO",
            "SSP2-L",
            "SSP2-M",
            "SSP2-VLHO",
            "SSP3-H",
            "SSP5-H",
        }
    ),
    "message": frozenset(
        {
            "SSP1-L",
            "SSP1-VL",
            "SSP2-L",
            "SSP2-LO",
            "SSP2-M",
            "SSP2-ML",
            "SSP2-VL",
            "SSP3-H",
            "SSP4-LO",
            "SSP5-H",
            "SSP5-LO",
        }
    ),
    "tiam-ucl": frozenset(
        {
            "SSP2-RCP19",
            "SSP2-RCP26",
            "SSP2-RCP45",
            "SSP2-RCP60",
        }
    ),
}


def get_scenario_cache_directory(root: Path) -> Path:
    """Return the record-specific directory for built-in IAM downloads."""

    return Path(root) / f"zenodo-{ZENODO_IAM_SCENARIO_RECORD_ID}"


def is_builtin_scenario(model: str, pathway: str) -> bool:
    """Return whether a model/pathway pair is published in the current record."""

    return pathway in ZENODO_IAM_SCENARIOS.get(model.lower(), ())


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

    if not is_builtin_scenario(model, pathway):
        available = sorted(ZENODO_IAM_SCENARIOS.get(model.lower(), ()))
        replacement = (
            " Use 'SSP2-RCP60' instead; built-in TIAM-UCL SSP2-Base was "
            "retired with the v2.5.0 archive."
            if model.lower() == "tiam-ucl" and pathway == "SSP2-Base"
            else ""
        )
        raise ValueError(
            f"No built-in IAM scenario is published for {model}/{pathway}. "
            f"Available pathways for {model}: {available}.{replacement}"
        )

    archive_stem = get_scenario_file_stems(model, pathway)[-1]
    return f"{ZENODO_IAM_SCENARIO_BASE_URL}/{archive_stem}.csv"


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

        version_str = ".".join(map(str, __version__))
        headers = {
            "User-Agent": f"premise-lca/{version_str} (https://github.com/polca/premise)"
        }
        partial_path = file_path.with_suffix(file_path.suffix + ".part")
        partial_path.unlink(missing_ok=True)
        response = requests.get(url, stream=True, timeout=60, headers=headers)

        try:
            response.raise_for_status()
            total_size = int(response.headers.get("Content-Length", 0))
            with (
                open(partial_path, "wb") as file_handle,
                tqdm(
                    total=total_size, unit="B", unit_scale=True, desc=file_name
                ) as progress,
            ):
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file_handle.write(chunk)
                        progress.update(len(chunk))
            os.replace(partial_path, file_path)
            print(f"{file_name} downloaded successfully.")
        except Exception:
            partial_path.unlink(missing_ok=True)
            raise
        finally:
            response.close()
    else:
        print(f"{file_name} already exists locally.")

    return file_path
