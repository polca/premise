from .filesystem_constants import DATA_DIR
import os
import requests
from pathlib import Path
from tqdm import tqdm  # Import tqdm for the progress bar


def download_csv(file_name: str, url: str, download_folder: Path) -> Path:
    """Downloads the CSV file from Zenodo if it is not present locally with a progress bar."""
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    file_path = download_folder / file_name

    # Check if the file exists locally
    if not os.path.exists(file_path):
        print(f"{file_name} not found locally. Downloading...")

        # Download the CSV file with a progress bar
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            total_size = int(response.headers.get("Content-Length", 0))
            with (
                open(file_path, "wb") as f,
                tqdm(
                    total=total_size, unit="B", unit_scale=True, desc=file_name
                ) as pbar,
            ):
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
            print(f"{file_name} downloaded successfully.")
        else:
            print(
                f"Failed to download {file_name}. Status code: {response.status_code}"
            )
    else:
        print(f"{file_name} already exists locally.")

    return file_path
