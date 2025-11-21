"""
This module contains constants for the filesystem paths used by Premise.
"""

from pathlib import Path
from typing import Any, Dict, Optional

import platformdirs
import yaml


def load_var_file() -> Optional[Dict[str, Any]]:
    """Load user-defined variables from ``variables.yaml`` if present.

    :return: Dictionary of variables loaded from the file, or ``None`` when the
        file does not exist.
    :rtype: Optional[Dict[str, Any]]
    """

    var_file = Path.cwd() / "variables.yaml"
    if var_file.exists():
        print(f"Loading variables from {var_file}")
        with open(var_file, "r", encoding="utf-8") as file:
            return yaml.safe_load(file)
    return None


VARIABLES = load_var_file() or {}

# Directories for data which comes with Premise
DATA_DIR = Path(__file__).resolve().parent / "data"
INVENTORY_DIR = DATA_DIR / "additional_inventories"
VARIABLES_DIR = Path(__file__).resolve().parent / "iam_variables_mapping"
IAM_OUTPUT_DIR = DATA_DIR / "iam_output_files"

if "USER_DATA_BASE_DIR" in VARIABLES:
    USER_DATA_BASE_DIR = Path(VARIABLES.get("USER_DATA_BASE_DIR"))
    print(f"USER_DATA_BASE_DIR: {USER_DATA_BASE_DIR}")
else:
    USER_DATA_BASE_DIR = platformdirs.user_data_path(
        appname="premise", appauthor="pylca"
    )
USER_DATA_BASE_DIR.mkdir(parents=True, exist_ok=True)

DIR_CACHED_DB = USER_DATA_BASE_DIR / "cache"
DIR_CACHED_DB.mkdir(parents=True, exist_ok=True)

DIR_CACHED_FILES = USER_DATA_BASE_DIR / "cached_files"
DIR_CACHED_FILES.mkdir(parents=True, exist_ok=True)

USER_LOGS_DIR = platformdirs.user_log_path(appname="premise", appauthor="pylca")
USER_LOGS_DIR.mkdir(parents=True, exist_ok=True)
