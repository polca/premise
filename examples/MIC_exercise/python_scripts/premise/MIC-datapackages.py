"""
Auto-parallel MIC datapackages launcher
"""

import subprocess
import json
from pathlib import Path
import time
from datetime import datetime

# Configuration
CONFIG = {
    "local_iam_models": {
        "image": {"folder": "image", "enabled": True},  # relative path to IAM files
        "remind": {"folder": "remind", "enabled": False},
        "message": {"folder": "message", "enabled": True},
    },
    "server_iam_models": {
        "image": {"enabled": False},
        "remind": {"enabled": True},
        "message": {"enabled": False},
    },
    "chunk_size": 1,
    "max_pathways_per_model": None,
}


def log(message):
    """Log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {message}"
    print(msg, flush=True)


def get_enabled_models():
    """Get list of all enabled models (local + server)"""
    models = []

    for model, config in CONFIG["local_iam_models"].items():
        if config.get("enabled", True):
            models.append(model)
            log(f"✓ {model} - LOCAL IAM files from ./{config['folder']}/")

    for model, config in CONFIG["server_iam_models"].items():
        if config.get("enabled", True):
            models.append(model)
            log(f"✓ {model} - SERVER retrieval (requires key)")

    return models


def check_local_iam_folder(model):
    """Check if local IAM folder exists and has files"""
    if model not in CONFIG["local_iam_models"]:
        return True

    folder_name = CONFIG["local_iam_models"][model]["folder"]
    folder_path = Path(__file__).parent / folder_name

    if not folder_path.exists():
        log(f"  WARNING: Local IAM folder missing: {folder_path}")
        return False

    iam_files = list(folder_path.glob("*.xlsx")) + list(folder_path.glob("*.csv"))
    if not iam_files:
        log(f"  WARNING: No IAM files found in {folder_path}")
        return False

    log(f"   Found {len(iam_files)} IAM files in {folder_path}")
    return True


def get_working_pathways(model):
    """Get working pathways for a model (with caching)"""
    cache_file = Path(f"working_pathways_{model}.json")
    if cache_file.exists():
        log(f"  Using cached pathways from {cache_file}")
        with open(cache_file, "r") as f:
            return json.load(f)

    is_local = model in CONFIG["local_iam_models"] and CONFIG["local_iam_models"][
        model
    ].get("enabled", False)

    if is_local:
        folder_name = CONFIG["local_iam_models"][model]["folder"]
        filepath = Path(__file__).parent / folder_name

        log(f"Scanning local IAM folder: {filepath}")

        # Find all IAM files
        iam_files = (
            list(filepath.glob("*.xlsx"))
            + list(filepath.glob("*.csv"))
            + list(filepath.glob("*.mif"))
        )

        if not iam_files:
            log(f"  ERROR: No IAM files found in {filepath}")
            return {
                "pathways": [],
                "model": model,
                "mode": "local",
                "folder": folder_name,
            }

        log(f"  Found {len(iam_files)} IAM files")

        working_pathways = []
        for f in iam_files:
            # Extract pathway from filename
            # Example: "IMAGE 3.4_SSP1_L.xlsx" → "SSP1_L"
            # Remove model prefix (case-insensitive) and any version numbers
            filename = f.stem

            # Remove model name and version (e.g., "IMAGE 3.4_", "REMIND_", etc.)
            # This handles various formats like "IMAGE 3.4_", "REMIND 2.1_", etc.
            import re

            # Match model name (letters) + optional version (numbers/dots) + underscore/space
            pattern = rf"^{model}\s*[\d\.]*[_\s]+"
            pathway = re.sub(pattern, "", filename, flags=re.IGNORECASE)
            pathway = pathway.replace("_", "-")

            working_pathways.append(pathway)
            log(f"    {f.name} → {pathway}")

        if not working_pathways:
            log(f"  WARNING: Could not extract pathways from filenames")

    else:
        from premise.data_collection import IAMDataCollection
        from premise.activity_maps import FINAL_ENERGY
        from premise.filesystem_constants import IAM_OUTPUT_DIR
        import yaml

        constants_path = FINAL_ENERGY.parent / "constants.yaml"
        with open(constants_path, "r", encoding="utf-8") as stream:
            constants = yaml.full_load(stream)

        supported_pathways = constants.get("SUPPORTED_PATHWAYS", [])
        if CONFIG["max_pathways_per_model"]:
            supported_pathways = supported_pathways[: CONFIG["max_pathways_per_model"]]

        log(f"Testing {len(supported_pathways)} pathways for {model} from server...")
        working_pathways = []

        for i, pathway in enumerate(supported_pathways, 1):
            try:
                log(f"  Testing {i}/{len(supported_pathways)}: {pathway}")
                iam_data = IAMDataCollection(
                    model=model,
                    pathway=pathway,
                    year=[2020],
                    filepath_iam_files=IAM_OUTPUT_DIR,
                    key="premise_key", # TO BE REQUESTED
                    system_model="cutoff",
                )
                working_pathways.append(pathway)
                del iam_data
            except Exception as e:
                log(f"    FAILED {pathway}: {str(e)[:50]}...")

    # Save cache
    cache_data = {
        "pathways": working_pathways,
        "model": model,
        "mode": "local" if is_local else "server",
        "folder": CONFIG["local_iam_models"][model]["folder"] if is_local else None,
    }

    with open(cache_file, "w") as f:
        json.dump(cache_data, f, indent=2)

    log(f"Found {len(working_pathways)} pathways for {model}")
    log(f"  Cached to {cache_file}")
    return cache_data


def submit_parallel_jobs():
    """Submit all parallel jobs automatically"""
    log("=" * 70)
    log("MIC DATAPACKAGES - AUTO-PARALLEL LAUNCHER")
    log("=" * 70)

    models = get_enabled_models()

    if not models:
        log("ERROR: No IAM models enabled in configuration")
        return []

    log("")
    log(f"Enabled models: {', '.join(models)}")
    log("")

    log("Checking local IAM folders...")
    for model in models:
        check_local_iam_folder(model)
    log("")

    submitted_jobs = []

    for model in models:
        pathway_data = get_working_pathways(model)
        if not pathway_data or not pathway_data.get("pathways"):
            log(f"ERROR: No working pathways for {model}")
            continue

        pathways = pathway_data["pathways"]
        num_chunks = (len(pathways) + CONFIG["chunk_size"] - 1) // CONFIG["chunk_size"]
        log(f"{model}: {len(pathways)} pathways -> {num_chunks} chunks")

        for chunk_num in range(1, num_chunks + 1):
            # Submit job
            cmd = [
                "sbatch",
                "MIC-sbatch-parallel.sh",
                model,
                str(chunk_num),
                str(num_chunks),
                str(CONFIG["chunk_size"]),
            ]

            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    job_id = result.stdout.strip().split()[-1]
                    is_local = model in CONFIG["local_iam_models"] and CONFIG[
                        "local_iam_models"
                    ][model].get("enabled", False)
                    submitted_jobs.append(
                        {
                            "model": model,
                            "chunk": chunk_num,
                            "job_id": job_id,
                            "mode": "local" if is_local else "server",
                        }
                    )
                    mode = "LOCAL" if is_local else "SERVER"
                    log(f"Submitted {model} chunk {chunk_num}: job {job_id} ({mode})")
                else:
                    log(f"ERROR submitting {model} chunk {chunk_num}: {result.stderr}")
            except Exception as e:
                log(f"ERROR submitting {model} chunk {chunk_num}: {str(e)}")

    log(f"Successfully submitted {len(submitted_jobs)} jobs")

    # Save job info
    with open("submitted_jobs.json", "w") as f:
        json.dump(submitted_jobs, f, indent=2)

    log("Job information saved to submitted_jobs.json")
    log("Use 'squeue -u $USER' to check job status")

    return submitted_jobs


def main():
    """Main execution - auto-parallelization"""
    log("Starting auto-parallel MIC processing")

    try:
        from premise import clear_cache

        log("Clearing premise cache to ensure fresh IAM downloads...")
        clear_cache()
        log("Cache cleared successfully")

        submitted_jobs = submit_parallel_jobs()

        if not submitted_jobs:
            log("ERROR: No jobs were submitted successfully")
            return 1

        log(f"Submitted {len(submitted_jobs)} parallel jobs")
        return 0

    except Exception as e:
        log(f"FATAL ERROR: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
