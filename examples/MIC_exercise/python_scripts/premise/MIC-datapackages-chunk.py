#!/usr/bin/env python3
"""
Individual chunk processor for parallel MIC jobs
"""

import os
import sys
from pathlib import Path
import yaml

# Creating isolated cache, to run premise in parallel
if len(sys.argv) >= 4:
    model = sys.argv[1]
    chunk_num = int(sys.argv[2])
    total_chunks = int(sys.argv[3])

    shared_cache = "/tmp/premise_shared_cache"
    os.makedirs(shared_cache, exist_ok=True)
    os.environ["USER_DATA_BASE_DIR"] = shared_cache

    original_cwd = Path.cwd()

    isolated_work_dir = Path(f"/tmp/pathways_{os.getpid()}_{chunk_num}_{model}")
    isolated_work_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(isolated_work_dir)

    with open("variables.yaml", "w") as f:
        yaml.dump({"USER_DATA_BASE_DIR": shared_cache}, f)
    print(f"Created variables.yaml with shared cache: {shared_cache}")

import bw2io as bi
import bw2data as bd
from premise import *
import json
import pickle
import gc
import time
from datetime import datetime, timedelta
import random
import shutil

# Configuration
CONFIG = {
    "key": "tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
    "years": [2020, 2025, 2030, 2035, 2040, 2045, 2050, 2060, 2070, 2080, 2090, 2100],
    "output_dir": "datapackages",
}


def get_model_config(model):
    """Get model configuration from cache file"""
    cache_file = original_cwd / f"working_pathways_{model}.json"
    if cache_file.exists():
        with open(cache_file, "r") as f:
            cache_data = json.load(f)
            if isinstance(cache_data, list):
                log(f"⚠ WARNING: Old cache format detected. Assuming server mode.")
                return {"pathways": cache_data, "mode": "server", "folder": None}
            return cache_data
    log(f"ERROR: No cache file found for {model}. Run MIC-datapackages.py first.")
    return None


def get_iam_filepath(model, model_config):
    """Get filepath for local IAM files, or None if using server"""
    if model_config["mode"] == "local" and model_config["folder"]:
        filepath = original_cwd / model_config["folder"]

        if filepath.exists():
            log(f"Using local IAM files for {model} from: {filepath}")
            return str(filepath)
        else:
            log(f"WARNING: Local IAM folder not found: {filepath}", level="WARNING")
            log(f"  Will attempt to retrieve from server instead")
            return None
    else:
        log(f"Using server retrieval for {model}")
        return None


def log(message, level="INFO"):
    """Log message to file and stdout"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {level}: {message}"
    print(msg, flush=True)

    if "original_cwd" in globals():
        logs_dir = original_cwd / "logs"
        logs_dir.mkdir(exist_ok=True)

        with open(logs_dir / "mic_progress.log", "a") as f:
            f.write(msg + "\n")
            f.flush()


def setup_database(project_name):
    """Set up a separate brightway2 project for this job"""
    log(f"Setting up brightway2 project: {project_name}")

    if project_name not in bd.projects:
        bd.projects.create_project(project_name)

    bd.projects.set_current(project_name)

    db_name = "ecoinvent-3.11-cutoff"
    if db_name not in bd.databases:
        log(f"Importing ecoinvent database into {project_name}...")

        job_id = os.environ.get("SLURM_JOB_ID", "0")
        chunk_num = int(sys.argv[2]) if len(sys.argv) > 2 else 1
        model = sys.argv[1] if len(sys.argv) > 1 else "default"

        import hashlib

        unique_string = f"{job_id}_{chunk_num}_{model}_{os.getpid()}"
        hash_value = int(hashlib.md5(unique_string.encode()).hexdigest()[:8], 16)

        delay_seconds = hash_value % 600

        if delay_seconds > 0:
            log(
                f"Waiting {delay_seconds}s before ecoinvent download to avoid rate limiting..."
            )
            time.sleep(delay_seconds)

        max_retries = 5
        retry_delay = 90

        for attempt in range(max_retries):
            try:
                bi.import_ecoinvent_release(
                    version="3.11",
                    system_model="cutoff",
                    username="Alvaro.Hahn",
                    password="psi$ecoinvent2023!!",
                    biosphere_name="biosphere",
                )
                log(f"Ecoinvent database imported into {project_name}")
                break

            except Exception as e:
                if attempt < max_retries - 1:
                    jitter = hash_value % 60
                    wait_time = retry_delay * (attempt + 1) + jitter
                    log(
                        f"Import failed (attempt {attempt + 1}/{max_retries}): {str(e)}"
                    )
                    log(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    log(
                        f"Failed to import ecoinvent after {max_retries} attempts",
                        level="ERROR",
                    )
                    raise
    else:
        log(f"Ecoinvent database already available in {project_name}")


# def get_working_pathways(model):
#     """Get working pathways for a model"""
#     cache_file = original_cwd / f"working_pathways_{model}.json"
#     if cache_file.exists():
#         with open(cache_file, 'r') as f:
#             return json.load(f)
#     return []


def create_datapackage_chunk(
    chunk_scenarios, chunk_num, model, project_name, model_config
):
    """Create a single datapackage chunk"""
    chunk_name = f"MIC-{model}-chunk-{chunk_num:02d}-{len(chunk_scenarios)}scenarios"

    log(f"Creating {chunk_name} in project {project_name}...")
    start_time = time.time()

    try:
        # Check if model uses local files or server
        iam_filepath = get_iam_filepath(model, model_config)
        use_local_files = iam_filepath is not None

        # Build scenarios with filepath if using local files
        scenarios_config = []
        for scenario in chunk_scenarios:
            scenario_dict = {"model": scenario["model"], "pathway": scenario["pathway"]}
            if use_local_files:
                scenario_dict["filepath"] = iam_filepath
            scenarios_config.append(scenario_dict)

        log(f"  Model: {model}")
        log(f"  Mode: {'LOCAL files' if use_local_files else 'SERVER retrieval'}")
        log(f"  Scenarios: {len(scenarios_config)}")
        log(f"  Years: {len(CONFIG['years'])}")

        pdp_args = {
            "scenarios": scenarios_config,
            "source_db": "ecoinvent-3.11-cutoff",
            "source_version": "3.11",
            "biosphere_name": "biosphere",
            "years": CONFIG["years"],
            "use_absolute_efficiency": True,
        }

        # Only add key if NOT using local files
        if not use_local_files:
            pdp_args["key"] = CONFIG["key"]

        log(f"  Initializing PathwaysDataPackage...")

        max_retries = 5
        for attempt in range(max_retries):
            try:
                ndb = PathwaysDataPackage(**pdp_args)
                log(f"  PathwaysDataPackage initialized successfully")
                break
            except FileNotFoundError as e:
                if "cached_files" in str(e) and attempt < max_retries - 1:
                    job_id = os.environ.get("SLURM_JOB_ID", "0")
                    chunk_num_val = int(sys.argv[2]) if len(sys.argv) > 2 else 1
                    retry_delay = (
                        (int(job_id) % 7) + (chunk_num_val % 5) + attempt
                    ) * 20 + 30

                    log(
                        f"  Cache file not ready, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(retry_delay)
                    continue
                else:
                    raise e

        log(f"  Creating datapackage...")
        ndb.create_datapackage(
            name=chunk_name,
            contributors=[{"name": "Alvaro", "email": "alvaro.hahn-menacho@psi.ch"}],
            strip_cdr_energy=False,
        )

        source_file = Path(f"{chunk_name}.zip")
        target_dir = original_cwd / CONFIG["output_dir"]
        target_dir.mkdir(exist_ok=True)
        target_file = target_dir / f"{chunk_name}.zip"

        if source_file.exists():
            shutil.copy2(source_file, target_file)
            file_size_mb = source_file.stat().st_size / (1024 * 1024)
            log(f"SUCCESS: {chunk_name} ({file_size_mb:.1f} MB)")
            log(f"  Saved to: {target_file}")
        else:
            log(f"WARNING: {chunk_name}.zip not found in {Path.cwd()}")

        elapsed = time.time() - start_time
        log(f"  Completed in {elapsed / 60:.1f} minutes")

        del ndb
        gc.collect()

        return chunk_name

    except Exception as e:
        log(f"ERROR creating {chunk_name}: {str(e)}", level="ERROR")
        import traceback

        traceback.print_exc()
        return None


def main():
    """Main execution for individual chunk"""
    if len(sys.argv) != 5:
        log(
            "ERROR: Usage: python MIC-datapackages-chunk.py <model> <chunk_num> <total_chunks> <chunk_size>"
        )
        return 1

    model = sys.argv[1]
    chunk_num = int(sys.argv[2])
    total_chunks = int(sys.argv[3])
    chunk_size = int(sys.argv[4])

    log(f"Processing {model} chunk {chunk_num}/{total_chunks}")

    # Create unique project name
    project_name = f"MIC_{model}_{chunk_num}_{total_chunks}"

    try:
        # Setup database
        setup_database(project_name)

        # Get pathways and process this chunk
        model_config = get_model_config(model)
        if not model_config:
            return 1

        pathways = model_config["pathways"]
        if not pathways:
            log(f"ERROR: No working pathways for {model}")
            return 1

        start_idx = (chunk_num - 1) * chunk_size
        end_idx = min(start_idx + chunk_size, len(pathways))

        if start_idx >= len(pathways):
            log(f"Chunk {chunk_num} is beyond available pathways")
            return 0

        chunk_pathways = pathways[start_idx:end_idx]
        chunk_scenarios = [{"model": model, "pathway": p} for p in chunk_pathways]

        log(f"Processing {len(chunk_scenarios)} scenarios in chunk {chunk_num}")

        result = create_datapackage_chunk(
            chunk_scenarios,
            chunk_num,
            model,
            project_name,
            model_config,
        )

        if result:
            log(f"Chunk {chunk_num} completed successfully")
            return 0
        else:
            return 1

    except Exception as e:
        log(f"FATAL ERROR: {str(e)}")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        if "original_cwd" in globals():
            os.chdir(original_cwd)
            if "isolated_work_dir" in globals():
                shutil.rmtree(isolated_work_dir, ignore_errors=True)


if __name__ == "__main__":
    exit(main())
