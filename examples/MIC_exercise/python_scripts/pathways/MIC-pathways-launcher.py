#!/usr/bin/env python3
"""
Auto-parallel MIC pathways launcher
"""
import subprocess
import os
from pathlib import Path
from datetime import datetime


def log(message):
    """Log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {message}"
    print(msg, flush=True)


def submit_parallel_jobs():
    """Submit all parallel pathways jobs automatically"""
    log("Submitting parallel pathways jobs...")

    # Find all datapackages
    datapackage_dir = Path("/data/user/hahnme_a/MIC_exercise/python_scripts/premise/datapackages")
    datapackages = list(datapackage_dir.glob("MIC*.zip"))

    if not datapackages:
        log("ERROR: No datapackages found!")
        return 1

    years = [2020, 2025, 2030, 2035, 2040, 2045, 2050, 2060, 2070, 2080, 2090, 2100]

    total_jobs = len(datapackages) * len(years)
    log(f"Found {len(datapackages)} datapackages and {len(years)} years = {total_jobs} total jobs")

    submitted_jobs = []
    task_num = 0

    for datapackage in datapackages:
        for year in years:
            task_num += 1
            
            # Submit individual pathways job for this datapackage-year combination
            cmd = ["sbatch", "pathways-sbatch.sh", str(datapackage), str(year), str(task_num), str(total_jobs)]

            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    job_id = result.stdout.strip().split()[-1]
                    submitted_jobs.append({
                        "datapackage": datapackage.name,
                        "year": year,
                        "job_id": job_id,
                        "task_num": task_num
                    })
                    log(f"Submitted {datapackage.name} - Year {year}: job {job_id}")
                else:
                    log(f"ERROR submitting {datapackage.name} - Year {year}: {result.stderr}")
            except Exception as e:
                log(f"ERROR submitting {datapackage.name} - Year {year}: {str(e)}")

    log(f"Successfully submitted {len(submitted_jobs)} pathways jobs")
    return 0


def main():
    """Main execution - auto-parallelization"""
    log("Starting auto-parallel pathways processing")

    try:
        return submit_parallel_jobs()
    except Exception as e:
        log(f"FATAL ERROR: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
