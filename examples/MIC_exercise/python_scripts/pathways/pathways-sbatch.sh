#!/bin/bash
#SBATCH --cluster=merlin7
#SBATCH --partition=daily
#SBATCH --time=03:00:00
#SBATCH --hint=nomultithread
#SBATCH --job-name=MIC_pathways
#SBATCH --cpus-per-task=6
#SBATCH --ntasks=1
#SBATCH --output=logs/pathways_%j.out
#SBATCH --error=logs/pathways_%j.err
#SBATCH --mem=160G

module use unstable
module load anaconda/2024.08

# Arguments from launcher
DATAPACKAGE_PATH="$1"
YEAR="$2"
TASK_NUM="$3"
TOTAL_TASKS="$4"

BASE_DIR="/data/user/hahnme_a/MIC_exercise/python_scripts/pathways"
RESULTS_DIR="$BASE_DIR/pathways_results"
TEMP_DIR="$BASE_DIR/pathways_temp_${SLURM_JOB_ID}"

# Create directories
mkdir -p "$RESULTS_DIR"
mkdir -p "$TEMP_DIR"
mkdir -p "$BASE_DIR/logs"

cd "$TEMP_DIR"

DATAPACKAGE_NAME="$(basename "${DATAPACKAGE_PATH}" .zip)"

echo "Processing: $DATAPACKAGE_NAME ($TASK_NUM/$TOTAL_TASKS)"
echo "Start time: $(date)"

conda run -p ~/envs/pathways python "$BASE_DIR/MIC-pathways.py" \
    --datapackage "$DATAPACKAGE_PATH" \
    --year "$YEAR" \
    --output-suffix "$DATAPACKAGE_NAME"

# Move results to centralized location
if ls *results*.gzip 1> /dev/null 2>&1; then
    for file in *results*.gzip; do
        mv "$file" "$RESULTS_DIR/${DATAPACKAGE_NAME}__${YEAR}_${file}"
        echo "Moved: $file to $RESULTS_DIR/${DATAPACKAGE_NAME}_${YEAR}_${file}"
    done
else
    echo "No .gzip results found in $(pwd)"
    echo "Files present:"
    ls -la
fi

# Cleanup temp directory
cd "$BASE_DIR"
rm -rf "$TEMP_DIR"

echo "Completed: $DATAPACKAGE_NAME - Year: $YEAR at $(date)"

# Create completion marker
touch "$RESULTS_DIR/.completed_${DATAPACKAGE_NAME}_${YEAR}"

COMPLETED=$(ls -1 "$RESULTS_DIR"/.completed_* 2>/dev/null | wc -l)
echo "Progress: $COMPLETED/$TOTAL_TASKS jobs completed"
