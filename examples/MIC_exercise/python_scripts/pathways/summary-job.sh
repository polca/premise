#!/bin/bash
#SBATCH --cluster=merlin7
#SBATCH --partition=hourly
#SBATCH --job-name=launch_all
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1
#SBATCH --time=00:05:00
#SBATCH --output=logs/launcher_%j.out
#SBATCH --error=logs/launcher_%j.err

# run_all.sh - Ultra-parallel processing launcher

set -e

echo "======================================"
echo "PARALLEL PROCESSING LAUNCHER"
echo "======================================"
echo "Launcher Job ID: $SLURM_JOB_ID"
echo "Start time: $(date)"
echo ""

# Create directories
mkdir -p logs
rm -rf temp_results
mkdir -p temp_results

echo "Finding all .gzip files..."
FILES=(pathways_results/*.gzip)
NUM_FILES=${#FILES[@]}

if [ $NUM_FILES -eq 0 ]; then
    echo "ERROR: No .gzip files found in pathways_results/"
    exit 1
fi

echo "Found $NUM_FILES files"
echo ""

# Array to store job IDs
JOB_IDS=()

# Submit one job per file
echo "Submitting processing jobs..."
for FILE in "${FILES[@]}"; do
    FILENAME=$(basename "$FILE")

    JOB_OUTPUT=$(sbatch --parsable \
        --cluster=merlin7 \
        --partition=hourly \
        --job-name="proc_${FILENAME:0:15}" \
        --mem=32G \
        --cpus-per-task=1 \
        --time=00:30:00 \
        --output=logs/file_%j.out \
        --error=logs/file_%j.err \
        --wrap="module use unstable && module load anaconda/2024.08 && conda run -p ~/envs/pathways python process_summary.py file $FILENAME")

    JOB_ID=${JOB_OUTPUT%;*}
    JOB_IDS+=($JOB_ID)
    echo "  [$JOB_ID] $FILENAME"
done

echo ""
echo "All $NUM_FILES processing jobs submitted!"
echo ""

# Build dependency string
DEPENDENCY=$(IFS=:; echo "${JOB_IDS[*]}")

# DIAGNOSTIC LINES - ADD THESE
echo ""
echo "=== DIAGNOSTICS ==="
echo "Job IDs submitted: ${JOB_IDS[@]}"
echo "Number of jobs: ${#JOB_IDS[@]}"
echo "Dependency string: afterok:$DEPENDENCY"
echo "==================="
echo ""

# Submit merge job
echo "Submitting merge job..."
MERGE_JOB=$(sbatch --parsable \
    --cluster=merlin7 \
    --partition=hourly \
    --job-name=merge_all \
    --mem=64G \
    --cpus-per-task=1 \
    --time=00:15:00 \
    --dependency=afterok:$DEPENDENCY \
    --output=logs/merge_%j.out \
    --error=logs/merge_%j.err \
    --wrap="module use unstable && module load anaconda/2024.08 && conda run -p ~/envs/pathways python process_summary.py merge")

echo "  [$MERGE_JOB] Merge job"
echo ""
echo "======================================"
echo "LAUNCH COMPLETE!"
echo "======================================"
echo ""
echo "Processing jobs: ${JOB_IDS[@]}"
echo "Merge job: $MERGE_JOB"
echo ""
echo "Monitor: watch -n 5 'squeue -u \$USER'"
echo "Logs: tail -f logs/file_*.out"
echo ""
echo "Expected completion: ~10-15 minutes"
echo "======================================"
echo "Launcher completed at: $(date)"
