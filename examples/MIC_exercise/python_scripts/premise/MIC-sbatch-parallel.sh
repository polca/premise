#!/bin/bash
#SBATCH --cluster=merlin7
#SBATCH --partition=daily
#SBATCH --time=03:00:00
#SBATCH --hint=nomultithread
#SBATCH --job-name=MIC_parallel
#SBATCH --cpus-per-task=8
#SBATCH --ntasks=1
#SBATCH --output=logs/MIC_parallel_%j_%A.out
#SBATCH --error=logs/MIC_parallel_%j_%A.err
#SBATCH --mem=96G

module use unstable
module load anaconda/2024.08

mkdir -p logs

export PYTHONUNBUFFERED=1

echo "=== MIC Parallel Job Started ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start time: $(date)"
echo "=============================="

# Run the individual chunk job
conda run -p ~/envs/premise_MIC python /data/user/hahnme_a/MIC_exercise/python_scripts/premise/MIC-datapackages-chunk.py $1 $2 $3 $4

echo "=============================="
echo "Job completed at: $(date)"
echo "=============================="
