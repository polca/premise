#!/bin/bash
#SBATCH --cluster=merlin7
#SBATCH --partition=hourly
#SBATCH --time=1:00:00
#SBATCH --hint=nomultithread
#SBATCH --job-name=MIC_launcher
#SBATCH --cpus-per-task=4
#SBATCH --ntasks=1
#SBATCH --output=logs/MIC_launcher_%j.out
#SBATCH --error=logs/MIC_launcher_%j.err
#SBATCH --mem=16G

module use unstable
module load anaconda/2024.08

mkdir -p logs

echo "=== MIC Auto-Parallel Launcher Started ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start time: $(date)"
echo "=============================="

# Run the auto-parallel script
conda run -p ~/envs/premise_MIC python /data/user/hahnme_a/MIC_exercise/python_scripts/premise/MIC-datapackages.py

echo "=============================="
echo "Launcher completed at: $(date)"
echo "=============================="
