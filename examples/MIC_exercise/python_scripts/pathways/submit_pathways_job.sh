#!/bin/bash
#SBATCH --cluster=merlin7
#SBATCH --partition=hourly
#SBATCH --time=1:00:00
#SBATCH --hint=nomultithread
#SBATCH --job-name=MIC_pathways_launcher
#SBATCH --cpus-per-task=4
#SBATCH --ntasks=1
#SBATCH --output=logs/pathways_launcher_%j.out
#SBATCH --error=logs/pathways_launcher_%j.err
#SBATCH --mem=16G

module use unstable
module load anaconda/2024.08

mkdir -p logs

echo "=== MIC Pathways Auto-Parallel Launcher Started ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start time: $(date)"
echo "=============================="

conda run -p ~/envs/pathways python /data/user/hahnme_a/MIC_exercise/python_scripts/pathways/MIC-pathways-launcher.py

echo "=============================="
echo "Launcher completed at: $(date)"
echo "=============================="
