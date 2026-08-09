#!/bin/bash
#SBATCH --job-name=CoChem_TestEngine
#SBATCH --partition=normal
#SBATCH --array=1-25
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --mem=8192M
#SBATCH --time=24:00:00
#SBATCH --output=CoChem_TestEngine_%j.out
#SBATCH --error=CoChem_TestEngine_%j.err

# --- CoChem-NODE HPC Submission Bridge ---
# Auto-generated on submission. Do not edit manually.

echo "Job started on $(hostname) at $(date)"

# Purge and load required modules (if specified)
module purge


module load openmpi/4.1.8



# Set execution environment variables
export OMP_NUM_THREADS=4
export TMPDIR=/scratch/${USER}/${SLURM_JOB_ID}
mkdir -p $TMPDIR

# Change to execution directory
cd D:\GitHub-Repo\CoChem-NODE

echo "Executing Engine: TestEngine"
echo "Command: python TestEngine_payload.py --task_id $SLURM_ARRAY_TASK_ID"

# --- EXECUTION BLOCK ---
time python TestEngine_payload.py --task_id $SLURM_ARRAY_TASK_ID

# --- CLEANUP BLOCK ---
# Trap errors and clean scratch to prevent quota issues (Improvement Suggestion 11)
exit_code=$?
echo "Job finished at $(date) with exit code $exit_code"
rm -rf $TMPDIR

exit $exit_code