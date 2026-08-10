#!/bin/bash
#SBATCH --job-name=CoChem_TestModule
#SBATCH --partition=compute
#SBATCH --array=1-25
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --mem=32000M
#SBATCH --time=00:30:00
#SBATCH --output=CoChem_TestModule_%j.out
#SBATCH --error=CoChem_TestModule_%j.err


# --- CoChem-NODE Core Pinning & Topology (§8A.0) ---
export KMP_HW_SUBSET=8c:intel_core,1t
export OMP_NUM_THREADS=8
export TMPDIR=/scratch/${USER}/${SLURM_JOB_ID}
mkdir -p $TMPDIR



# Purge and load required modules
module purge


module load openmpi/4.1.8



cd D:\Gdrive\__CoChem\GitHub-Repo\CoChem-NODE

echo "Executing Engine: TestModule"
echo "Tier: T1-30min"
echo "Command: python TestModule_payload.py --task_id $SLURM_ARRAY_TASK_ID"

# --- EXECUTION BLOCK ---
time python TestModule_payload.py --task_id $SLURM_ARRAY_TASK_ID

exit_code=$?
echo "Job finished at $(date) with exit code $exit_code"



rm -rf $TMPDIR
exit $exit_code