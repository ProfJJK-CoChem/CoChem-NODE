# CoChem-NODE: Execution Workflow (2026-08-07)

## Phase 1: Resource Profiling
1. **Node Handshake:** CoChem-BASE queries NODE. NODE authenticates via SSH and surveys the HPC cluster (via `sinfo`), mapping available CPU/GPU nodes, queue limits, and memory.
2. **Task Batching:** NODE calculates the optimal `#SBATCH --cpus-per-task` and `--mem` based on the incoming module's requirement (e.g., ORCA needs MPI/OpenMP balance; JAX needs exclusive GPU access).

## Phase 2: Dispatch & Execution
1. **Payload Injection:** The serialized `cochem_state.h5` and input scripts are synced to the remote scratch drive via `rsync`.
2. **Slurm Submission:** NODE submits the auto-generated `sbatch` scripts and activates the ZeroMQ listener.
3. **Dynamic Checkpointing:** If a spot-instance node is evicted by the HPC scheduler, NODE catches the `SIGTERM`, saves the ORCA SCF iteration, and resubmits to a higher-priority queue.

## Phase 3: Retrieval & Reporting
1. **Telemetry Dashboard:** The Jupyter UI displays a live color-coded node map, showing exactly where CoChem tasks are executing and their CPU/GPU utilization %.
2. **Data Aggregation:** Upon completion, NODE compresses the output logs (`.tar.zst`), securely transfers them back to the local host, and cleans the remote scratch drive.
