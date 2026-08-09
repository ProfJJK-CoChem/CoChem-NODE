# CoChem-NODE: Software Engineering Specification
**Target Phase:** Python Implementation

This document serves as the exact coding blueprint for the next LLM agent to construct the `CoChem-NODE` repository.

## 1. Directory & File Architecture
```text
CoChem-NODE/
├── node_core/
│   ├── __init__.py
│   ├── broker.py          # ZeroMQ asynchronous message listener
│   ├── hardware_map.py    # Slurm sinfo parser and CPU/GPU pinning
│   ├── sbatch_writer.py   # Dynamic HPC bash script compiler
│   └── pHDF5_sync.py      # GPUDirect parallel HDF5 I/O handler
├── tests/
│   ├── test_broker.py
│   └── test_sbatch_writer.py
├── requirements.txt       # pyzmq, h5py, paramiko
└── README.md
```

## 2. File-by-File Blueprint

### `node_core/broker.py`
- **Purpose:** Non-blocking async queue to talk to BASE and the HPC controller.
- **Functions:**
  - `async def listen_for_payloads(port: int) -> dict:`
    - *Returns:* JSON payload from BASE.
  - `async def broadcast_status(job_id: str, status: str):`
    - *Action:* Pushes `RUNNING`, `FAILED`, or `COMPLETED` back to BASE.

### `node_core/hardware_map.py`
- **Purpose:** Queries Slurm to find optimal nodes.
- **Functions:**
  - `def query_slurm_partitions() -> dict:`
    - *Returns:* Available nodes, mapping `[GPU_MAX]` to partition names.
  - `def pin_threads_hwloc(n_cores: int) -> str:`
    - *Returns:* Bash string for `hwloc-bind` to prevent AMD L3 cache thrashing.

### `node_core/sbatch_writer.py`
- **Purpose:** Compiles the batch script.
- **Functions:**
  - `def compile_sbatch(payload: dict, partition: str) -> str:`
    - *Returns:* A complete `#SBATCH` script with dynamically calculated `--mem` and `--cpus-per-task`.

## 3. Execution Data Flow (The Payload Trace)
1. **ZeroMQ Ingest:** `broker.py` constantly listens on a specific TCP port. When `BASE` sends a dispatch payload, NODE unpacks it.
2. **Hardware Matching:** `hardware_map.py` checks the payload tags (e.g., `[CPU_FAT]`). It pings the Slurm controller and selects the appropriate partition.
3. **Script Compilation:** `sbatch_writer.py` generates the exact bash execution script, dynamically injecting `module load` commands.
4. **Data Sync & Submission:** `pHDF5_sync.py` securely `rsync`s the subset of `cochem_state.h5` to the remote scratch drive. NODE executes `sbatch`.
5. **Monitoring & Retrieval:** NODE monitors the job via `squeue`. Upon completion, it zips the `.out` logs, syncs the updated pHDF5 block back to `BASE`, and broadcasts `COMPLETED`.

## 4. PyTest Roadmap
- **Test 1 (`test_sbatch_writer.py`):** Assert that a payload with `[GPU_MAX]` and a heavy RAM requirement accurately produces an `sbatch` string with `#SBATCH --gres=gpu:a100:1`.
- **Test 2 (`test_broker.py`):** Use PyTest-Asyncio to simulate `BASE` sending a payload and verify that `broker.py` correctly parses the JSON and pushes an acknowledgment.
