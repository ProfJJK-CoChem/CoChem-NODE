# CoChem-NODE

**CoChem-NODE** is the massive HPC Job Dispatch and Hardware-Scaling engine of the extended CoChem suite.

It is responsible for:
- Auto-compiling dynamic Slurm (`sbatch`) scripts specifically tailored for the target module (e.g., locking GPU nodes for JAX/MACE, and CPU fat-nodes for ORCA CCSD).
- Managing thousands of independent micro-tasks via an asynchronous ZeroMQ message broker instead of classical polling.
- Deploying GPUDirect Storage / NVLink optimizations to stream the massive `cochem_state.h5` tensor across the distributed network via Parallel HDF5.
- Tracking job states, implementing spot-instance checkpoint restarts, and guaranteeing the architectural time-tiers via deep telemetry profiling.

## Usage
Please refer to the authoritative `CoChem_Master_User_Manual.md` located in the `CoChem-BASE` repository for full execution instructions across the entire pipeline.