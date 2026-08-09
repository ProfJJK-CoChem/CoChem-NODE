# CoChem-NODE: Architectural Changes (2026-08-07)

## 1. ZeroMQ Asynchronous Message Broker
**Target File:** `node_core/broker.py`
**Required Architectural Change:**
- NODE must transition from synchronous SSH `paramiko` polling to an asynchronous ZeroMQ message queue to track Slurm job status (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`) across thousands of micro-tasks.

## 2. Hardware Profiling & Routing
**Target File:** `node_core/router.py`
**Required Architectural Change:**
- Implement CPU thread pinning (`hwloc`) to avoid L3 cache thrashing on AMD EPYC architectures. Route massive CCSD(T) memory jobs strictly to 1TB+ RAM fat-nodes. Route MACE AI evaluations exclusively to A100/H100 GPU nodes.

## 3. GPUDirect NVLink Serialization
**Target File:** `node_core/tensor_io.py`
**Required Architectural Change:**
- To handle the massive `cochem_state.h5` tensor across a distributed network, NODE must implement parallel HDF5 (pHDF5) utilizing GPUDirect Storage, bypassing the CPU PCIe bottleneck.
