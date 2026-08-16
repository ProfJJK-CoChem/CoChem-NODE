# SDPM REPORT: CoChem-NODE Modernization Plan (v4 Method Matrix Compliance)

## Executive Summary
This document outlines the required architectural upgrades to `CoChem-NODE` to achieve full compliance with the CoChem v4.1 Method Matrix. The primary objective is to eradicate legacy mock execution paths ("MOCK-16", "MOCK-17") in local execution modes, enforce robust Pydantic validations, and fully implement the Scout-and-Anchor Co-Scheduling bounds (§8A.2).

## Current State Analysis & Deficits

The current codebase contains several scaffolding artifacts that violate production readiness:
1.  **`cochem_job_batcher.py` (MOCK-16):** Task ingestion historically relied on hardcoded loops generating dummy `isomer_i.xyz` payloads. While partially mitigated by `load_candidate_geometries`, the absence of genuine geometry ingestion validation persists.
2.  **`cochem_hpc_poll.py` (MOCK-17):** When executing locally without an HPC backend, `_query_local_job_status` falls back to querying the local process table or job log exit codes. However, local dispatch in `cochem_hpc_dispatch.py` simply returns a `LOCAL_JOB_{name}` string without actually spawning a process, guaranteeing a failed poll state later.
3.  **`cochem_hpc_dispatch.py` (Local Fallback):** The `dispatch_job` and `dispatch_co_scheduled_pair` methods contain `else:` blocks for local execution that merely log the intent to dispatch and immediately return success. **No actual local execution takes place.**
4.  **Schema Enforcement (`cochem_registry_manager.py`):** While v4 Pydantic models are defined in `cochem_registry_schema.py`, error handling during `load()` and `save()` lacks rigorous recovery strategies beyond raising raw exceptions.

## Actionable Remediation Plan (WBS)

### Task 1: Eradicate "Fake" Local Dispatch (`cochem_hpc_dispatch.py`)
**Objective:** Replace the mock local return with genuine local subprocess execution.
*   **Subtask 1.1:** Import `safe_subprocess_run` or `subprocess.Popen` into `cochem_hpc_dispatch.py`.
*   **Subtask 1.2:** In the `else:` block of `dispatch_job` (when `self.bridge.client is None`), render the bash script locally via `self.templater.render_job()`.
*   **Subtask 1.3:** Write the rendered script to `local_tmp_script`.
*   **Subtask 1.4:** Execute the script asynchronously using `subprocess.Popen`. Capture the PID as the returned `job_id`.
*   **Subtask 1.5:** Replicate this logic for `dispatch_co_scheduled_pair`, launching both Anchor and Scout processes locally and returning their PIDs.

### Task 2: Synchronize Polling with Local PIDs (`cochem_hpc_poll.py`)
**Objective:** Align `_query_local_job_status` to correctly monitor the PIDs returned by Task 1.
*   **Subtask 2.1:** Modify the local process check in `_query_local_job_status` to accept numerical PIDs (as strings) instead of arbitrary `LOCAL_JOB_name` strings.
*   **Subtask 2.2:** Use `ps -p <PID>` (Unix) or `tasklist /FI "PID eq <PID>"` (Windows) to accurately determine if the process is still running.

### Task 3: Strengthen Pydantic Schema Validation
**Objective:** Ensure configuration inputs strictly adhere to the v4 Method Matrix boundary constraints (e.g., LVT thresholds, Wall-Clock Tiers).
*   **Subtask 3.1:** Review `cochem_registry_schema.py` to ensure `CoChemConfig` validates the 10-Tier Wall-Clock Logic (T0 to T9).
*   **Subtask 3.2:** Ensure Product A/B/C constraints are representable or validatable within the config schema.

### Task 4: Anti-Spoofing & Test Fidelity
**Objective:** Prevent simulation of HPC status and ensure tests reflect production.
*   **Subtask 4.1:** The `_get_remote_checksum` and `_get_local_checksum` in `cochem_hpc_sync.py` currently provide artifact integrity. We must ensure the bridge actively rejects simulated responses.
*   **Subtask 4.2:** Design unit tests for `cochem_hpc_dispatch.py` that utilize a local execution environment, verifying that genuine PIDs are returned and polled correctly by `cochem_hpc_poll.py`.

## Execution Handoff
The immediate next step is to assign the `cochem-improve` agent to implement **Task 1: Eradicate "Fake" Local Dispatch in `cochem_hpc_dispatch.py`**.
