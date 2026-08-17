"""
CoChem-NODE: Stage 3.1 - The Slurm Watchdog
Provides a network-safe polling mechanism to check remote job status
using exponential backoff to prevent SSH connection bans.
"""

import time
import logging
import os
import shlex
import subprocess
from typing import Tuple, Optional
from pathlib import Path

# Import CoChem modules
import sys
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

try:
    from cochem_node_connection_bridge import NodeBridge, CoChemHPCError
except ImportError:
    from Libraries.cochem_node_connection_bridge import NodeBridge, CoChemHPCError

logger = logging.getLogger("CoChem_NODE_Watchdog")
logger.setLevel(logging.INFO)

class SlurmWatchdog:
    """
    Monitors HPC job status via `squeue` with exponential backoff.
    """
    def __init__(self, bridge: Optional[NodeBridge] = None) -> None:
        self.bridge = bridge or NodeBridge()
        
        # Exponential Backoff Parameters
        self.base_delay = 5.0      # Start with a 5-second delay
        self.max_delay = 300.0     # Cap the delay at 5 minutes
        self.backoff_factor = 1.5  # Multiplier for each subsequent check
        self.current_delay = self.base_delay

    def _ensure_connection(self) -> None:
        if self.bridge.client is not None:
            if not self.bridge.client.get_transport() or not self.bridge.client.get_transport().is_active():
                logger.info("Watchdog: Re-establishing SSH connection...")
                self.bridge.establish_heartbeat()

    def _query_sacct_status(self, job_id: str) -> str:
        """
        Queries sacct to verify exact exit status when job is missing from squeue (NODE-15).
        """
        if self.bridge.client is None:
            return self._query_local_job_status(job_id)

        safe_job_id = shlex.quote(str(job_id))
        command = f"sacct -j {safe_job_id} -h -o State"
        try:
            stdin, stdout, stderr = self.bridge.client.exec_command(command)
            sacct_out = stdout.read().decode('utf-8').strip().upper()
            if sacct_out:
                if any(err in sacct_out for err in ["FAILED", "CANCELLED", "OUT_OF_MEMORY", "NODE_FAIL", "TIMEOUT"]):
                    logger.error(f"sacct verified job {job_id} failed with state: {sacct_out}")
                    return "FAILED"
                elif "COMPLETED" in sacct_out:
                    return "COMPLETED"
        except Exception as e:
            logger.warning(f"sacct fallback query failed for job {job_id}: {e}")
            
        # Default fallback: status truly unknown — do NOT assume COMPLETED (MOCK-17)
        return "UNKNOWN"

    def check_job_status(self, job_id: str) -> str:
        """
        Executes `squeue` for the specific Job ID.
        Returns one of: PENDING, RUNNING, COMPLETED, FAILED, UNKNOWN
        """
        self._ensure_connection()
        if self.bridge.client is None:
            return self._query_local_job_status(job_id)
            
        safe_job_id = shlex.quote(str(job_id))
        command = f"squeue -j {safe_job_id} -h -O State"
        
        try:
            stdin, stdout, stderr = self.bridge.client.exec_command(command)
            output = stdout.read().decode('utf-8').strip()
            err_output = stderr.read().decode('utf-8').strip()
            
            if err_output:
                # If squeue returns an error like "Invalid job id", check sacct exit state (NODE-15)
                if "Invalid job id" in err_output or "slurm_load_jobs error" in err_output:
                    logger.info(f"Job {job_id} not in active squeue. Querying sacct for exact status...")
                    return self._query_sacct_status(job_id)
                logger.error(f"squeue error for job {job_id}: {err_output}")
                return "UNKNOWN"
                
            if not output:
                 logger.info(f"Job {job_id} returned empty squeue state. Querying sacct...")
                 return self._query_sacct_status(job_id)

            # Slurm states: PENDING, RUNNING, SUSPENDED, COMPLETING, COMPLETED, CANCELLED, FAILED, TIMEOUT
            state = output.upper()
            
            if state in ["PENDING", "RUNNING", "COMPLETING"]:
                return state
            elif state == "COMPLETED":
                return "COMPLETED"
            elif state in ["CANCELLED", "FAILED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY"]:
                logger.error(f"Job {job_id} terminated abnormally with state: {state}")
                return "FAILED"
            else:
                logger.warning(f"Unrecognized Slurm state '{state}' for job {job_id}")
                return "UNKNOWN"
                
        except Exception as e:
            logger.error(f"Network failure while polling job {job_id}: {e}")
            return "UNKNOWN"

    def wait_for_completion(self, job_id: str, timeout_hours: float = 24.0) -> bool:
        """
        Blocks the local thread, polling the cluster until the job finishes or fails.
        Implements exponential backoff to protect the remote login node.
        """
        start_time = time.time()
        timeout_seconds = timeout_hours * 3600
        
        logger.info(f"Watchdog started for Job {job_id}. Timeout: {timeout_hours}h")
        self.current_delay = self.base_delay # Reset delay

        while True:
            if (time.time() - start_time) > timeout_seconds:
                logger.error(f"Watchdog timeout reached for Job {job_id}.")
                return False

            status = self.check_job_status(job_id)
            
            if status == "COMPLETED":
                logger.info(f"✅ Job {job_id} has completed.")
                return True
            elif status == "FAILED":
                logger.error(f"❌ Job {job_id} failed on the cluster.")
                return False
            
            logger.info(f"Job {job_id} status: {status}. Next check in {int(self.current_delay)} seconds...")
            time.sleep(self.current_delay)
            self.current_delay = min(self.current_delay * self.backoff_factor, self.max_delay)

    def _query_local_job_status(self, job_id: str) -> str:
        """
        MOCK-17 fix: queries local process table or job log exit codes
        instead of blindly returning "COMPLETED".

        Checks:
            1. Job log directory for an exit code marker file.
            2. Local process table (Unix: ``ps``; Windows: ``tasklist``).
        Returns one of: COMPLETED, FAILED, RUNNING, UNKNOWN.
        """
        # 1. Check for job log exit code files written by the payload wrapper
        log_candidates = [
            Path("HPC_Payloads") / "logs" / f"{job_id}.exit",
            Path("cochem_node_data") / "logs" / f"{job_id}.exit",
        ]
        for exit_file in log_candidates:
            if exit_file.is_file():
                try:
                    exit_code = int(exit_file.read_text().strip())
                    if exit_code == 0:
                        logger.info(f"Local job {job_id}: exit code 0 → COMPLETED.")
                        return "COMPLETED"
                    else:
                        logger.error(f"Local job {job_id}: exit code {exit_code} → FAILED.")
                        return "FAILED"
                except (ValueError, OSError) as exc:
                    logger.warning(f"Could not parse exit file {exit_file}: {exc}")

        # 2. Check local process table for a running process matching the job_id
        try:
            import psutil
            try:
                pid = int(job_id)
                if psutil.pid_exists(pid):
                    logger.info(f"Local job {job_id} found in process table -> RUNNING.")
                    return "RUNNING"
            except ValueError:
                logger.warning(f"Job ID {job_id} is not a valid integer PID.")
        except ImportError:
            logger.warning("psutil not found. Could not query local process table.")
        except Exception as exc:
            logger.warning(f"Could not query local process table: {exc}")

        logger.warning(f"Local job {job_id}: no exit file or running process found -> UNKNOWN.")
        return "UNKNOWN"

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        watchdog = SlurmWatchdog()
        logger.info("Watchdog initialized successfully.")
    except Exception as e:
        logger.error(f"Watchdog Error: {e}")