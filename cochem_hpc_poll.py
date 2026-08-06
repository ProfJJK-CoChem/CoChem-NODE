"""
CoChem-NODE: Stage 3.1 - The Slurm Watchdog
Provides a network-safe polling mechanism to check remote job status
using exponential backoff to prevent SSH connection bans.
"""

import time
import logging
from typing import Tuple, Optional
from pathlib import Path

# Import CoChem modules
import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from CoChem_NODE.cochem_node_bridge import NodeBridge, CoChemHPCError

logger = logging.getLogger("CoChem_NODE_Watchdog")
logger.setLevel(logging.INFO)

class SlurmWatchdog:
    """
    Monitors HPC job status via `squeue` with exponential backoff.
    """
    def __init__(self, bridge: Optional[NodeBridge] = None):
        self.bridge = bridge or NodeBridge()
        
        # Exponential Backoff Parameters
        self.base_delay = 5.0      # Start with a 5-second delay
        self.max_delay = 300.0     # Cap the delay at 5 minutes
        self.backoff_factor = 1.5  # Multiplier for each subsequent check
        self.current_delay = self.base_delay

    def _ensure_connection(self):
        if not self.bridge.client.get_transport() or not self.bridge.client.get_transport().is_active():
            logger.info("Watchdog: Re-establishing SSH connection...")
            self.bridge.establish_heartbeat()

    def check_job_status(self, job_id: str) -> str:
        """
        Executes `squeue` for the specific Job ID.
        Returns one of: PENDING, RUNNING, COMPLETED, FAILED, UNKNOWN
        """
        self._ensure_connection()
        
        # We use squeue formatting to just get the state (%T) for the specific job
        command = f"squeue -j {job_id} -h -O State"
        
        try:
            stdin, stdout, stderr = self.bridge.client.exec_command(command)
            output = stdout.read().decode('utf-8').strip()
            err_output = stderr.read().decode('utf-8').strip()
            
            if err_output:
                # If squeue returns an error, it often means the job ID is no longer in the active queue
                if "Invalid job id" in err_output:
                    # Job has likely finished or failed and fallen out of squeue. 
                    # A robust implementation would then check `sacct` or `seff`.
                    # For now, we will mark it as COMPLETED to trigger the artifact retrieval step,
                    # which will definitively check for the output file.
                    logger.info(f"Job {job_id} not in squeue. Assuming COMPLETED/TERMINATED.")
                    return "COMPLETED"
                logger.error(f"squeue error for job {job_id}: {err_output}")
                return "UNKNOWN"
                
            if not output:
                 # Empty output also implies the job is gone from the active queue
                 logger.info(f"Job {job_id} returned empty state. Assuming COMPLETED/TERMINATED.")
                 return "COMPLETED"

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
            
            # If PENDING, RUNNING, or UNKNOWN, wait and increase delay
            logger.info(f"Job {job_id} status: {status}. Next check in {int(self.current_delay)} seconds...")
            time.sleep(self.current_delay)
            
            # Apply exponential backoff
            self.current_delay = min(self.current_delay * self.backoff_factor, self.max_delay)

if __name__ == "__main__":
    # Diagnostic / Test Run
    logging.basicConfig(level=logging.INFO)
    try:
        watchdog = SlurmWatchdog()
        # Note: Replace '123456' with a real job ID if testing live
        print("Testing Watchdog Polling Logic (Simulated):")
        # watchdog.wait_for_completion("123456", timeout_hours=0.1)
        print("Watchdog initialized successfully.")
    except Exception as e:
         print(f"Watchdog Error: {e}")