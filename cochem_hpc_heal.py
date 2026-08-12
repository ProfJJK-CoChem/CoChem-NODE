"""
CoChem-NODE: Stage 3.3 - Registry Reconciliation (The Healer)
Recovers orphaned HPC jobs by cross-referencing local registry state
against the live remote Slurm queue.
"""

import logging
import shlex
import re
from pathlib import Path
from typing import Dict, List, Optional

# Import CoChem modules
import sys
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

try:
    from cochem_node_connection_bridge import NodeBridge, CoChemHPCError
    from cochem_registry_manager import RegistryManager
except ImportError:
    from Libraries.cochem_node_connection_bridge import NodeBridge, CoChemHPCError
    from Libraries.cochem_registry_manager import RegistryManager

logger = logging.getLogger("CoChem_NODE_Healer")
logger.setLevel(logging.INFO)

class RegistryHealer:
    """
    Synchronizes local tracking databases with the remote Slurm queue.
    """
    def __init__(self, bridge: Optional[NodeBridge] = None, registry_manager: Optional[RegistryManager] = None) -> None:
        self.registry_manager = registry_manager or RegistryManager()
        self.config = self.registry_manager.get_config()
        self.bridge = bridge or NodeBridge(self.config)

    def _ensure_connection(self) -> None:
        if self.bridge.client is not None:
            if not self.bridge.client.get_transport() or not self.bridge.client.get_transport().is_active():
                logger.info("RegistryHealer: Re-establishing SSH connection...")
                self.bridge.establish_heartbeat()

    def query_remote_queue(self) -> Dict[str, Dict[str, str]]:
        """
        Executes `squeue` to retrieve all jobs belonging to the configured user.
        Returns a dictionary mapping Job IDs to their status details.
        Sanitizes username parameter against command injection (NODE-09).
        """
        self._ensure_connection()
        username = self.config.hpc.username or "user"
        
        if not re.match(r'^[a-zA-Z0-9_\-\.]+$', username):
            logger.error(f"Invalid username format: '{username}'")
            return {}
            
        safe_username = shlex.quote(username)
        # Format: JobID|JobName|State|TimeUsed|Nodes
        command = f"squeue -u {safe_username} -h -o '%i|%j|%T|%M|%D'"
        
        active_remote_jobs = {}
        try:
            stdin, stdout, stderr = self.bridge.client.exec_command(command)
            output = stdout.read().decode('utf-8').strip()
            err_output = stderr.read().decode('utf-8').strip()
            
            if err_output:
                logger.error(f"Failed to query remote queue: {err_output}")
                return {}
                
            if not output:
                logger.info(f"No active remote jobs found for user {username}.")
                return {}

            for line in output.split('\n'):
                parts = line.split('|')
                if len(parts) >= 5:
                    job_id, name, state, time_used, nodes = [p.strip() for p in parts]
                    active_remote_jobs[job_id] = {
                        "name": name,
                        "state": state,
                        "time_used": time_used,
                        "nodes": nodes
                    }
                    
            return active_remote_jobs
            
        except Exception as e:
            logger.error(f"Network error during queue query: {e}")
            return {}

    def heal_registry(self, known_local_jobs: Dict[str, str]) -> Dict[str, str]:
        """
        Compares a local dictionary of {job_id: expected_state} against the live queue.
        Returns an updated dictionary reflecting the true state.
        """
        logger.info("Initiating Registry Reconciliation...")
        live_queue = self.query_remote_queue()
        
        updated_jobs = {}
        orphans_adopted = 0
        
        for job_id, local_state in known_local_jobs.items():
            if job_id in live_queue:
                actual_state = live_queue[job_id]['state']
                
                if local_state != actual_state:
                     logger.info(f"🔄 Healing State: Job {job_id} changed from {local_state} to {actual_state}")
                
                # We update our local record with the true remote state
                updated_jobs[job_id] = actual_state
            else:
                if local_state not in ["COMPLETED", "FAILED", "CANCELLED"]:
                    logger.warning(f"⚠️ Orphan Detected: Job {job_id} ({local_state}) is missing from remote queue.")
                    updated_jobs[job_id] = "UNKNOWN_REQUIRES_PULL"
                    orphans_adopted += 1
                else:
                    updated_jobs[job_id] = local_state
        
        for remote_id, details in live_queue.items():
            if remote_id not in known_local_jobs:
                logger.warning(f"👻 Ghost Job Detected: Remote Job {remote_id} ({details['name']}) is running but not tracked locally.")
                updated_jobs[remote_id] = details['state']
                orphans_adopted += 1

        logger.info(f"Reconciliation complete. {orphans_adopted} orphaned/ghost jobs adopted.")
        return updated_jobs

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        healer = RegistryHealer()
        logger.info("Registry Healer initialized.")
    except Exception as e:
        logger.error(f"Healer Error: {e}")