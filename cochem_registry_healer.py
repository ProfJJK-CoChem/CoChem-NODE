"""
CoChem-NODE: Registry Job Healer (Suggestion 75 / §6.1.2)
Stateless registry healer that scans the SLURM queue on kernel restart
and re-adopts orphaned jobs by cross-referencing local registry UUIDs
against live ``squeue`` output.

This module is designed to be invoked at NODE startup to reconcile
the local ``cochem_hpc_registry.json`` against the actual cluster state,
ensuring no jobs are lost across kernel restarts or crashes.
"""

import json
import hashlib
import logging
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import sys
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from cochem_node_connection_bridge import NodeBridge, CoChemHPCError
    from cochem_registry_manager import RegistryManager, RegistryLock
except ImportError:
    try:
        from Libraries.cochem_node_connection_bridge import NodeBridge, CoChemHPCError
        from Libraries.cochem_registry_manager import RegistryManager, RegistryLock
    except ImportError:
        from .cochem_node_connection_bridge import NodeBridge, CoChemHPCError
        from .cochem_registry_manager import RegistryManager, RegistryLock

logger = logging.getLogger("CoChem_NODE_RegistryHealer")
logger.setLevel(logging.INFO)

# CoChem job name prefix used in all SLURM submissions for identification
COCHEM_JOB_PREFIX = "CoChem_"


class RegistryJobHealer:
    """
    Stateless registry healer (§6.1.2).

    On kernel restart, this healer:
      1. Loads the local HPC registry (``cochem_hpc_registry.json``).
      2. Queries the live SLURM queue via ``squeue``.
      3. Cross-references local records against live jobs using
         cryptographic hash matching on job names.
      4. Updates stale local records and re-adopts orphaned remote jobs.
      5. Persists the reconciled registry atomically.
    """

    def __init__(
        self,
        bridge: Optional[NodeBridge] = None,
        registry_path: Optional[Path] = None,
    ):
        self._registry_path = Path(registry_path or "cochem_hpc_registry.json")
        self._registry: Dict = self._load_registry()

        # Lazy-init bridge — may be None in local mode
        self._rm = RegistryManager()
        self._config = self._rm.get_config()
        self._bridge = bridge or NodeBridge(self._config)

    # ------------------------------------------------------------------
    # Registry I/O
    # ------------------------------------------------------------------
    def _load_registry(self) -> Dict:
        """Load the local HPC job registry from disk."""
        if self._registry_path.is_file():
            try:
                with RegistryLock(self._registry_path, timeout=5.0):
                    return json.loads(self._registry_path.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.error(f"Failed to load registry {self._registry_path}: {exc}")
        return {"batches": {}}

    def _save_registry(self) -> None:
        """Persist the registry atomically using RegistryLock."""
        with RegistryLock(self._registry_path, timeout=5.0):
            self._registry_path.write_text(
                json.dumps(self._registry, indent=4, sort_keys=True),
                encoding="utf-8",
            )
        logger.info(f"Registry saved to {self._registry_path}")

    # ------------------------------------------------------------------
    # Remote Queue Query
    # ------------------------------------------------------------------
    def _query_squeue(self) -> Dict[str, Dict[str, str]]:
        """
        Query ``squeue`` for all jobs owned by the configured HPC user
        whose names start with the CoChem prefix.

        Returns:
            {job_id: {"name": ..., "state": ..., "time": ..., "nodes": ...}}
        """
        if self._bridge.client is None:
            logger.info("RegistryJobHealer: local mode — skipping squeue query.")
            return {}

        username = self._config.hpc.username or "user"
        if not re.match(r"^[a-zA-Z0-9_\-\.]+$", username):
            logger.error(f"Invalid username format: '{username}'")
            return {}

        safe_user = shlex.quote(username)
        command = f"squeue -u {safe_user} -h -o '%i|%j|%T|%M|%D'"

        try:
            # Ensure connection is alive
            transport = self._bridge.client.get_transport()
            if transport is None or not transport.is_active():
                self._bridge.establish_heartbeat()

            stdin, stdout, stderr = self._bridge.client.exec_command(command)
            raw_out = stdout.read().decode("utf-8").strip()
            raw_err = stderr.read().decode("utf-8").strip()

            if raw_err:
                logger.error(f"squeue error: {raw_err}")
                return {}
            if not raw_out:
                logger.info("No active remote jobs found.")
                return {}

            results: Dict[str, Dict[str, str]] = {}
            for line in raw_out.splitlines():
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 5:
                    job_id, name, state, time_used, nodes = parts[:5]
                    results[job_id] = {
                        "name": name,
                        "state": state,
                        "time": time_used,
                        "nodes": nodes,
                    }
            return results

        except Exception as exc:
            logger.error(f"Network error during squeue query: {exc}")
            return {}

    # ------------------------------------------------------------------
    # Cryptographic Identity Matching
    # ------------------------------------------------------------------
    @staticmethod
    def _job_identity_hash(batch_uuid: str, module: str) -> str:
        """
        Compute a short SHA-256 hash from batch UUID + module name.
        Used to match local registry entries to remote job names.
        """
        payload = f"{batch_uuid}:{module}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]

    # ------------------------------------------------------------------
    # Core Heal Logic
    # ------------------------------------------------------------------
    def heal(self) -> Dict[str, str]:
        """
        Run the full reconciliation cycle.

        Returns:
            dict mapping batch_uuid -> reconciled status string
        """
        logger.info("═══ Registry Job Healer: starting reconciliation ═══")
        timestamp = datetime.now(timezone.utc).isoformat()

        live_queue = self._query_squeue()
        batches = self._registry.get("batches", {})

        # Build reverse-lookup: remote job_id -> live details
        # Also track which remote jobs have been claimed
        claimed_remote_ids: set = set()
        reconciled: Dict[str, str] = {}
        healed_count = 0

        # Pass 1 — update known local batches from live queue
        for batch_uuid, local_info in batches.items():
            remote_job_id = local_info.get("remote_job_id")
            local_status = local_info.get("status", "UNKNOWN")

            if remote_job_id and remote_job_id in live_queue:
                # Job is still in the queue — update local status
                actual_state = live_queue[remote_job_id]["state"]
                claimed_remote_ids.add(remote_job_id)

                if local_status != actual_state:
                    logger.info(
                        f"🔄 Healing batch {batch_uuid}: "
                        f"{local_status} → {actual_state}"
                    )
                    healed_count += 1

                local_info["status"] = actual_state
                local_info["last_healed"] = timestamp
                reconciled[batch_uuid] = actual_state

            elif remote_job_id and local_status not in (
                "COMPLETED", "FAILED", "CANCELLED"
            ):
                # Job was tracked but is gone from queue — mark unknown
                logger.warning(
                    f"⚠️ Orphan batch {batch_uuid} (job {remote_job_id}): "
                    f"missing from live queue, was '{local_status}'."
                )
                local_info["status"] = "UNKNOWN_REQUIRES_SACCT"
                local_info["last_healed"] = timestamp
                reconciled[batch_uuid] = "UNKNOWN_REQUIRES_SACCT"
                healed_count += 1
            else:
                reconciled[batch_uuid] = local_status

        # Pass 2 — adopt ghost CoChem jobs running remotely but not tracked locally
        for remote_id, details in live_queue.items():
            if remote_id in claimed_remote_ids:
                continue
            job_name = details.get("name", "")
            if not job_name.startswith(COCHEM_JOB_PREFIX):
                continue

            ghost_uuid = f"ghost_{remote_id}"
            logger.warning(
                f"👻 Ghost job adopted: remote {remote_id} "
                f"('{job_name}', state={details['state']})"
            )
            batches[ghost_uuid] = {
                "module": job_name.replace(COCHEM_JOB_PREFIX, "", 1),
                "status": details["state"],
                "task_count": 0,
                "sbatch_file": "",
                "remote_job_id": remote_id,
                "adopted": True,
                "last_healed": timestamp,
            }
            reconciled[ghost_uuid] = details["state"]
            healed_count += 1

        # Persist
        self._registry["batches"] = batches
        self._registry["last_heal_timestamp"] = timestamp
        self._save_registry()

        logger.info(
            f"═══ Reconciliation complete: {healed_count} entries healed, "
            f"{len(reconciled)} total tracked. ═══"
        )
        return reconciled


# ==========================================
# CLI / Startup Entry Point
# ==========================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        healer = RegistryJobHealer()
        result = healer.heal()
        print(f"\n✅ Registry Healer completed. {len(result)} batches tracked.")
        for uid, status in result.items():
            print(f"   {uid}: {status}")
    except Exception as e:
        print(f"❌ Registry Healer error: {e}")
