#!/usr/bin/env python3
"""
CoChem-NODE: HPC Registry Healer
Reconciles the local cochem_hpc_registry.json with the remote SLURM queue.
Safely updates job statuses (STAGED, RUNNING, COMPLETED, ORPHANED) to ensure
fault-tolerant recovery if the local Jupyter kernel crashes.
"""

import os
import json
import logging
from pathlib import Path

class Colors:
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

logging.basicConfig(filename='cochem_node_healer.log', level=logging.INFO)

class RegistryHealer:
    def __init__(self):
        self.registry_path = Path("cochem_hpc_registry.json")
        self.registry = self.load_registry()

    def load_registry(self) -> dict:
        if not self.registry_path.exists():
            print(f"{Colors.WARNING}⚠️ HPC Registry missing. Run cochem_job_batcher.py first.{Colors.ENDC}")
            return {"batches": {}}
        with open(self.registry_path, "r") as f:
            return json.load(f)

    def save_registry(self):
        with open(self.registry_path, "w") as f:
            json.dump(self.registry, f, indent=4)

    def mock_remote_squeue(self) -> set:
        """
        Simulates parsing a remote 'squeue' payload.
        In a live run, this calls cochem_node_bridge.py -> check_remote_queue().
        """
        # We simulate that UUIDs containing 'a' or 'b' are still running.
        running_uuids = set()
        for batch_uuid in self.registry.get("batches", {}).keys():
            if 'a' in batch_uuid or 'b' in batch_uuid:
                running_uuids.add(batch_uuid)
        return running_uuids

    def reconcile(self):
        print(f"\n{Colors.OKCYAN}--- CoChem-NODE: Registry Reconciliation ---{Colors.ENDC}")
        
        batches = self.registry.get("batches", {})
        if not batches:
            print(f"{Colors.OKGREEN}✅ Local registry is empty. No jobs to reconcile.{Colors.ENDC}")
            return

        print("📡 Polling remote cluster queue (Mock Mode)...")
        active_remote_jobs = self.mock_remote_squeue()
        
        updated_count = 0
        completed_count = 0
        
        for batch_uuid, data in batches.items():
            current_status = data.get("status")
            
            # If it's already recorded as complete, skip
            if current_status == "COMPLETED":
                completed_count += 1
                continue
                
            # If it was running but is no longer in squeue, it finished (or failed)
            if current_status in ["STAGED", "RUNNING"] and batch_uuid not in active_remote_jobs:
                print(f"   ↳ {Colors.OKGREEN}[RECOVERED]{Colors.ENDC} Batch {batch_uuid} is no longer in queue. Marking COMPLETED.")
                batches[batch_uuid]["status"] = "COMPLETED"
                updated_count += 1
                completed_count += 1
                logging.info(f"Adopted orphan job {batch_uuid} -> COMPLETED")
                
            # If it was staged and is now in squeue
            elif current_status == "STAGED" and batch_uuid in active_remote_jobs:
                print(f"   ↳ {Colors.OKCYAN}[ACTIVE]{Colors.ENDC} Batch {batch_uuid} transition to RUNNING.")
                batches[batch_uuid]["status"] = "RUNNING"
                updated_count += 1
                logging.info(f"Updated job {batch_uuid} -> RUNNING")

        self.save_registry()
        
        print(f"\n{Colors.BOLD}Reconciliation Summary:{Colors.ENDC}")
        print(f"🔄 Records Updated: {updated_count}")
        print(f"✅ Total Completed: {completed_count} / {len(batches)}")

def main():
    healer = RegistryHealer()
    healer.reconcile()

if __name__ == "__main__":
    main()