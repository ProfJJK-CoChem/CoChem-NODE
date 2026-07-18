#!/usr/bin/env python3
"""
CoChem-NODE: Job Array Batcher
Slices massive computational tasks into SLURM job arrays, generating
secure .sbatch templates and registering UUIDs locally for fault tolerance.
"""

import os
import json
import uuid
import logging
from pathlib import Path

class Colors:
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

logging.basicConfig(filename='cochem_node_batcher.log', level=logging.INFO)

class HPCBatcher:
    def __init__(self):
        self.registry_path = Path("cochem_hpc_registry.json")
        self.registry = self.load_registry()
        self.max_array_size = 1000 # Standard SLURM limit constraint

    def load_registry(self) -> dict:
        if self.registry_path.exists():
            with open(self.registry_path, "r") as f:
                return json.load(f)
        return {"batches": {}}

    def save_registry(self):
        with open(self.registry_path, "w") as f:
            json.dump(self.registry, f, indent=4)

    def generate_sbatch_template(self, batch_uuid: str, array_count: int, module_name: str) -> str:
        """Constructs the SLURM submission script."""
        
        template = f"""#!/bin/bash
#SBATCH --job-name=CoChem_{module_name}
#SBATCH --output=logs/{batch_uuid}_%A_%a.out
#SBATCH --error=logs/{batch_uuid}_%A_%a.err
#SBATCH --array=1-{array_count}
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --mem=32G

echo "Starting CoChem Task Array ID: $SLURM_ARRAY_TASK_ID"

# Load modules based on cochem_system_config
module load openmpi/4.1.8

# Execute payload matching the array ID
python {module_name}_payload.py --task_id $SLURM_ARRAY_TASK_ID

echo "Task $SLURM_ARRAY_TASK_ID completed."
"""
        return template

    def create_batch(self, task_list: list, module_name: str) -> list:
        """Splits tasks to respect SLURM array limits and registers them."""
        total_tasks = len(task_list)
        print(f"📦 Batching {total_tasks} tasks for HPC module: {module_name}")
        
        generated_scripts = []
        chunks = [task_list[i:i + self.max_array_size] for i in range(0, total_tasks, self.max_array_size)]
        
        out_dir = Path("./HPC_Payloads")
        out_dir.mkdir(exist_ok=True)
        (out_dir / "logs").mkdir(exist_ok=True)
        
        for idx, chunk in enumerate(chunks):
            batch_uuid = str(uuid.uuid4())[:8]
            array_count = len(chunk)
            
            # Serialize the specific tasks for this array
            chunk_file = out_dir / f"payload_{batch_uuid}.json"
            with open(chunk_file, "w") as f:
                json.dump({"tasks": chunk}, f)
                
            # Generate SLURM script
            sbatch_content = self.generate_sbatch_template(batch_uuid, array_count, module_name)
            sbatch_file = out_dir / f"submit_{batch_uuid}.sbatch"
            with open(sbatch_file, "w") as f:
                f.write(sbatch_content)
                
            # Register locally
            self.registry["batches"][batch_uuid] = {
                "module": module_name,
                "status": "STAGED",
                "task_count": array_count,
                "sbatch_file": str(sbatch_file),
                "remote_job_id": None # Populated after dispatch
            }
            generated_scripts.append(sbatch_file)
            print(f"   ↳ Generated Array [{idx+1}/{len(chunks)}] -> UUID: {batch_uuid} ({array_count} tasks)")
            
        self.save_registry()
        return generated_scripts

def main():
    print(f"\n{Colors.OKCYAN}--- CoChem-NODE: Array Batcher ---{Colors.ENDC}")
    
    batcher = HPCBatcher()
    
    # Mocking 2,500 target isomers from the TOPOS pipeline
    mock_tasks = [f"isomer_{i}.xyz" for i in range(2500)]
    
    scripts = batcher.create_batch(mock_tasks, "GOAT_Opt")
    
    print(f"{Colors.OKGREEN}✅ Safely divided into {len(scripts)} SLURM arrays to prevent scheduler timeout.{Colors.ENDC}")
    print(f"📁 Payloads saved to ./HPC_Payloads. Ready for Bridge dispatch.\n")

if __name__ == "__main__":
    main()