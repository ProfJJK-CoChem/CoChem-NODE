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
from typing import Optional

import sys
from pathlib import Path
from typing import Optional, Any

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from cochem_slurm_templater import SlurmTemplater
    from cochem_registry_manager import get_current_config, RegistryLock
except ImportError:
    try:
        from Libraries.cochem_slurm_templater import SlurmTemplater
        from Libraries.cochem_registry_manager import get_current_config, RegistryLock
    except ImportError:
        from .cochem_slurm_templater import SlurmTemplater
        from .cochem_registry_manager import get_current_config, RegistryLock

class Colors:
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

logger = logging.getLogger("CoChem_NODE_Batcher")
logging.basicConfig(filename='cochem_node_batcher.log', level=logging.INFO)

class HPCBatcher:
    def __init__(self, config: Any = None) -> None:
        self.config = config or get_current_config()
        self.templater = SlurmTemplater(config=self.config)
        self.registry_path = Path("cochem_hpc_registry.json")
        self.registry = self.load_registry()
        self.max_array_size = 1000 # Standard SLURM limit constraint

    def load_registry(self) -> dict:
        if self.registry_path.exists():
            with RegistryLock(self.registry_path, timeout=5.0):
                with open(self.registry_path, "r", encoding="utf-8") as f:
                    return json.loads(f.read())
        return {"batches": {}}

    def save_registry(self) -> None:
        with RegistryLock(self.registry_path, timeout=5.0):
            with open(self.registry_path, "w", encoding="utf-8") as f:
                json.dump(self.registry, f, indent=4)

    def generate_sbatch_template(self, batch_uuid: str, array_count: int, module_name: str) -> str:
        """
        Constructs the SLURM submission script delegating to SlurmTemplater (NODE-16, NODE-17).
        Dynamically queries OpenMPI version from config/registry.
        """
        openmpi_module = getattr(self.config.engines, 'openmpi_module', 'openmpi/4.1.8') if hasattr(self.config, 'engines') else "openmpi/4.1.8"
        modules = [openmpi_module] if openmpi_module else []
        modules.extend(['mpqc', 'libint', 'madness'])

        execution_cmd = f"python {module_name}_payload.py --task_id $SLURM_ARRAY_TASK_ID"

        # Delegate template rendering to SlurmTemplater (NODE-17)
        sbatch_script = self.templater.render_job(
            job_name=f"CoChem_{module_name}",
            work_dir=os.getcwd(),
            execution_command=execution_cmd,
            engine_name=module_name,
            requested_cores=8,
            requested_memory_mb=32000,
            modules_to_load=modules
        )

        # Inject job array directive into rendered script
        lines = sbatch_script.split('\n')
        array_line = f"#SBATCH --array=1-{array_count}"
        lines.insert(3, array_line)
        return '\n'.join(lines)

    def create_batch(self, task_list: list, module_name: str) -> list:
        """Splits tasks to respect SLURM array limits and registers them."""
        total_tasks = len(task_list)
        logger.info(f"Batching {total_tasks} tasks for HPC module: {module_name}")
        
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
                
            # Generate SLURM script using SlurmTemplater
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
            logger.info(f"   Generated Array [{idx+1}/{len(chunks)}] -> UUID: {batch_uuid} ({array_count} tasks)")
            
        self.save_registry()
        return generated_scripts

def load_candidate_geometries(search_dir: Optional[Path] = None) -> list:
    """
    Reads candidate geometry files (.xyz) from the active workspace or
    job queue directory. Returns a list of absolute path strings suitable
    for batching.
    
    MOCK-16 fix: replaces hardcoded 2500 fake 'isomer_i.xyz' task generation.

    Search order:
        1. Explicit ``search_dir`` argument.
        2. ``./cochem_node_data/jobs/`` (standard NODE job queue).
        3. ``./HPC_Payloads/`` (legacy payload staging area).
    """
    candidate_dirs = [
        search_dir,
        Path("cochem_node_data") / "jobs",
        Path("HPC_Payloads"),
    ]
    
    for candidate in candidate_dirs:
        if candidate is None:
            continue
        candidate = Path(candidate).resolve()
        if candidate.is_dir():
            xyz_files = sorted(candidate.glob("**/*.xyz"))
            if xyz_files:
                logging.info(f"Loaded {len(xyz_files)} candidate geometries from {candidate}")
                return [str(f) for f in xyz_files]
    
    logging.warning("No candidate .xyz geometry files found in any search directory.")
    return []


def main() -> None:
    logger.info("--- CoChem-NODE: Array Batcher ---")
    
    import argparse
    parser = argparse.ArgumentParser(description="CoChem-NODE Job Array Batcher")
    parser.add_argument(
        "--geometry-dir", type=str, default=None,
        help="Path to directory containing .xyz candidate geometries"
    )
    parser.add_argument(
        "--module", type=str, default="GOAT_Opt",
        help="HPC module name for SLURM job (default: GOAT_Opt)"
    )
    args = parser.parse_args()

    search_path = Path(args.geometry_dir) if args.geometry_dir else None
    tasks = load_candidate_geometries(search_dir=search_path)

    if not tasks:
        logger.error("No .xyz geometry files found. Provide a --geometry-dir or populate ./cochem_node_data/jobs/.")
        sys.exit(1)
    
    batcher = HPCBatcher()
    scripts = batcher.create_batch(tasks, args.module)
    
    logger.info(f"Safely divided {len(tasks)} tasks into {len(scripts)} SLURM arrays to prevent scheduler timeout.")
    logger.info("Payloads saved to ./HPC_Payloads. Ready for Bridge dispatch.")

if __name__ == "__main__":
    main()