"""
CoChem-NODE: Stage 2.0.1 - Slurm Template Generation
Dynamically constructs HPC submission scripts using Jinja2, ensuring resource limits
are respected and engine paths are correctly mapped from the Golden Gatekeeper.
"""

import logging
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, TemplateError
from typing import Dict, Any, Optional

# Import the Golden Gatekeeper
import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from Libraries.cochem_registry_manager import get_current_config
from Libraries.cochem_registry_schema import CoChemConfig

logger = logging.getLogger("CoChem_NODE_Templater")
logger.setLevel(logging.INFO)

# ==========================================
# Jinja2 Template Base String (Fallback)
# ==========================================
# Typically this would be loaded from a file (.j2), but we embed it as a fallback
# to ensure the pipeline doesn't break if the template folder is missing.
DEFAULT_SLURM_TEMPLATE = """#!/bin/bash
#SBATCH --job-name={{ job_name }}
#SBATCH --partition={{ partition }}
#SBATCH --nodes=1
#SBATCH --ntasks-per-node={{ cores }}
#SBATCH --mem={{ memory_mb }}M
#SBATCH --time={{ walltime }}
#SBATCH --output={{ output_log }}
#SBATCH --error={{ error_log }}

# --- CoChem-NODE HPC Submission Bridge ---
# Auto-generated on submission. Do not edit manually.

echo "Job started on $(hostname) at $(date)"

# Purge and load required modules (if specified)
module purge
{% if modules_to_load %}
{% for module in modules_to_load %}
module load {{ module }}
{% endfor %}
{% endif %}

# Set execution environment variables
export OMP_NUM_THREADS={{ cores }}
export TMPDIR=/scratch/${USER}/${SLURM_JOB_ID}
mkdir -p $TMPDIR

# Change to execution directory
cd {{ work_dir }}

echo "Executing Engine: {{ engine_name }}"
echo "Command: {{ execution_command }}"

# --- EXECUTION BLOCK ---
time {{ execution_command }}

# --- CLEANUP BLOCK ---
# Trap errors and clean scratch to prevent quota issues (Improvement Suggestion 11)
exit_code=$?
echo "Job finished at $(date) with exit code $exit_code"
rm -rf $TMPDIR

exit $exit_code
"""

class SlurmTemplater:
    """
    Renders Slurm batch scripts utilizing Jinja2 and the central registry.
    """
    def __init__(self, template_dir: Optional[str] = None, config: Optional[CoChemConfig] = None):
        self.config = config or get_current_config()
        self.env = None
        
        # Initialize Jinja Environment
        if template_dir and Path(template_dir).is_dir():
            self.env = Environment(loader=FileSystemLoader(template_dir))
            logger.info(f"Initialized Jinja2 environment from {template_dir}")
        else:
            logger.warning("No template directory provided or found. Using embedded fallback template.")

    def render_job(self, 
                   job_name: str, 
                   work_dir: str, 
                   execution_command: str, 
                   engine_name: str = "ORCA",
                   requested_cores: Optional[int] = None,
                   requested_memory_mb: Optional[int] = None,
                   walltime: str = "24:00:00",
                   modules_to_load: list = None) -> str:
        """
        Renders the final sbatch string payload.
        Enforces registry limits to prevent scheduler rejection.
        """
        
        # Enforce Resource Limits (Safety Check)
        max_cores = self.config.hardware.cpu_cores
        max_mem = self.config.hardware.ram_mb
        
        cores = min(requested_cores or max_cores, max_cores)
        memory_mb = min(requested_memory_mb or max_mem, max_mem)
        
        if cores < (requested_cores or 0):
             logger.warning(f"Requested {requested_cores} cores exceeds registry max ({max_cores}). Throttling down.")
        if memory_mb < (requested_memory_mb or 0):
             logger.warning(f"Requested {requested_memory_mb} MB exceeds registry max ({max_mem} MB). Throttling down.")

        payload_vars = {
            "job_name": job_name,
            "partition": self.config.hpc.partition,
            "cores": cores,
            "memory_mb": memory_mb,
            "walltime": walltime,
            "output_log": f"{job_name}_%j.out",
            "error_log": f"{job_name}_%j.err",
            "modules_to_load": modules_to_load or [],
            "work_dir": work_dir,
            "engine_name": engine_name,
            "execution_command": execution_command
        }

        try:
            if self.env:
                template = self.env.get_template("slurm_base.sh.j2")
            else:
                template = Environment().from_string(DEFAULT_SLURM_TEMPLATE)
                
            return template.render(**payload_vars)
            
        except TemplateError as e:
            logger.error(f"Failed to render Slurm template: {e}")
            raise

if __name__ == "__main__":
    # Diagnostic / Test Run
    logging.basicConfig(level=logging.INFO)
    try:
        templater = SlurmTemplater()
        
        # Example ORCA Job
        script = templater.render_job(
            job_name="CoChem_Molecule_001",
            work_dir="/home/hpc_user/cochem_jobs/001",
            execution_command="/opt/orca/orca input.inp > output.txt",
            engine_name="ORCA 6.1.1",
            requested_cores=128, # Intentionally triggering the safety throttle
            requested_memory_mb=16000,
            modules_to_load=["openmpi/4.1.1", "orca/6.1.1"]
        )
        
        print("\n✅ Successfully Generated Slurm Script:\n")
        print("-" * 40)
        print(script)
        print("-" * 40)
        
    except Exception as e:
         print(f"Templater Error: {e}")