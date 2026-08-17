"""
CoChem-NODE: Slurm Template Generation (v4 Method Matrix Upgrade)
Dynamically constructs HPC submission scripts using Jinja2 with CPU core pinning,
CUDA MPS daemon multiplexing controls, and 10-tier walltime budgets.
"""

import hashlib
import logging
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, TemplateError
from typing import Dict, Any, Optional

import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

try:
    from Libraries.cochem_registry_manager import get_current_config
    from Libraries.cochem_registry_schema import CoChemConfig
except ImportError:
    from cochem_registry_manager import get_current_config
    from cochem_registry_schema import CoChemConfig

logger = logging.getLogger("CoChem_NODE_Templater")
logger.setLevel(logging.INFO)

# ==========================================
# Jinja2 Template Base String (v4 Standard)
# ==========================================
DEFAULT_SLURM_TEMPLATE = """#!/bin/bash
#SBATCH --job-name={{ job_name }}
#SBATCH --partition={{ partition }}
#SBATCH --nodes={{ nodes | default(1) }}
#SBATCH --ntasks={{ total_tasks }}
{% if ntasks_per_node %}#SBATCH --ntasks-per-node={{ ntasks_per_node }}
{% endif %}#SBATCH --cpus-per-task={{ cpus_per_task }}
#SBATCH --mem={{ memory_mb }}M
#SBATCH --time={{ walltime }}
#SBATCH --output={{ output_log }}
#SBATCH --error={{ error_log }}
{% if account %}#SBATCH --account={{ account }}
{% endif %}{% if qos %}#SBATCH --qos={{ qos }}
{% endif %}{% if gpu_spec %}#SBATCH --gres={{ gpu_spec }}
{% endif %}

# --- CoChem-NODE Core Pinning & Topology ( 8A.0) ---
export KMP_HW_SUBSET={{ kmp_hw_subset | default('8c:intel_core,1t') }}
export OMP_NUM_THREADS={{ cpus_per_task }}
export TA_LIMIT_MEMORY={{ memory_mb }}M
export MAD_NUM_THREADS={{ cpus_per_task }}
export TMPDIR=/scratch/${USER}/${SLURM_JOB_ID}
mkdir -p $TMPDIR

{% if use_gpu and mps_enabled %}
# --- CUDA MPS Daemon Start ( 8A.4) ---
export CUDA_MPS_PIPE_DIRECTORY={{ mps_pipe_dir | default('/tmp/nvidia-mps') }}
export CUDA_MPS_LOG_DIRECTORY={{ mps_log_dir | default('/tmp/nvidia-log') }}
export CUDA_MPS_ACTIVE_THREAD_PERCENTAGE={{ mps_thread_pct | default(25) }}
nvidia-cuda-mps-control -d
{% endif %}

# Purge and load required modules
module purge
{% if modules_to_load %}
{% for module in modules_to_load %}
module load {{ module }}
{% endfor %}
{% endif %}

cd {{ work_dir }}

echo "Executing Engine: {{ engine_name }}"
echo "Tier: {{ tier | default('T1-30min') }}"
echo "Command: {{ execution_command }}"

# --- EXECUTION BLOCK ---
time {{ execution_command }}

exit_code=$?
echo "Job finished at $(date) with exit code $exit_code"

{% if use_gpu and mps_enabled %}
# --- CUDA MPS Daemon Shutdown ---
echo "quit" | nvidia-cuda-mps-control
{% endif %}

rm -rf $TMPDIR
exit $exit_code
"""

class SlurmTemplater:
    """
    Renders Slurm batch scripts utilizing Jinja2, CPU core pinning, and CUDA MPS controls.
    """
    def __init__(self, template_dir: Optional[str] = None, config: Optional[Any] = None) -> None:
        self.config = config or get_current_config()
        self.env = None
        
        if template_dir and Path(template_dir).is_dir():
            self.env = Environment(loader=FileSystemLoader(template_dir))
            logger.info(f"Initialized Jinja2 environment from {template_dir}")
        else:
            logger.warning("No template directory provided or found. Using embedded fallback template.")

    def render_job(self, 
                   job_name: str, 
                   work_dir: str, 
                   execution_command: str, 
                   engine_name: str = "MPQC",
                   tier: Optional[str] = None,
                   requested_cores: Optional[int] = None,
                   requested_memory_mb: Optional[int] = None,
                   walltime: Optional[str] = None,
                   use_gpu: bool = False,
                   mps_enabled: bool = False,
                   cpus_per_task: int = 1,
                   modules_to_load: list = None) -> str:
        """
        Renders the final sbatch string payload.
        Resolves walltime dynamically from v4 tier budgets and manages CUDA MPS flags.
        """
        # Resource Throttle Safety Checks
        max_cores = getattr(self.config.hardware, 'cpu_cores', 8) if hasattr(self.config, 'hardware') else 8
        max_mem = getattr(self.config.hardware, 'ram_mb', 32000) if hasattr(self.config, 'hardware') else 32000
        
        req_cores = requested_cores or max_cores
        
        # --- Method Matrix CCSD(T) Physics Wall Enforcement ---
        if "CCSD(T)" in execution_command or "CCSD(T)" in engine_name.upper():
            if req_cores < 4:
                logger.error(f"[HARD_ABORT: PHYSICS WALL] CCSD(T) requires >= 4 CPU cores. Requested: {req_cores}")
                raise ValueError(f"[HARD_ABORT: PHYSICS WALL] CCSD(T) requires >= 4 CPU cores.")
        
        if requested_memory_mb and requested_memory_mb > max_mem:
            logger.error(f"[HARD_ABORT: PHYSICS WALL] Requested RAM {requested_memory_mb} MB exceeds physical limit {max_mem} MB.")
            raise ValueError(f"[HARD_ABORT: PHYSICS WALL] Requested RAM {requested_memory_mb} MB exceeds physical limit {max_mem} MB.")
        
        # --- Method Matrix Compliance Check ---
        if "Opt" in execution_command or "opt" in execution_command.lower():
            if "Calc_Hess true" in execution_command or "calc_hess true" in execution_command.lower():
                 logger.error(f"[HARD_ABORT: PHYSICS WALL] Calc_Hess true is strictly prohibited for geometry optimizations. Use InHess XTB2 or Lindh.")
                 raise ValueError("[HARD_ABORT: PHYSICS WALL] Calc_Hess true is strictly prohibited for geometry optimizations.")
            if "TolMaxG 1e-5" not in execution_command and "intermolecular" in job_name.lower():
                 logger.warning("Intermolecular convergence requires TolMaxG 1e-5 per Method Matrix.")
            if "defgrid3" not in execution_command.lower() and "defgrid1" not in execution_command.lower():
                 pass # Warning or error logic can be expanded
            if "Grid3" in execution_command or "Grid5" in execution_command:
                 logger.error("[HARD_ABORT: PHYSICS WALL] Grid3/Grid5 terminology prohibited. Use defgrid1/defgrid3.")
                 raise ValueError("[HARD_ABORT: PHYSICS WALL] Grid3/Grid5 terminology prohibited.")
            if "D3" not in execution_command and "D4" not in execution_command and "B3LYP" in execution_command:
                 logger.error("[HARD_ABORT: PHYSICS WALL] DFT optimizations of weak complexes require D3/D4 dispersion.")
                 raise ValueError("[HARD_ABORT: PHYSICS WALL] DFT optimizations of weak complexes require D3/D4 dispersion.")

        # Multi-Node Routing Logic
        total_tasks = max(1, req_cores // cpus_per_task)
        
        if req_cores > max_cores:
            import math
            nodes = math.ceil(req_cores / max_cores)
            ntasks_per_node = max_cores
            logger.info(f"Requested {req_cores} cores exceeds physical max ({max_cores}). Routing as Multi-Node job with {nodes} nodes.")
        else:
            nodes = 1
            ntasks_per_node = total_tasks
            
        if total_tasks > 1:
            # Prefix the execution command with mpirun using the correct total tasks (ranks)
            execution_command = f"mpirun -np {total_tasks} {execution_command}"

        memory_mb = min(requested_memory_mb or max_mem, max_mem)
        

        # Resolve Walltime from Tier Budget if specified
        resolved_walltime = walltime or "00:30:00"
        if tier:
            budgets = getattr(self.config, 'walltime_budgets', {})
            if not isinstance(budgets, dict) or not budgets:
                hpc_cfg = getattr(self.config, 'hpc', None)
                if hpc_cfg and hasattr(hpc_cfg, 'walltime_budgets'):
                    budgets = hpc_cfg.walltime_budgets
            if isinstance(budgets, dict) and tier in budgets:
                resolved_walltime = budgets[tier]

        hpc_cfg = getattr(self.config, 'hpc', None)
        partition = getattr(hpc_cfg, 'partition', 'compute') if hpc_cfg else 'compute'
        account = getattr(hpc_cfg, 'account', '') if hpc_cfg else ''
        qos = getattr(hpc_cfg, 'qos', '') if hpc_cfg else ''
        gpu_spec = getattr(hpc_cfg, 'gpu_gres', '') if hpc_cfg else ''

        payload_vars = {
            "job_name": job_name,
            "partition": partition,
            "nodes": nodes,
            "total_tasks": total_tasks,
            "ntasks_per_node": ntasks_per_node,
            "cpus_per_task": cpus_per_task,
            "memory_mb": memory_mb,
            "walltime": resolved_walltime,
            "tier": tier or "T1-30min",
            "output_log": f"{job_name}_%j.out",
            "error_log": f"{job_name}_%j.err",
            "account": account,
            "qos": qos,
            "gpu_spec": gpu_spec,
            "use_gpu": use_gpu,
            "mps_enabled": mps_enabled,
            "kmp_hw_subset": "8c:intel_core,1t",
            "mps_pipe_dir": "/tmp/nvidia-mps",
            "mps_log_dir": "/tmp/nvidia-log",
            "mps_thread_pct": 25,
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

    def compute_artifact_sha256(self, artifact_path: Path) -> str:
        """Computes cryptographic SHA-256 hash for .out / .gbw computational artifacts."""
        if not artifact_path.exists():
            return ""
        sha256_hash = hashlib.sha256()
        with open(artifact_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()