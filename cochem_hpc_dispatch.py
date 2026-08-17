"""
CoChem-NODE: Job Dispatcher & Scout-and-Anchor Co-Scheduler (NODE-04)
Orchestrates heterogeneous CPU anchor (MPQC CCSD(T)-F12 7 P-cores) + GPU scout (MLFF 1 P-core + MPS)
execution while bounding CPU contention (<= 1.20x).
"""

import os
import time
import logging
import shlex
import tempfile
from pathlib import Path
from typing import Tuple, Optional, Dict, Any

import sys
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

try:
    from cochem_node_connection_bridge import NodeBridge, CoChemHPCError
    from cochem_slurm_templater import SlurmTemplater
    from cochem_registry_manager import get_current_config
except ImportError:
    from Libraries.cochem_node_connection_bridge import NodeBridge, CoChemHPCError
    from Libraries.cochem_slurm_templater import SlurmTemplater
    from Libraries.cochem_registry_manager import get_current_config

logger = logging.getLogger("CoChem_NODE_Dispatcher")
logger.setLevel(logging.INFO)

class ScoutAnchorCoScheduler:
    """
    Heterogeneous Scout-and-Anchor Co-Scheduler (§8A.2).
    Manages concurrent CPU anchor (7 P-cores) and GPU scout (1 P-core + MPS) task pairs.
    """
    def __init__(self, templater: SlurmTemplater) -> None:
        self.templater = templater
        self.max_cpu_contention_ratio = 1.20  # Mandated bound (§8A.2)

    def prepare_co_scheduled_payloads(self, 
                                       anchor_spec: Dict[str, Any], 
                                       scout_spec: Dict[str, Any]) -> Tuple[str, str]:
        """
        Renders paired Slurm templates for CPU anchor and GPU scout.
        - Anchor: 7 P-cores (KMP_HW_SUBSET=8c:intel_core,1t with 7 cores), MPQC CPU.
        - Scout: 1 P-core (KMP_HW_SUBSET=8c:intel_core,1t with 1 core), GPU MPS (25% thread pct).
        """
        anchor_script = self.templater.render_job(
            job_name=f"ANCHOR_{anchor_spec.get('job_name', 'mpqc_task')}",
            work_dir=anchor_spec.get('work_dir', '.'),
            execution_command=anchor_spec.get('execution_command', ''),
            engine_name=anchor_spec.get('engine_name', 'MPQC'),
            tier=anchor_spec.get('tier', 'T2-3h'),
            requested_cores=7,
            use_gpu=False,
            mps_enabled=False,
            modules_to_load=anchor_spec.get('modules', ['mpqc', 'libint', 'madness'])
        )

        scout_script = self.templater.render_job(
            job_name=f"SCOUT_{scout_spec.get('job_name', 'mlff_task')}",
            work_dir=scout_spec.get('work_dir', '.'),
            execution_command=scout_spec.get('execution_command', ''),
            engine_name=scout_spec.get('engine_name', 'MACE-OFF24m'),
            tier=scout_spec.get('tier', 'T1-30min'),
            requested_cores=1,
            use_gpu=True,
            mps_enabled=True
        )

        return anchor_script, scout_script

    def verify_contention_bound(self, anchor_runtime: float, standalone_benchmark: float) -> bool:
        """Asserts that CPU contention slowdown ratio is within Section 8A.2 limits (<= 1.20x)."""
        if standalone_benchmark <= 0:
            return True
        ratio = anchor_runtime / standalone_benchmark
        if ratio > self.max_cpu_contention_ratio:
            logger.warning(f"⚠️ CPU contention ratio ({ratio:.2f}x) exceeded bound ({self.max_cpu_contention_ratio}x)!")
            return False
        return True

class HPCDispatcher:
    """
    Handles single job dispatch and Scout-and-Anchor co-scheduled submission via NodeBridge.
    """
    def __init__(self, bridge: Optional[NodeBridge] = None) -> None:
        self.config = get_current_config()
        self.bridge = bridge or NodeBridge(self.config)
        self.templater = SlurmTemplater(config=self.config)
        self.co_scheduler = ScoutAnchorCoScheduler(self.templater)

    def dispatch_job(self, 
                     job_name: str, 
                     local_input_file: Path, 
                     remote_work_dir: str, 
                     engine_name: str,
                     execution_command: str,
                     requested_cores: int,
                     requested_memory_mb: int,
                     tier: Optional[str] = None,
                     use_gpu: bool = False,
                     mps_enabled: bool = False,
                     modules_to_load: list = None) -> Tuple[bool, str]:
        """Dispatch a single HPC job (remote SSH or local fallback mode)."""
        if self.bridge.client is not None:
            if not self.bridge.client.get_transport() or not self.bridge.client.get_transport().is_active():
                 logger.info("Initializing NodeBridge connection...")
                 self.bridge.establish_heartbeat()
        else:
            logger.info(f"Local dispatch executing for '{job_name}'")
            import subprocess
            try:
                os.makedirs(remote_work_dir, exist_ok=True)
                proc = subprocess.Popen(execution_command, shell=True, cwd=remote_work_dir)
                return True, str(proc.pid)
            except Exception as e:
                logger.error(f"Local dispatch failed for '{job_name}': {e}")
                return False, str(e)

        safe_remote_dir = shlex.quote(remote_work_dir)
        safe_job_name = shlex.quote(job_name)
        self.bridge.client.exec_command(f"mkdir -p {safe_remote_dir}")

        local_tmp_script = None
        sftp = None
        try:
            sftp = self.bridge.client.open_sftp()
            if not local_input_file.exists():
                raise FileNotFoundError(f"Local input file missing: {local_input_file}")
            
            remote_input_path = f"{remote_work_dir}/{local_input_file.name}"
            sftp.put(str(local_input_file), remote_input_path)
            
            slurm_script_content = self.templater.render_job(
                job_name=job_name,
                work_dir=remote_work_dir,
                execution_command=execution_command,
                engine_name=engine_name,
                tier=tier,
                requested_cores=requested_cores,
                requested_memory_mb=requested_memory_mb,
                use_gpu=use_gpu,
                mps_enabled=mps_enabled,
                modules_to_load=modules_to_load
            )
            
            remote_script_path = f"{remote_work_dir}/submit_{job_name}.sh"
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".sh") as tmp:
                tmp.write(slurm_script_content)
                local_tmp_script = Path(tmp.name)
                 
            sftp.put(str(local_tmp_script), remote_script_path)

            sbatch_cmd = f"cd {safe_remote_dir} && sbatch submit_{safe_job_name}.sh"
            stdin, stdout, stderr = self.bridge.client.exec_command(sbatch_cmd)
            
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8').strip()
            error_output = stderr.read().decode('utf-8').strip()
            
            if exit_code != 0:
                logger.error(f"Slurm submission error (exit code {exit_code}): {error_output}")
                return False, f"Submission Failed (exit code {exit_code}): {error_output}"
                
            if "Submitted batch job" in output:
                job_id = output.split()[-1]
                logger.info(f"✅ Job submitted successfully. Slurm ID: {job_id}")
                return True, job_id
            else:
                return False, f"Unexpected Slurm output: {output}"

        except Exception as e:
            logger.error(f"Dispatch process failed: {e}")
            return False, str(e)
            
        finally:
            if local_tmp_script and local_tmp_script.exists():
                try:
                    local_tmp_script.unlink()
                except OSError as e:
                    logger.warning(f"Failed to delete temp file {local_tmp_script}: {e}")
            if sftp:
                try:
                    sftp.close()
                except Exception as e:
                    logger.warning(f"Failed to close SFTP connection: {e}")
    def dispatch_co_scheduled_pair(self, 
                                   anchor_spec: Dict[str, Any], 
                                   scout_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Dispatches a CPU Anchor + GPU Scout job pair co-scheduled on hardware (§8A.2).
        Actually dispatches to HPC via bridge rather than mocking.
        """
        anchor_script, scout_script = self.co_scheduler.prepare_co_scheduled_payloads(anchor_spec, scout_spec)
        logger.info("Prepared Scout-and-Anchor co-scheduled Slurm payloads.")
        
        anchor_job_id = None
        scout_job_id = None
        
        if self.bridge.client is not None:
            # Anchor dispatch
            anchor_dir = anchor_spec.get('work_dir', '.')
            anchor_name = anchor_spec.get('job_name', 'anchor')
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".sh") as tmp:
                tmp.write(anchor_script)
                anchor_tmp = tmp.name
            
            try:
                sftp = self.bridge.client.open_sftp()
                self.bridge.client.exec_command(f"mkdir -p {shlex.quote(anchor_dir)}")
                remote_anchor_path = f"{anchor_dir}/submit_{anchor_name}.sh"
                sftp.put(anchor_tmp, remote_anchor_path)
                sbatch_cmd = f"cd {shlex.quote(anchor_dir)} && sbatch submit_{shlex.quote(anchor_name)}.sh"
                stdin, stdout, stderr = self.bridge.client.exec_command(sbatch_cmd)
                if stdout.channel.recv_exit_status() == 0:
                    out = stdout.read().decode('utf-8').strip()
                    if "Submitted batch job" in out:
                        anchor_job_id = out.split()[-1]
            except Exception as e:
                logger.error(f"Anchor dispatch failed: {e}")
            finally:
                Path(anchor_tmp).unlink(missing_ok=True)
                
            # Scout dispatch
            scout_dir = scout_spec.get('work_dir', '.')
            scout_name = scout_spec.get('job_name', 'scout')
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".sh") as tmp:
                tmp.write(scout_script)
                scout_tmp = tmp.name
                
            try:
                self.bridge.client.exec_command(f"mkdir -p {shlex.quote(scout_dir)}")
                remote_scout_path = f"{scout_dir}/submit_{scout_name}.sh"
                sftp.put(scout_tmp, remote_scout_path)
                sbatch_cmd = f"cd {shlex.quote(scout_dir)} && sbatch submit_{shlex.quote(scout_name)}.sh"
                stdin, stdout, stderr = self.bridge.client.exec_command(sbatch_cmd)
                if stdout.channel.recv_exit_status() == 0:
                    out = stdout.read().decode('utf-8').strip()
                    if "Submitted batch job" in out:
                        scout_job_id = out.split()[-1]
            except Exception as e:
                logger.error(f"Scout dispatch failed: {e}")
            finally:
                Path(scout_tmp).unlink(missing_ok=True)
                if 'sftp' in locals() and sftp:
                    sftp.close()
        else:
            logger.info("Local fallback dispatch executing for co-scheduled pair")
            import subprocess
            try:
                anc_dir = anchor_spec.get('work_dir', '.')
                sct_dir = scout_spec.get('work_dir', '.')
                os.makedirs(anc_dir, exist_ok=True)
                os.makedirs(sct_dir, exist_ok=True)
                
                anc_proc = subprocess.Popen(anchor_spec['execution_command'], shell=True, cwd=anc_dir)
                anchor_job_id = str(anc_proc.pid)
                
                sct_proc = subprocess.Popen(scout_spec['execution_command'], shell=True, cwd=sct_dir)
                scout_job_id = str(sct_proc.pid)
            except Exception as e:
                logger.error(f"Local co-scheduled dispatch failed: {e}")
                return {"status": "FAILED", "anchor_job_id": None, "scout_job_id": None, "anchor_job_name": anchor_spec.get('job_name'), "scout_job_name": scout_spec.get('job_name')}
        
        return {
            "status": "DISPATCHED" if (anchor_job_id and scout_job_id) else "FAILED",
            "anchor_job_id": anchor_job_id,
            "scout_job_id": scout_job_id,
            "anchor_job_name": anchor_spec.get('job_name'),
            "scout_job_name": scout_spec.get('job_name')
        }

    def teardown(self) -> None:
        self.bridge.disconnect()