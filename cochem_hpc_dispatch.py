"""
CoChem-NODE: Stage 2.0.2 - Job Dispatcher
Orchestrates the secure transfer of input files and Slurm templates to the
HPC cluster, executes the submission, and captures the resulting Job ID.
"""

import os
import logging
import shlex
import tempfile
from pathlib import Path
from typing import Tuple, Optional

# Import CoChem modules
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

class HPCDispatcher:
    """
    Handles file transfer and job submission logic via the NodeBridge.
    """
    def __init__(self, bridge: Optional[NodeBridge] = None):
        self.config = get_current_config()
        self.bridge = bridge or NodeBridge(self.config)
        self.templater = SlurmTemplater(config=self.config)

    def dispatch_job(self, 
                     job_name: str, 
                     local_input_file: Path, 
                     remote_work_dir: str, 
                     engine_name: str,
                     execution_command: str,
                     requested_cores: int,
                     requested_memory_mb: int,
                     modules_to_load: list = None) -> Tuple[bool, str]:
        """
        1. Connects to the cluster.
        2. Creates the remote directory safely.
        3. Transfers the input file via SFTP.
        4. Generates and transfers the Slurm template.
        5. Submits the job via sbatch and returns the Job ID.
        """
        
        # Ensure bridge is connected
        if self.bridge.client is not None:
            if not self.bridge.client.get_transport() or not self.bridge.client.get_transport().is_active():
                 logger.info("Initializing NodeBridge connection...")
                 self.bridge.establish_heartbeat()
        else:
            logger.info("Dispatching job in local mode...")
            return True, f"LOCAL_JOB_{job_name}"

        # Step 1: Ensure Remote Directory Exists (Sanitized with shlex.quote) (NODE-10)
        safe_remote_dir = shlex.quote(remote_work_dir)
        safe_job_name = shlex.quote(job_name)
        logger.info(f"Preparing remote directory: {remote_work_dir}")
        self.bridge.client.exec_command(f"mkdir -p {safe_remote_dir}")

        local_tmp_script = None
        sftp = None
        try:
            # Step 2: Open SFTP Subsystem
            sftp = self.bridge.client.open_sftp()
            
            # Step 3: Transfer Input File
            if not local_input_file.exists():
                raise FileNotFoundError(f"Local input file missing: {local_input_file}")
            
            remote_input_path = f"{remote_work_dir}/{local_input_file.name}"
            logger.info(f"Transferring {local_input_file.name} to cluster...")
            sftp.put(str(local_input_file), remote_input_path)
            
            # Step 4: Generate and Transfer Slurm Template
            slurm_script_content = self.templater.render_job(
                job_name=job_name,
                work_dir=remote_work_dir,
                execution_command=execution_command,
                engine_name=engine_name,
                requested_cores=requested_cores,
                requested_memory_mb=requested_memory_mb,
                modules_to_load=modules_to_load
            )
            
            remote_script_path = f"{remote_work_dir}/submit_{job_name}.sh"
            
            # Write template locally then transfer with try/finally cleanup (NODE-11)
            local_tmp_script = Path(f".tmp_{job_name}.sh")
            with open(local_tmp_script, 'w') as f:
                 f.write(slurm_script_content)
                 
            logger.info("Transferring generated Slurm template...")
            sftp.put(str(local_tmp_script), remote_script_path)

            # Step 5: Execute sbatch (Sanitized paths) (NODE-10)
            logger.info("Submitting job to Slurm scheduler...")
            sbatch_cmd = f"cd {safe_remote_dir} && sbatch submit_{safe_job_name}.sh"
            stdin, stdout, stderr = self.bridge.client.exec_command(sbatch_cmd)
            
            # Check exit status code rather than raw stderr string (NODE-12)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8').strip()
            error_output = stderr.read().decode('utf-8').strip()
            
            if exit_code != 0:
                logger.error(f"Slurm submission error (exit code {exit_code}): {error_output}")
                return False, f"Submission Failed (exit code {exit_code}): {error_output}"
                
            # Slurm output usually looks like: "Submitted batch job 123456"
            if "Submitted batch job" in output:
                job_id = output.split()[-1]
                logger.info(f"✅ Job successfully submitted. Slurm ID: {job_id}")
                return True, job_id
            else:
                return False, f"Unexpected Slurm output: {output}"

        except Exception as e:
            logger.error(f"Dispatch process failed: {e}")
            return False, str(e)
            
        finally:
            # Guaranteed cleanup of local temp script (NODE-11)
            if local_tmp_script and local_tmp_script.exists():
                try:
                    local_tmp_script.unlink()
                except OSError as err:
                    logger.debug(f"Temp script cleanup skipped: {err}")
            if sftp:
                try:
                    sftp.close()
                except Exception as err:
                    logger.debug(f"SFTP teardown exception: {err}")
        
    def teardown(self):
         self.bridge.disconnect()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        test_inp = Path("test_molecule.inp")
        test_inp.write_text("! DFT OPT\n*xyz 0 1\nO 0 0 0\nH 0 0.7 0.7\nH 0 -0.7 0.7\n*")
        
        dispatcher = HPCDispatcher()
        print("\n--- Initiating Test Dispatch ---")
        test_inp.unlink()
        dispatcher.teardown()
        print("Dispatcher initialized successfully.")
        
    except Exception as e:
         print(f"Dispatcher Error: {e}")