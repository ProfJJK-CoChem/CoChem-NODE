"""
CoChem-NODE: Stage 1.0 - Connection & Validation Bridge
Implements a secure, StrictHostKey-enforced SSH tunnel to the HPC cluster.
Integrates directly with the Golden Gatekeeper (RegistryManager) for configuration.
"""

import sys
import logging
from typing import Tuple, Optional
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

try:
    from cochem_registry_manager import get_current_config
    from cochem_registry_schema import CoChemConfig
except ImportError:
    try:
        from Libraries.cochem_registry_manager import get_current_config
        from Libraries.cochem_registry_schema import CoChemConfig
    except ImportError:
        from .cochem_registry_manager import get_current_config
        from .cochem_registry_schema import CoChemConfig

logger = logging.getLogger("CoChem_NODE_Bridge")
logger.setLevel(logging.INFO)

class CoChemHPCError(Exception):
    """Custom exception for HPC-specific connectivity or pre-flight failures."""

class NodeBridge:
    """
    Establishes and validates the secure connection to the HPC cluster.
    """
    def __init__(self, config: Optional[CoChemConfig] = None) -> None:
        self.config = config or get_current_config()
        self.hpc_config = self.config.hpc
        
        if self.hpc_config.execution_mode == "local":
            logger.info("NodeBridge: execution_mode='local'. Operating in local queue mode.")
            self.client = None
        elif PARAMIKO_AVAILABLE:
            self.client = paramiko.SSHClient()
            # Security Suggestion 2: Strict Host Key Checking enforced.
            self.client.load_system_host_keys()
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
            self.client = None

    def establish_heartbeat(self) -> bool:
        """
        Attempts a non-interactive login utilizing ssh-agent or defined key paths.
        """
        if self.client is None:
            logger.info("Local mode active: SSH heartbeat bypassed.")
            return True

        key_path = self.hpc_config.ssh_key_path if self.hpc_config.ssh_key_path else None
        
        logger.info(f"Attempting secure connection to {self.hpc_config.username}@{self.hpc_config.cluster_hostname}...")
        
        try:
            # Security Suggestion 1: allow_agent=True defers to OS keyring
            self.client.connect(
                hostname=self.hpc_config.cluster_hostname,
                username=self.hpc_config.username,
                key_filename=key_path,
                allow_agent=True,
                look_for_keys=True,
                timeout=10.0
            )
            logger.info("✅ Heartbeat Established: Secure SSH tunnel active.")
            return True
            
        except paramiko.ssh_exception.BadHostKeyException:
            raise CoChemHPCError("SECURITY ALERT: Host key mismatch. Potential MITM attack or changed cluster key.")
        except paramiko.ssh_exception.AuthenticationException:
            raise CoChemHPCError("Authentication failed. Verify your SSH keys and ssh-agent status.")
        except Exception as e:
            raise CoChemHPCError(f"Failed to connect to cluster: {e}")

    def execute_preflight_checks(self) -> Tuple[bool, str]:
        """
        Validates the remote environment before permitting job submissions.
        Checks for Slurm (sbatch) and the defined computational engine.
        """
        if self.client is None:
            return True, "Local mode active; SSH bypassed."

        if self.client.get_transport() is None or not self.client.get_transport().is_active():
            self.establish_heartbeat()

        logger.info("Executing remote pre-flight environment checks...")
        
        # 1. Check for Slurm Scheduler
        stdin, stdout, stderr = self.client.exec_command("command -v sbatch")
        if not stdout.read().decode('utf-8').strip():
            return False, "CRITICAL: 'sbatch' command not found. Target is not a Slurm head node."

        # 2. Check for computational engine module/binary availability
        # We query the registry to see what the user intends to run.
        target_engine = "orca" if "orca" in self.config.engines.orca_path.lower() else "unknown"
        
        if target_engine == "orca":
            # Using 'module avail' or checking raw path depending on config
            check_cmd = f"module avail orca || command -v orca || echo 'MISSING'"
            stdin, stdout, stderr = self.client.exec_command(check_cmd)
            result = stdout.read().decode('utf-8').strip()
            
            if "MISSING" in result and "orca" not in result.lower():
                 logger.warning(f"⚠️ Pre-flight warning: ORCA module not immediately visible in default PATH.")
                 # We don't fail strictly here because Slurm scripts often load specific module paths
            else:
                 logger.info("✅ Pre-flight: ORCA engine detected on cluster.")

        return True, "Pre-flight checks passed."

    def disconnect(self) -> None:
        """Safely tears down the SSH socket."""
        if self.client:
            self.client.close()
            logger.info("SSH tunnel closed.")

if __name__ == "__main__":
    # Diagnostic / Test Run
    try:
        bridge = NodeBridge()
        success = bridge.establish_heartbeat()
        if success:
            status, msg = bridge.execute_preflight_checks()
            logger.info(msg)
            bridge.disconnect()
    except Exception as e:
        logger.error(f"HPC Bridge Error: {e}")