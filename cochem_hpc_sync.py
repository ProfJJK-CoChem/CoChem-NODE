"""
CoChem-NODE: Stage 3.2 - Artifact Retriever & Integrity Gate
Securely transfers computation artifacts from the HPC cluster to the local
workspace, strictly enforcing SHA256 checksum parity to prevent silent file corruption.
"""

import os
import hashlib
import logging
import time
from pathlib import Path
from typing import List, Tuple, Optional

# Import CoChem modules
import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from CoChem_NODE.cochem_node_bridge import NodeBridge, CoChemHPCError

logger = logging.getLogger("CoChem_NODE_Sync")
logger.setLevel(logging.INFO)

class ArtifactRetriever:
    """
    Handles secure SFTP extraction and cryptographic validation of HPC outputs.
    """
    def __init__(self, bridge: Optional[NodeBridge] = None):
        self.bridge = bridge or NodeBridge()

    def _ensure_connection(self):
        if not self.bridge.client.get_transport() or not self.bridge.client.get_transport().is_active():
            logger.info("ArtifactRetriever: Re-establishing SSH connection...")
            self.bridge.establish_heartbeat()

    def _get_remote_checksum(self, remote_path: str) -> str:
        """Executes sha256sum on the HPC node and parses the hash."""
        # Note: We use awk to extract just the hash string, ignoring the filepath
        command = f"sha256sum '{remote_path}' | awk '{{print $1}}'"
        stdin, stdout, stderr = self.bridge.client.exec_command(command)
        
        checksum = stdout.read().decode('utf-8').strip()
        err_output = stderr.read().decode('utf-8').strip()
        
        if not checksum or err_output:
            raise CoChemHPCError(f"Failed to compute remote checksum for {remote_path}. Error: {err_output}")
            
        return checksum

    def _get_local_checksum(self, local_path: Path) -> str:
        """Computes SHA256 hash of the downloaded file chunk-by-chunk."""
        sha256_hash = hashlib.sha256()
        with open(local_path, "rb") as f:
            # Read in 4K blocks to prevent Memory OOM on massive .gbw files
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def retrieve_artifacts(self, 
                           remote_dir: str, 
                           local_dir: Path, 
                           extensions: List[str] = ['.out', '.err', '.gbw', '.opt', '.engrad']) -> Tuple[bool, List[Path]]:
        """
        Scans the remote directory for specific file types, downloads them via SFTP,
        and validates their cryptographic integrity.
        """
        self._ensure_connection()
        
        if not local_dir.exists():
            local_dir.mkdir(parents=True, exist_ok=True)
            
        retrieved_files = []
        sftp = None
        
        try:
            sftp = self.bridge.client.open_sftp()
            
            # 1. Fetch directory listing
            logger.info(f"Scanning remote directory: {remote_dir}")
            try:
                remote_files = sftp.listdir(remote_dir)
            except IOError:
                logger.error(f"Remote directory {remote_dir} does not exist or is inaccessible.")
                return False, []
                
            # 2. Filter target artifacts
            targets = [f for f in remote_files if any(f.endswith(ext) for ext in extensions)]
            
            if not targets:
                logger.warning(f"No artifacts matching {extensions} found in {remote_dir}.")
                return False, []

            # 3. Download and Verify
            for filename in targets:
                remote_path = f"{remote_dir}/{filename}"
                local_path = local_dir / filename
                
                logger.info(f"Retrieving: {filename}...")
                
                # Retrieve the file via SFTP
                sftp.get(remote_path, str(local_path))
                
                # Integrity Verification (Suggestion 12)
                remote_hash = self._get_remote_checksum(remote_path)
                local_hash = self._get_local_checksum(local_path)
                
                if remote_hash != local_hash:
                    logger.error(f"CRITICAL: Integrity Check Failed for {filename}!")
                    logger.error(f"Remote: {remote_hash} | Local: {local_hash}")
                    # Delete the corrupted local file
                    local_path.unlink()
                    return False, retrieved_files
                    
                logger.info(f"✅ Integrity Verified: {filename} ({local_hash[:8]}...)")
                retrieved_files.append(local_path)

            return True, retrieved_files

        except Exception as e:
            logger.error(f"Artifact retrieval encountered a fatal error: {e}")
            return False, retrieved_files
            
        finally:
            if sftp:
                sftp.close()

if __name__ == "__main__":
    # Diagnostic / Test Run
    logging.basicConfig(level=logging.INFO)
    try:
        retriever = ArtifactRetriever()
        print("Artifact Retriever initialized. Cryptographic validation module is active.")
        # Example usage:
        # success, files = retriever.retrieve_artifacts(
        #     remote_dir="/scratch/user/job_12345", 
        #     local_dir=Path("./local_outputs")
        # )
    except Exception as e:
         print(f"Retriever Error: {e}")