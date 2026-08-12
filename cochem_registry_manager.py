"""
CoChem-NODE: Registry Manager (Golden Gatekeeper)
Handles atomic serialization, strict permission enforcing, environment variable
interpolation, and cross-platform file locking for the CoChem ecosystem.
"""

import os
import json
import time
import shutil
import logging
from pathlib import Path
from typing import Any, Optional
from pydantic import ValidationError

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import the validated Pydantic schema from Stage 1.0
try:
    from cochem_registry_schema import CoChemConfig
except ImportError:
    try:
        from Libraries.cochem_registry_schema import CoChemConfig
    except ImportError:
        from .cochem_registry_schema import CoChemConfig

logger = logging.getLogger("CoChem_Registry")
logger.setLevel(logging.INFO)

# ==========================================
# Custom Exception Hierarchy
# ==========================================
class CoChemRegistryLockError(Exception):
    """Raised when the registry lock cannot be acquired within the timeout."""

class CoChemSecurityError(Exception):
    """Raised when registry integrity or permissions are compromised."""

# ==========================================
# Cross-Platform Lock Manager
# ==========================================
class RegistryLock:
    """
    A cross-platform, multi-process lock utilizing atomic directory creation.
    Implements stale lock detection to prevent deadlocks from process crashes (NODE-18).
    """
    def __init__(self, target_file: Path, timeout: float = 5.0, stale_age_sec: float = 60.0) -> None:
        self.lock_dir = target_file.parent / f".{target_file.name}.lock"
        self.timeout = timeout
        self.stale_age_sec = stale_age_sec

    def __enter__(self) -> "RegistryLock":
        start_time = time.time()
        while True:
            try:
                os.mkdir(self.lock_dir)
                return self
            except FileExistsError:
                # Check for stale lock directory mtime (NODE-18)
                try:
                    if self.lock_dir.exists():
                        mtime = self.lock_dir.stat().st_mtime
                        if (time.time() - mtime) > self.stale_age_sec:
                            logger.warning(f"⚠️ Stale registry lock detected ({self.lock_dir}). Evicting abandoned lock.")
                            shutil.rmtree(self.lock_dir, ignore_errors=True)
                            continue
                except OSError as err:
                    logger.debug(f"Stale lock check skipped: {err}")

                if time.time() - start_time > self.timeout:
                    raise CoChemRegistryLockError(
                        f"CRITICAL: Timeout ({self.timeout}s) waiting for registry lock. "
                        f"Another CoChem module may have crashed. Manually delete {self.lock_dir} if stuck."
                    )
                time.sleep(0.1) # NFS-aware polling delay

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if self.lock_dir.exists():
                os.rmdir(self.lock_dir)
        except OSError as e:
            logger.warning(f"Failed to release lock directory {self.lock_dir}: {e}")

# ==========================================
# Registry Manager (Singleton)
# ==========================================
class RegistryManager:
    """
    Singleton manager for the CoChem Registry. (Suggestion 3)
    Ensures that multiple modules within the same Python kernel share the same config instance.
    """
    _instance = None
    
    def __new__(cls, registry_path: str = "cochem_system_config.json") -> "RegistryManager":
        if cls._instance is None:
            cls._instance = super(RegistryManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, registry_path: str = "cochem_system_config.json") -> None:
        self.registry_path = Path(registry_path).resolve()
        if self._initialized:
            return
            
        self.config: Optional[CoChemConfig] = None
        self._initialized = True
        
        # Auto-load on initialization if the file exists
        if self.registry_path.exists():
            self.load()

    # ------------------------------------------
    # Security & Portability Helper Methods
    # ------------------------------------------
    def _interpolate_env_vars(self, data: Any) -> Any:
        """Recursively resolves ${VAR} patterns in string values."""
        if isinstance(data, dict):
            return {k: self._interpolate_env_vars(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._interpolate_env_vars(v) for v in data]
        elif isinstance(data, str):
            return os.path.expandvars(data)
        return data

    def _enforce_permissions(self) -> None:
        """Forces strict 0600 (rw-------) permissions on the registry. (Suggestion 1)"""
        try:
            os.chmod(self.registry_path, 0o600)
        except OSError as e:
            logger.warning(f"Could not enforce strict OS permissions on {self.registry_path}: {e}")

    def _rotate_backups(self) -> None:
        """Maintains a rolling buffer of 5 registry backups. (Suggestion 5)"""
        if not self.registry_path.exists():
            return

        # Shift existing backups down (bak.4 -> bak.5)
        for i in range(4, 0, -1):
            old_bak = Path(f"{self.registry_path}.bak.{i}")
            new_bak = Path(f"{self.registry_path}.bak.{i+1}")
            if old_bak.exists():
                os.replace(old_bak, new_bak)
                
        # Copy current state to bak.1
        shutil.copy2(self.registry_path, Path(f"{self.registry_path}.bak.1"))

    # ------------------------------------------
    # Core I/O Operations
    # ------------------------------------------
    def load(self) -> CoChemConfig:
        """
        Reads the JSON, interpolates variables, applies the migration layer,
        validates against the schema, and verifies the checksum.
        """
        with RegistryLock(self.registry_path, timeout=5.0):
            try:
                with open(self.registry_path, 'r', encoding='utf-8') as f:
                    raw_data = json.loads(f.read())
                
                # Resolve ${ENV_VARS}
                interpolated_data = self._interpolate_env_vars(raw_data)
                
                # Pydantic handles Schema Migration & Validation
                self.config = CoChemConfig(**interpolated_data)
                
                # Cryptographic Verification
                if not self.config.verify_integrity():
                    logger.warning("REGISTRY WARNING: Checksum mismatch. File may have been edited manually.")
                
                return self.config
                
            except FileNotFoundError:
                try:
                    from Libraries.cochem_registry_schema import HardwareConfig, EnginePaths, HPCConfig
                except ImportError:
                    from cochem_registry_schema import HardwareConfig, EnginePaths, HPCConfig
                self.config = CoChemConfig(
                    hardware=HardwareConfig(cpu_cores=4, ram_mb=8192),
                    engines=EnginePaths(),
                    hpc=HPCConfig()
                )
                return self.config
            except json.JSONDecodeError as e:
                raise ValueError(f"CRITICAL: Registry is structurally corrupted (invalid JSON): {e}")
            except ValidationError as e:
                logger.error("SCHEMA VALIDATION FAILED:" + "\n" + f"{e.json(indent=2)}")
                raise

    def save(self) -> None:
        """
        Performs an atomic, lock-protected write to prevent data corruption.
        """
        if self.config is None:
            raise ValueError("Cannot save an empty configuration. Load or initialize first.")
            
        with RegistryLock(self.registry_path, timeout=5.0):
            # 1. Update cryptographic hash
            self.config.registry_checksum = self.config.generate_hash()
            
            # 2. Extract payload
            payload = self.config.model_dump(mode='json')
            temp_path = self.registry_path.with_suffix('.json.tmp')

            try:
                # 3. Rotate backups
                self._rotate_backups()

                # 4. Write to temporary file
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(payload, f, indent=4, sort_keys=True)
                
                # Flush to ensure OS actually writes bits to disk before swapping
                f.flush()
                os.fsync(f.fileno())
                
                # 5. Atomic Swap
                os.replace(temp_path, self.registry_path)
                
                # 6. Secure the new file
                self._enforce_permissions()
                
                logger.info(f"Registry successfully saved to {self.registry_path}")
                
            except Exception as e:
                if temp_path.exists():
                    temp_path.unlink()
                raise IOError(f"Failed to write registry atomically: {e}")

    def get_config(self) -> CoChemConfig:
        """Returns the active, validated configuration object."""
        if self.config is None:
            self.load()
        return self.config

# ==========================================
# Ecosystem Accessor
# ==========================================
def get_current_config(path: str = "cochem_system_config.json") -> CoChemConfig:
    """Helper method for downstream modules to fetch the active config."""
    manager = RegistryManager(path)
    return manager.get_config()

if __name__ == "__main__":
    # Diagnostic / Test Run
    logger.info("Initializing Registry Manager...")
    try:
        manager = RegistryManager("cochem_system_config.json")
        logger.info("Registry Manager loaded successfully. Golden Gatekeeper is active.")
    except Exception as e:
        logger.error(f"Initialization Failed: {e}")