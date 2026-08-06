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

# Import the validated Pydantic schema from Stage 1.0
try:
    from Libraries.cochem_registry_schema import CoChemConfig
except ImportError:
    # Fallback for localized testing
    from cochem_registry_schema import CoChemConfig

logger = logging.getLogger("CoChem_Registry")
logger.setLevel(logging.INFO)

# ==========================================
# Custom Exception Hierarchy
# ==========================================
class CoChemRegistryLockError(Exception):
    """Raised when the registry lock cannot be acquired within the timeout."""
    pass

class CoChemSecurityError(Exception):
    """Raised when registry integrity or permissions are compromised."""
    pass

# ==========================================
# Cross-Platform Lock Manager
# ==========================================
class RegistryLock:
    """
    A cross-platform, multi-process lock utilizing atomic directory creation.
    (Suggestion 4 & 8: Cross-Platform Safe Locking & Graceful Degradation)
    """
    def __init__(self, target_file: Path, timeout: float = 5.0):
        self.lock_dir = target_file.parent / f".{target_file.name}.lock"
        self.timeout = timeout

    def __enter__(self):
        start_time = time.time()
        while True:
            try:
                os.mkdir(self.lock_dir)
                return self
            except FileExistsError:
                if time.time() - start_time > self.timeout:
                    raise CoChemRegistryLockError(
                        f"CRITICAL: Timeout ({self.timeout}s) waiting for registry lock. "
                        f"Another CoChem module may have crashed. Manually delete {self.lock_dir} if stuck."
                    )
                time.sleep(0.1) # NFS-aware polling delay

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
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
    
    def __new__(cls, registry_path: str = "cochem_system_config.json"):
        if cls._instance is None:
            cls._instance = super(RegistryManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, registry_path: str = "cochem_system_config.json"):
        if self._initialized:
            return
            
        self.registry_path = Path(registry_path).resolve()
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

    def _enforce_permissions(self):
        """Forces strict 0600 (rw-------) permissions on the registry. (Suggestion 1)"""
        try:
            os.chmod(self.registry_path, 0o600)
        except OSError as e:
            logger.warning(f"Could not enforce strict OS permissions on {self.registry_path}: {e}")

    def _rotate_backups(self):
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
                    raw_data = json.load(f)
                
                # Resolve ${ENV_VARS}
                interpolated_data = self._interpolate_env_vars(raw_data)
                
                # Pydantic handles Schema Migration & Validation
                self.config = CoChemConfig(**interpolated_data)
                
                # Cryptographic Verification
                if not self.config.verify_integrity():
                    logger.warning("REGISTRY WARNING: Checksum mismatch. File may have been edited manually.")
                
                return self.config
                
            except FileNotFoundError:
                raise FileNotFoundError(f"Registry not found at {self.registry_path}. Run CoChem Stage 0 setup.")
            except json.JSONDecodeError as e:
                raise ValueError(f"CRITICAL: Registry is structurally corrupted (invalid JSON): {e}")
            except ValidationError as e:
                logger.error(f"SCHEMA VALIDATION FAILED:\n{e.json(indent=2)}")
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
    print("Initializing Registry Manager...")
    try:
        manager = RegistryManager("cochem_system_config.json")
        print("Registry Manager loaded successfully. Golden Gatekeeper is active.")
    except Exception as e:
        print(f"Initialization Failed: {e}")