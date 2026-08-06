"""
CoChem-NODE: Registry Schema & Integrity Gateway
Implements Pydantic v2 data models for rigorous configuration validation, 
automatic legacy schema migration, and cryptographic integrity checks.
"""

from pydantic import BaseModel, Field, model_validator, ValidationError
from typing import Dict, Any, Optional
import hashlib
import json
import logging

logger = logging.getLogger("CoChem_Schema")

# ==========================================
# Nested Sub-Schemas
# ==========================================

class HardwareConfig(BaseModel):
    """Defines physical node limits. Hardware IDs are immutable."""
    cpu_cores: int = Field(..., gt=0, description="Available CPU compute cores")
    ram_mb: int = Field(..., gt=0, description="Total allocated system RAM in MB")
    avx512_support: bool = Field(default=False, description="Flag for AVX-512 vectorization")
    
    # Immutability Flag: Prevents accidental overwriting of environment constants
    host_id: str = Field(default="UNKNOWN", frozen=True, description="Immutable Host UUID")

class EnginePaths(BaseModel):
    """Maps executable paths for computational engines."""
    orca_path: str = Field(default="", description="Path to ORCA executable")
    openmpi_path: str = Field(default="", description="Path to OpenMPI bin directory")
    mace_model_path: Optional[str] = Field(default=None, description="Path to ML force field")

class HPCConfig(BaseModel):
    """CoChem-NODE specific cluster routing configurations."""
    cluster_hostname: str = Field(default="", description="FQDN of the remote cluster")
    partition: str = Field(default="normal", description="Default Slurm partition")
    ssh_key_path: str = Field(default="", description="Path to SSH identity file (or Vault alias)")
    username: str = Field(default="", description="Cluster login node username")

# ==========================================
# Master Registry Schema
# ==========================================

class CoChemConfig(BaseModel):
    """
    The Golden Gatekeeper schema for CoChem.
    Enforces structure, handles schema migrations, and tracks data integrity.
    """
    registry_version: str = Field(default="2.0", description="Schema version tracker")
    registry_checksum: str = Field(default="", description="SHA256 integrity hash")
    
    hardware: HardwareConfig
    engines: EnginePaths
    hpc: HPCConfig = Field(default_factory=HPCConfig)

    # ------------------------------------------
    # 1. Schema Migration Layer
    # ------------------------------------------
    @model_validator(mode='before')
    @classmethod
    def migrate_legacy_configs(cls, data: Any) -> Any:
        """
        Intercepts raw JSON dictionary before validation.
        If it detects a v1.0 config, it seamlessly migrates the keys to v2.0
        preventing the pipeline from crashing on older projects.
        """
        if not isinstance(data, dict):
            return data

        version = data.get("registry_version", "1.0")
        
        # Migration: v1.0 -> v2.0 (Flattened -> Nested)
        if version == "1.0":
            logger.info("Legacy v1.0 config detected. Applying schema migration to v2.0...")
            
            if "engines" not in data:
                data["engines"] = {}
                
            # Funnel old root-level paths into the nested engine schema
            if "orca_binary" in data:
                data["engines"]["orca_path"] = data.pop("orca_binary")
            if "mace_model" in data:
                data["engines"]["mace_model_path"] = data.pop("mace_model")
            
            # Ensure HPC block exists to pass validation
            if "hpc" not in data:
                data["hpc"] = {}
                
            data["registry_version"] = "2.0"
            
        return data

    # ------------------------------------------
    # 2. Cryptographic Integrity Check
    # ------------------------------------------
    def generate_hash(self) -> str:
        """
        Calculates the SHA256 hash of the configuration payload.
        Used to detect manual user tampering or file corruption.
        """
        # Dump the model to a dict, explicitly excluding the checksum field
        payload = self.model_dump(exclude={"registry_checksum"})
        
        # Sort keys to ensure deterministic hashing across OS platforms
        json_string = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(json_string.encode('utf-8')).hexdigest()

    def verify_integrity(self) -> bool:
        """
        Validates if the stored checksum matches the mathematically computed state.
        """
        if not self.registry_checksum:
            # Unverified / Brand new config
            return False 
        return self.registry_checksum == self.generate_hash()

# ==========================================
# Diagnostic / Pre-Flight Execution
# ==========================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        # Simulating a legacy v1.0 JSON payload being loaded from disk
        legacy_data = {
            "hardware": {"cpu_cores": 16, "ram_mb": 32000, "host_id": "NODE-ALPHA"},
            "orca_binary": "/opt/orca/orca"
        }
        
        # The migrator automatically catches 'orca_binary' and upgrades it.
        config = CoChemConfig(**legacy_data)
        
        # Calculate the lock hash
        config.registry_checksum = config.generate_hash()
        
        print("\n✅ Schema Validation & Migration: SUCCESS")
        print(f"   Computed Integrity Hash: {config.registry_checksum}")
        print(f"   ORCA Path smoothly migrated to: {config.engines.orca_path}")
        
    except ValidationError as e:
        print(f"\n❌ Schema Validation Failed:\n{e}")