"""
CoChem-NODE: Registry Schema & Integrity Gateway (v4.0.0 Upgrade)
Implements Pydantic v2 data models for rigorous configuration validation, 
automatic legacy schema migration, and cryptographic integrity checks.
"""

from pydantic import BaseModel, Field, model_validator, ValidationError
from typing import Dict, Any, Literal, Optional, Union
import hashlib
import json
import logging

logger = logging.getLogger("CoChem_Schema")

# ==========================================
# Nested Sub-Schemas
# ==========================================

class MPSConfig(BaseModel):
    """CUDA Multi-Process Service (MPS) configuration (§8A.4)."""
    enabled: bool = Field(default=True, description="Enable CUDA MPS daemon multiplexing")
    max_workers: int = Field(default=4, description="Max concurrent MPS worker tasks per GPU")
    thread_percentage: int = Field(default=25, description="CUDA MPS active thread percentage ceiling")
    pipe_dir: str = Field(default="/tmp/nvidia-mps", description="MPS pipe directory")
    log_dir: str = Field(default="/tmp/nvidia-log", description="MPS log directory")

class CorePinningConfig(BaseModel):
    """Core Pinning and Topology Configuration (§8A.0)."""
    kmp_hw_subset: str = Field(default="8c:intel_core,1t", description="OpenMP core pinning HW subset spec")
    anchor_p_cores: int = Field(default=7, description="Number of P-cores assigned to CPU anchor tasks")
    scout_p_cores: int = Field(default=1, description="Number of P-cores assigned to GPU scout tasks")
    background_e_cores: int = Field(default=8, description="E-cores reserved for OS/background tasks")

class HardwareConfig(BaseModel):
    """Defines physical node limits and hardware topology."""
    cpu_cores: int = Field(default=8, gt=0, description="Available CPU compute cores")
    ram_mb: int = Field(default=32000, gt=0, description="Total allocated system RAM in MB")
    physical_cpu_cores: Optional[int] = Field(default=8, description="Physical CPU cores count")
    logical_cpu_cores: Optional[int] = Field(default=16, description="Logical CPU cores count")
    ram_gb: Optional[float] = Field(default=32.0, description="Total system RAM in GB")
    avx512_support: bool = Field(default=True, description="Flag for AVX-512 vectorization")
    gpu_profile: Optional[str] = Field(default="NVIDIA RTX 4090", description="Target GPU hardware profile")
    vram_gb: Optional[float] = Field(default=24.0, description="GPU VRAM in GB")
    subnormal_precision_trap: bool = Field(default=False, description="Subnormal FP precision trap flag")
    os_target: Optional[str] = Field(default="win32_x86_64", description="Target OS architecture")
    host_id: str = Field(default="UNKNOWN", frozen=True, description="Immutable Host UUID")
    mps: MPSConfig = Field(default_factory=MPSConfig, description="CUDA MPS config")
    core_pinning: CorePinningConfig = Field(default_factory=CorePinningConfig, description="Core pinning topology")

    @model_validator(mode='before')
    @classmethod
    def flex_hardware_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "cpu_cores" not in data and "physical_cpu_cores" in data:
                data["cpu_cores"] = data["physical_cpu_cores"]
            elif "physical_cpu_cores" not in data and "cpu_cores" in data:
                data["physical_cpu_cores"] = data["cpu_cores"]

            if "ram_mb" not in data and "ram_gb" in data:
                data["ram_mb"] = int(data["ram_gb"] * 1024)
            elif "ram_gb" not in data and "ram_mb" in data:
                data["ram_gb"] = float(data["ram_mb"]) / 1024.0
        return data

class EngineItem(BaseModel):
    """Individual computational engine metadata."""
    status: str = Field(default="found")
    path: str = Field(default="")
    version: str = Field(default="")
    gpu_support: bool = Field(default=False)
    track: Optional[str] = Field(default=None)
    engine: Optional[str] = Field(default=None)
    hash: str = Field(default="auto")

class EnginePaths(BaseModel):
    """Maps executable paths for computational engines (v1/v2 compatibility)."""
    orca_path: str = Field(default="", description="Path to ORCA executable")
    openmpi_path: str = Field(default="", description="Path to OpenMPI bin directory")
    mace_model_path: Optional[str] = Field(default=None, description="Path to ML force field")

class HPCConfig(BaseModel):
    """CoChem-NODE specific cluster routing configurations."""
    cluster_hostname: str = Field(default="", description="FQDN of the remote cluster")
    partition: str = Field(default="compute", description="Default Slurm partition")
    default_partition: str = Field(default="compute", description="Default partition name")
    scheduler: str = Field(default="local", description="HPC scheduler type")
    ssh_key_path: str = Field(default="", description="Path to SSH identity file")
    username: str = Field(default="", description="Cluster login node username")
    execution_mode: Literal["ssh", "local"] = Field(
        default="local",
        description="Execution mode: 'ssh' for remote HPC cluster, 'local' for local queue mode"
    )
    walltime_budgets: Dict[str, str] = Field(
        default_factory=lambda: {
            "T1-10s": "00:00:10",
            "T1-1min": "00:01:00",
            "T1-30min": "00:30:00",
            "T2-1h": "01:00:00",
            "T2-3h": "03:00:00",
            "T2-12h": "12:00:00",
            "T3-1d": "24:00:00",
            "T3-3d": "72:00:00",
            "T4-1w": "168:00:00",
            "T4-1mo": "720:00:00"
        },
        description="10 Wall-Clock Budget Tiers"
    )

    @model_validator(mode='after')
    def infer_execution_mode(self) -> 'HPCConfig':
        """If cluster_hostname is empty or unset, force execution_mode to 'local'."""
        if not self.cluster_hostname:
            object.__setattr__(self, 'execution_mode', 'local')
        return self

# ==========================================
# Master Registry Schema
# ==========================================

class CoChemConfig(BaseModel):
    """
    The Golden Gatekeeper schema for CoChem (v4.0.0).
    Enforces structure, handles schema migrations, and tracks data integrity.
    """
    schema_version: str = Field(default="4.0.0", description="System schema version tracker")
    registry_version: str = Field(default="4.0", description="Registry schema version tracker")
    orca_version: Optional[str] = Field(default="6.1.1", description="Default ORCA version")
    rdkit_random_seed: Optional[int] = Field(default=42, description="Global random seed")
    registry_checksum: str = Field(default="", description="SHA256 integrity hash")
    
    hardware: HardwareConfig = Field(default_factory=HardwareConfig)
    engines: Union[Dict[str, Any], EnginePaths] = Field(default_factory=dict)
    hpc: HPCConfig = Field(default_factory=HPCConfig)

    @property
    def walltime_budgets(self) -> Dict[str, str]:
        """Accessor for walltime_budgets inside hpc config block."""
        return self.hpc.walltime_budgets

    # ------------------------------------------
    # 1. Schema Migration Layer
    # ------------------------------------------
    @model_validator(mode='before')
    @classmethod
    def migrate_legacy_configs(cls, data: Any) -> Any:
        """
        Intercepts raw JSON dictionary before validation.
        Seamlessly migrates v1.0 and v2.0 configurations to v4.0.0.
        """
        if not isinstance(data, dict):
            return data

        version = str(data.get("registry_version", data.get("schema_version", "1.0")))
        
        # Migration: v1.0 / v2.0 -> v4.0
        if version in ["1.0", "2.0"]:
            logger.info(f"Legacy {version} config detected. Applying schema migration to v4.0...")
            
            if "engines" not in data or not isinstance(data["engines"], dict):
                data["engines"] = {}
                
            if "orca_binary" in data and isinstance(data["engines"], dict):
                data["engines"]["orca_path"] = data.pop("orca_binary")
            if "mace_model" in data and isinstance(data["engines"], dict):
                data["engines"]["mace_model_path"] = data.pop("mace_model")
            
            if "hpc" not in data or not isinstance(data["hpc"], dict):
                data["hpc"] = {}
                
            data["registry_version"] = "4.0"
            data["schema_version"] = "4.0.0"

        # Purge legacy silos if present
        data.pop("silos", None)

        return data

    # ------------------------------------------
    # 2. Cryptographic Integrity Check
    # ------------------------------------------
    def generate_hash(self) -> str:
        """
        Calculates the SHA256 hash of the configuration payload.
        Used to detect manual user tampering or file corruption.
        """
        payload = self.model_dump(exclude={"registry_checksum"})
        json_string = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(json_string.encode('utf-8')).hexdigest()

    def verify_integrity(self) -> bool:
        """
        Validates if the stored checksum matches the mathematically computed state.
        """
        if not self.registry_checksum:
            return False 
        return self.registry_checksum == self.generate_hash()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        with open("../cochem_system_config.json", "r", encoding="utf-8") as f:
            raw = json.loads(f.read())
        cfg = CoChemConfig(**raw)
        cfg.registry_checksum = cfg.generate_hash()
        logger.info("Schema Validation & Hash: SUCCESS")
        logger.info(f"   Integrity Hash: {cfg.registry_checksum}")
    except Exception as e:
        logger.error(f"Schema Validation Failed: {e}")