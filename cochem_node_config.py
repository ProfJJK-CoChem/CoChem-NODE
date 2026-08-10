# cochem_canvas_target: cochem_node_config.py
"""
Configuration module for CoChem-NODE.
Upgraded under v4 Method Matrix architecture (NODE-02).
Implements dynamic tier queue routing, CUDA MPS caps, and core pinning configuration.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger("CoChem_NODE_Config")

try:
    from cochem_registry_manager import RegistryManager, get_current_config
except ImportError:
    from Libraries.cochem_registry_manager import RegistryManager, get_current_config

class NODEConfig:
    """
    Unified configuration wrapper for CoChem-NODE system (NODE-02).
    Delegates settings to RegistryManager with dynamic v4 tier routing and MPS controls.
    """
    
    # 10 Wall-Clock Tier Registry (§8A)
    TIER_WALLTIME_MAP: Dict[str, str] = {
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
    }

    # Dynamic Tier Concurrency Limits (§8A.2)
    TIER_CONCURRENCY_MAP: Dict[str, int] = {
        "T1-10s": 16,
        "T1-1min": 8,
        "T1-30min": 4,
        "T2-1h": 2,
        "T2-3h": 2,
        "T2-12h": 1,
        "T3-1d": 1,
        "T3-3d": 1,
        "T4-1w": 1,
        "T4-1mo": 1
    }

    def __init__(self, config_file: str = "cochem_system_config.json"):
        """Initialize configuration connected to global RegistryManager."""
        self.config_file = config_file
        self.registry_manager = RegistryManager(registry_path=config_file)
        self.config = self._load_config()
        
    def _load_config(self) -> dict:
        """Load configuration via RegistryManager."""
        try:
            cfg_obj = self.registry_manager.get_config()
            return cfg_obj.model_dump()
        except Exception as e:
            logger.warning(f"Error loading config from RegistryManager: {e}. Falling back to v4 defaults.")
            return self._get_default_config()
            
    def _get_default_config(self) -> dict:
        """Get v4 default configuration values."""
        return {
            "project_name": "CoChem-NODE",
            "version": "4.0.0",
            "schema_version": "4.0.0",
            "data_dir": "./cochem_node_data",
            "scheduler": {
                "type": "slurm",
                "default_partition": "compute",
                "dynamic_routing_enabled": True
            },
            "walltime_budgets": self.TIER_WALLTIME_MAP,
            "tier_concurrency": self.TIER_CONCURRENCY_MAP,
            "mps": {
                "enabled": True,
                "max_workers": 4,
                "thread_percentage": 25,
                "pipe_dir": "/tmp/nvidia-mps",
                "log_dir": "/tmp/nvidia-log"
            },
            "core_pinning": {
                "kmp_hw_subset": "8c:intel_core,1t",
                "anchor_p_cores": 7,
                "scout_p_cores": 1,
                "background_e_cores": 8
            },
            "registry": {
                "type": "json",
                "file_path": "./cochem_system_config.json"
            },
            "logging": {
                "level": "INFO",
                "file": "./node.log",
                "max_file_size_mb": 100
            }
        }
        
    def get_walltime_for_tier(self, tier_name: str) -> str:
        """Retrieve walltime string for a specific v4 tier."""
        budgets = self.config.get("hpc", {}).get("walltime_budgets", self.TIER_WALLTIME_MAP)
        if tier_name not in budgets:
            raise KeyError(f"Invalid wall-clock budget tier '{tier_name}'. Valid tiers: {list(budgets.keys())}")
        return budgets[tier_name]

    def get_tier_concurrency_limit(self, tier_name: str) -> int:
        """Retrieve dynamic concurrency limit for a v4 tier."""
        return self.TIER_CONCURRENCY_MAP.get(tier_name, 1)

    def get_mps_config(self) -> dict:
        """Retrieve CUDA MPS configuration dictionary."""
        return self.config.get("hardware", {}).get("mps", self._get_default_config()["mps"])

    def get_scout_anchor_allocation(self) -> dict:
        """Retrieve Scout-and-Anchor P-core allocation map (§8A.2)."""
        return self.config.get("hardware", {}).get("core_pinning", self._get_default_config()["core_pinning"])

    def validate_tier(self, tier_name: str) -> bool:
        """Validate whether a tier string is a recognized v4 tier."""
        return tier_name in self.TIER_WALLTIME_MAP

    def get(self, key: str, default=None):
        """Get configuration value by key."""
        return self.config.get(key, default)
        
    def set(self, key: str, value):
        """Set configuration value."""
        self.config[key] = value
        self._save_config()
        
    def _save_config(self):
        """Save current configuration using RegistryManager."""
        try:
            if hasattr(self.registry_manager, 'config') and self.registry_manager.config is not None:
                self.registry_manager.save()
        except Exception as e:
            logger.warning(f"Warning saving via RegistryManager: {e}")
            
    def update_from_dict(self, updates: dict):
        """Update configuration from dictionary."""
        self.config.update(updates)
        self._save_config()

def main():
    """Main entry point for configuration module."""
    print("Initializing CoChem-NODE v4 Configuration")
    config = NODEConfig()
    print("Loaded v4 Configuration. Tiers:", list(config.TIER_WALLTIME_MAP.keys()))

if __name__ == "__main__":
    main()