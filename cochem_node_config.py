# cochem_canvas_target: cochem_node_config.py
"""
Configuration module for CoChem-NODE.
Unified under RegistryManager (cochem_system_config.json) (NODE-19).
"""

import json
from pathlib import Path
from typing import Optional

try:
    from cochem_registry_manager import RegistryManager, get_current_config
except ImportError:
    from Libraries.cochem_registry_manager import RegistryManager, get_current_config

class NODEConfig:
    """
    Unified configuration wrapper for CoChem-NODE system (NODE-19).
    Delegates all settings to RegistryManager.
    """
    
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
            print(f"⚠️ Error loading config from RegistryManager: {e}")
            return self._get_default_config()
            
    def _get_default_config(self) -> dict:
        """Get default configuration values."""
        return {
            "project_name": "CoChem-NODE",
            "version": "0.1.0",
            "data_dir": "./cochem_node_data",
            "scheduler": {
                "type": "slurm",
                "default_partition": "compute",
                "max_jobs_per_node": 10
            },
            "registry": {
                "type": "json",
                "file_path": "./cochem_system_config.json"
            },
            "logging": {
                "level": "INFO",
                "file": "./node.log",
                "max_file_size_mb": 100
            },
            "resources": {
                "max_concurrent_jobs": 8,
                "memory_per_job_gb": 4,
                "cpu_per_job": 2
            }
        }
        
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
            if self.registry_manager.config is not None:
                self.registry_manager.save()
        except Exception as e:
            print(f"⚠️ Warning saving via RegistryManager: {e}")
            
    def update_from_dict(self, updates: dict):
        """Update configuration from dictionary."""
        self.config.update(updates)
        self._save_config()

def main():
    """Main entry point for configuration module."""
    print("Initializing CoChem-NODE Configuration")
    
    config = NODEConfig()
    print("Current configuration:", config.config)

if __name__ == "__main__":
    main()