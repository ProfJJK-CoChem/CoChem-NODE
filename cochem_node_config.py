# cochem_canvas_target: cochem_node_config.py
"""
Configuration module for CoChem-NODE.
Handles all configuration settings for the NODE system.
"""

import json
from pathlib import Path

class NODEConfig:
    """
    Configuration class for CoChem-NODE system.
    """
    
    def __init__(self, config_file: str = "cochem_node_config.json"):
        """Initialize configuration."""
        self.config_file = config_file
        self.config = self._load_config()
        
    def _load_config(self) -> dict:
        """Load configuration from file."""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Return default configuration
            return self._get_default_config()
        except json.JSONDecodeError as e:
            print(f"❌ Error loading config: {e}")
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
                "file_path": "./registry.json"
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
        """Save current configuration to file."""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
            
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