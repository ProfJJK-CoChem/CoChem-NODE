import pytest
import os
from pathlib import Path
from cochem_registry_manager import RegistryManager, RegistryLock

def test_registry_manager(tmp_path):
    config_file = tmp_path / "cochem_system_config.json"
    manager = RegistryManager(str(config_file))
    assert manager.registry_path == config_file.resolve()

def test_registry_lock(tmp_path):
    target = tmp_path / "test.json"
    with RegistryLock(target):
        assert (tmp_path / ".test.json.lock").exists()
    assert not (tmp_path / ".test.json.lock").exists()
