import os
import json
import pytest
import tempfile
from pathlib import Path

from cochem_registry_manager import RegistryLock, RegistryManager, CoChemRegistryLockError
from cochem_job_batcher import HPCBatcher
from cochem_node_main import NODEOrchestrator

def test_registry_lock_acquisition_and_release(tmp_path) -> None:
    target = tmp_path / "test_config.json"
    target.write_text('{"test": 123}')
    
    with RegistryLock(target, timeout=1.0):
        lock_dir = target.parent / f".{target.name}.lock"
        assert lock_dir.exists()
        
    assert not lock_dir.exists()

def test_hpc_batcher_creation(tmp_path) -> None:
    batcher = HPCBatcher()
    tasks = [f"task_{i}.inp" for i in range(25)]
    scripts = batcher.create_batch(tasks, "TestEngine")
    assert len(scripts) > 0
    assert Path(scripts[0]).exists()

def test_node_orchestrator(tmp_path) -> None:
    orchestrator = NODEOrchestrator()
    orchestrator.initialize()
    assert orchestrator.is_initialized is True
    
    res = orchestrator.run_node_management("poll", {"job_id": "999999"})
    assert "job_status" in res
    
    rep = orchestrator.generate_node_report(str(tmp_path))
    assert Path(rep).exists()
