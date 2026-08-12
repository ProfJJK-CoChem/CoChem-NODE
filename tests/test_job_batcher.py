import pytest
from pathlib import Path
from cochem_job_batcher import HPCBatcher

def test_job_batcher_create_batch(tmp_path) -> None:
    batcher = HPCBatcher()
    tasks = [f"task_{i}" for i in range(25)]
    scripts = batcher.create_batch(tasks, "TestModule")
    assert len(scripts) > 0
    for s in scripts:
        assert Path(s).exists()
