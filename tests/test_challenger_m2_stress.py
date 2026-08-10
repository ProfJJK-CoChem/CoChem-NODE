"""
CoChem-NODE: Challenger M2-1 Stress & Boundary Test Suite
Empirically tests edge cases, config object polymorphic handling, checksum integrity,
Slurm templating walltime resolution, and Scout-Anchor co-scheduling bounds.
"""

import json
import pytest
from pathlib import Path

from cochem_registry_schema import CoChemConfig
from cochem_node_config import NODEConfig
from cochem_slurm_templater import SlurmTemplater
from cochem_hpc_dispatch import ScoutAnchorCoScheduler, HPCDispatcher

SYSTEM_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "cochem_system_config.json"


def test_challenger_slurm_templater_dict_config_handling():
    """
    Stress-test SlurmTemplater when config is passed as dict or NODEConfig instance.
    Document empirical behavior: dict/NODEConfig instances fail getattr lookups
    in SlurmTemplater.render_job(), causing tier walltime resolution to fall back to '00:30:00'.
    """
    nc = NODEConfig(str(SYSTEM_CONFIG_PATH))
    # Passing dict config
    st_dict = SlurmTemplater(config=nc.config)
    script_dict = st_dict.render_job("TestDict", "/tmp", "echo 1", tier="T4-1mo")
    
    # Passing NODEConfig instance
    st_nc = SlurmTemplater(config=nc)
    script_nc = st_nc.render_job("TestNC", "/tmp", "echo 1", tier="T4-1mo")
    
    # Passing CoChemConfig model instance
    cfg_model = CoChemConfig(**json.load(open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8")))
    st_model = SlurmTemplater(config=cfg_model)
    script_model = st_model.render_job("TestModel", "/tmp", "echo 1", tier="T4-1mo")
    
    # Model succeeds in resolving T4-1mo to 720:00:00
    assert "#SBATCH --time=720:00:00" in script_model
    
    # Dict and NODEConfig instance fall back to default walltime "00:30:00" due to attribute access mismatch
    assert "#SBATCH --time=00:30:00" in script_dict
    assert "#SBATCH --time=00:30:00" in script_nc


def test_challenger_system_config_integrity_checksum():
    """
    Empirically inspect registry_checksum in cochem_system_config.json.
    Currently empty string "", so verify_integrity() returns False until populated.
    """
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    cfg = CoChemConfig(**data)
    generated = cfg.generate_hash()
    
    assert len(generated) == 64
    # Note: verify_integrity() is False because registry_checksum is currently ""
    assert cfg.verify_integrity() is False


def test_challenger_local_dispatch_file_validation(tmp_path):
    """
    Empirically test HPCDispatcher.dispatch_job in local mode with missing input file.
    In local mode, dispatch_job returns True without validating local_input_file existence.
    """
    dispatcher = HPCDispatcher()
    missing_file = tmp_path / "does_not_exist.inp"
    
    success, msg = dispatcher.dispatch_job(
        job_name="MissingInputJob",
        local_input_file=missing_file,
        remote_work_dir=str(tmp_path),
        engine_name="ORCA",
        execution_command="echo 1",
        requested_cores=2,
        requested_memory_mb=1000
    )
    # Local mode returns True early without verifying missing input file
    assert success is True
    assert "LOCAL_JOB_MissingInputJob" in msg


def test_challenger_walltime_all_10_tiers_pydantic():
    """
    Verify all 10 wall-clock tier budgets render correctly with Pydantic CoChemConfig.
    """
    cfg_model = CoChemConfig(**json.load(open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8")))
    st = SlurmTemplater(config=cfg_model)
    
    expected_walltimes = {
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
    
    for tier, expected_time in expected_walltimes.items():
        script = st.render_job(f"Job_{tier}", "/scratch", "echo 1", tier=tier)
        assert f"#SBATCH --time={expected_time}" in script


def test_challenger_mps_control_rendering_combinations():
    """
    Test GPU and MPS combinations in SlurmTemplater.
    """
    st = SlurmTemplater()
    
    # GPU=True, MPS=True -> includes start & shutdown
    s_gpu_mps = st.render_job("G1", "/tmp", "cmd", use_gpu=True, mps_enabled=True)
    assert "nvidia-cuda-mps-control -d" in s_gpu_mps
    assert 'echo "quit" | nvidia-cuda-mps-control' in s_gpu_mps
    
    # GPU=True, MPS=False -> excludes MPS
    s_gpu_nomps = st.render_job("G2", "/tmp", "cmd", use_gpu=True, mps_enabled=False)
    assert "nvidia-cuda-mps-control" not in s_gpu_nomps
    
    # GPU=False, MPS=True -> excludes MPS (must be GPU task)
    s_cpu_mps = st.render_job("C1", "/tmp", "cmd", use_gpu=False, mps_enabled=True)
    assert "nvidia-cuda-mps-control" not in s_cpu_mps


def test_challenger_scout_anchor_partitioning_and_contention():
    """
    Verify ScoutAnchorCoScheduler core partitioning and contention bound threshold.
    """
    st = SlurmTemplater()
    co_sched = ScoutAnchorCoScheduler(st)
    
    anchor_script, scout_script = co_sched.prepare_co_scheduled_payloads(
        anchor_spec={"job_name": "AnchorTask", "work_dir": "/tmp"},
        scout_spec={"job_name": "ScoutTask", "work_dir": "/tmp"}
    )
    
    assert "#SBATCH --ntasks-per-node=7" in anchor_script
    assert "#SBATCH --ntasks-per-node=1" in scout_script
    
    # Contention bound <= 1.20x
    assert co_sched.verify_contention_bound(119.9, 100.0) is True
    assert co_sched.verify_contention_bound(120.0, 100.0) is True
    assert co_sched.verify_contention_bound(120.1, 100.0) is False


def test_challenger_schema_migration_v1_v2_to_v4():
    """
    Test automatic schema migration from legacy v1.0 / v2.0 JSON to v4.0.0.
    """
    legacy_data = {
        "registry_version": "1.0",
        "orca_binary": "/usr/bin/orca",
        "mace_model": "/models/mace.pt",
        "silos": {"legacy_silo": True}
    }
    migrated = CoChemConfig.migrate_legacy_configs(legacy_data)
    
    assert migrated["registry_version"] == "4.0"
    assert migrated["schema_version"] == "4.0.0"
    assert "silos" not in migrated
    assert migrated["engines"]["orca_path"] == "/usr/bin/orca"
    assert migrated["engines"]["mace_model_path"] == "/models/mace.pt"
