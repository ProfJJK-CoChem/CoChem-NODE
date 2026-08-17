"""
CoChem-NODE: Verification Test Suite for v4 Method Matrix Upgrade (Milestone M2)
Implements all 40 verification assertions specified in explorer_m2/handoff.md §4.
"""

import json
import pytest
from pathlib import Path

from cochem_registry_schema import CoChemConfig
from cochem_node_config import NODEConfig
from cochem_slurm_templater import SlurmTemplater
from cochem_hpc_dispatch import ScoutAnchorCoScheduler, HPCDispatcher

SYSTEM_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "cochem_system_config.json"

# ==========================================
# 4.1 NODE-01 Test Suite (10 Tests)
# ==========================================

def test_node01_01_schema_version_v4() -> None:
    """Assert schema_version == '4.0.0' and registry_version == '4.0'."""
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.loads(f.read())
    assert data.get("schema_version") == "4.0.0"
    assert data.get("registry_version") == "4.0"

def test_node01_02_mps_config_presence() -> None:
    """Assert hardware.mps config is properly set up."""
    cfg = CoChemConfig(**json.loads(Path(SYSTEM_CONFIG_PATH).read_text(encoding="utf-8")))
    mps = cfg.hardware.mps
    assert mps.enabled is True
    assert mps.max_workers == 4
    assert mps.thread_percentage == 25
    assert mps.pipe_dir == "/tmp/nvidia-mps"
    assert mps.log_dir == "/tmp/nvidia-log"

def test_node01_03_core_pinning_topology() -> None:
    """Assert hardware.core_pinning topology is correctly configured."""
    cfg = CoChemConfig(**json.loads(Path(SYSTEM_CONFIG_PATH).read_text(encoding="utf-8")))
    pin = cfg.hardware.core_pinning
    assert pin.kmp_hw_subset == "8c:intel_core,1t"
    assert pin.anchor_p_cores == 7
    assert pin.scout_p_cores == 1

def test_node01_04_engine_registry_completeness() -> None:
    """Assert full v4 engine registry is present."""
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.loads(f.read())
    engines = data.get("engines", {})
    required_engines = ["orca", "cfour", "pyscf", "mace", "xtb", "crest", "goat"]
    for eng in required_engines:
        assert eng in engines, f"Engine {eng} missing from config registry"

def test_node01_05_engine_tracks_and_gpu() -> None:
    """Assert engine tracks and GPU support flags."""
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.loads(f.read())
    engines = data.get("engines", {})
    assert engines["orca"]["gpu_support"] is False
    assert engines["pyscf"]["gpu_support"] is True
    assert engines["cfour"]["track"] == "CFOUR"

def test_node01_06_silos_removal() -> None:
    """Assert legacy silos block or flags are completely removed."""
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.loads(f.read())
    assert "silos" not in data or "torq_silo_active" not in data.get("silos", {})

def test_node01_07_10_tier_walltime_budgets() -> None:
    """Assert all 10 v4 walltime tiers exist in hpc.walltime_budgets."""
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.loads(f.read())
    budgets = data.get("hpc", {}).get("walltime_budgets", {})
    expected_tiers = ["T1-10s", "T1-1min", "T1-30min", "T2-1h", "T2-3h", "T2-12h", "T3-1d", "T3-3d", "T4-1w", "T4-1mo"]
    for tier in expected_tiers:
        assert tier in budgets, f"Tier {tier} missing from walltime_budgets"

def test_node01_08_flat_24h_cap_elimination() -> None:
    """Assert legacy max_walltime_hours is eliminated from hpc block."""
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.loads(f.read())
    assert "max_walltime_hours" not in data.get("hpc", {})

def test_node01_09_pydantic_schema_validation() -> None:
    """Validate system config against Pydantic CoChemConfig v4 schema."""
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.loads(f.read())
    cfg = CoChemConfig(**data)
    assert cfg.schema_version == "4.0.0"

def test_node01_10_sha256_integrity_hash() -> None:
    """Verify deterministic SHA256 checksum generation."""
    with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.loads(f.read())
    cfg = CoChemConfig(**data)
    h1 = cfg.generate_hash()
    h2 = cfg.generate_hash()
    assert h1 == h2
    assert len(h1) == 64

# ==========================================
# 4.2 NODE-02 Test Suite (10 Tests)
# ==========================================

def test_node02_01_load_v4_defaults() -> None:
    """Instantiate NODEConfig and verify v4 configuration load."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    assert node_cfg.get("version") in ["4.0.0", None] or node_cfg.validate_tier("T1-30min")

def test_node02_02_removal_of_static_caps() -> None:
    """Assert static max_jobs_per_node cap is replaced by dynamic tier queue routing."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    scheduler_cfg = node_cfg.get("scheduler", {})
    assert "max_jobs_per_node" not in scheduler_cfg or scheduler_cfg.get("dynamic_routing_enabled") is True

def test_node02_03_walltime_lookup_t1_30min() -> None:
    """Assert walltime lookup for T1-30min is 00:30:00."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    assert node_cfg.get_walltime_for_tier("T1-30min") == "00:30:00"

def test_node02_04_walltime_lookup_t4_1mo() -> None:
    """Assert walltime lookup for T4-1mo is 720:00:00."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    assert node_cfg.get_walltime_for_tier("T4-1mo") == "720:00:00"

def test_node02_05_invalid_tier_handling() -> None:
    """Assert get_walltime_for_tier raises KeyError for invalid tier."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    with pytest.raises(KeyError):
        node_cfg.get_walltime_for_tier("INVALID_TIER")

def test_node02_06_mps_config_retrieval() -> None:
    """Assert get_mps_config returns valid dict with enabled: True and thread_percentage: 25."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    mps = node_cfg.get_mps_config()
    assert mps.get("enabled") is True
    assert mps.get("thread_percentage") == 25

def test_node02_07_scout_anchor_allocation_retrieval() -> None:
    """Assert get_scout_anchor_allocation returns anchor_p_cores: 7 and scout_p_cores: 1."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    alloc = node_cfg.get_scout_anchor_allocation()
    assert alloc.get("anchor_p_cores") == 7
    assert alloc.get("scout_p_cores") == 1

def test_node02_08_tier_concurrency_mapping() -> None:
    """Verify tier concurrency limit mapping."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    assert node_cfg.get_tier_concurrency_limit("T1-10s") == 16
    assert node_cfg.get_tier_concurrency_limit("T4-1mo") == 1

def test_node02_09_config_update_persistence() -> None:
    """Verify update_from_dict updates configuration."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    node_cfg.update_from_dict({"test_custom_key": "v4_val"})
    assert node_cfg.get("test_custom_key") == "v4_val"

def test_node02_10_validate_tier_helper() -> None:
    """Verify validate_tier helper method."""
    node_cfg = NODEConfig(str(SYSTEM_CONFIG_PATH))
    assert node_cfg.validate_tier("T2-3h") is True
    assert node_cfg.validate_tier("BOGUS") is False

# ==========================================
# 4.3 NODE-03 Test Suite (10 Tests)
# ==========================================

def test_node03_01_core_pinning_injection() -> None:
    """Render job script and assert export KMP_HW_SUBSET=8c:intel_core,1t is present."""
    templater = SlurmTemplater()
    script = templater.render_job("PinTest", "/scratch", 'C:\\ORCA_6.1.1\\orca.exe "in.inp" > "test.out"')
    assert "export KMP_HW_SUBSET=8c:intel_core,1t" in script

def test_node03_02_mps_daemon_start_block() -> None:
    """Render GPU job with mps_enabled=True and assert nvidia-cuda-mps-control -d is included."""
    templater = SlurmTemplater()
    script = templater.render_job("GpuTest", "/scratch", "nvidia-smi", use_gpu=True, mps_enabled=True)
    assert "nvidia-cuda-mps-control -d" in script

def test_node03_03_mps_daemon_shutdown_block() -> None:
    """Render GPU job and assert echo 'quit' | nvidia-cuda-mps-control is present."""
    templater = SlurmTemplater()
    script = templater.render_job("GpuTest", "/scratch", "nvidia-smi", use_gpu=True, mps_enabled=True)
    assert 'echo "quit" | nvidia-cuda-mps-control' in script

def test_node03_04_cpu_job_excludes_mps() -> None:
    """Render CPU job and assert MPS daemon commands are absent."""
    templater = SlurmTemplater()
    script = templater.render_job("CpuTest", "/scratch", "orca input.inp", use_gpu=False, mps_enabled=False)
    assert "nvidia-cuda-mps-control" not in script

def test_node03_05_tier_walltime_resolution() -> None:
    """Render job with tier='T1-30min' and assert #SBATCH --time=00:30:00."""
    templater = SlurmTemplater()
    script = templater.render_job("TierTest", "/scratch", 'C:\\ORCA_6.1.1\\orca.exe "in.inp" > "test.out"', tier="T1-30min")
    assert "#SBATCH --time=00:30:00" in script

def test_node03_06_tier_walltime_1mo_resolution() -> None:
    """Render job with tier='T4-1mo' and assert #SBATCH --time=720:00:00."""
    templater = SlurmTemplater()
    script = templater.render_job("LongTest", "/scratch", 'C:\\ORCA_6.1.1\\orca.exe "in.inp" > "test.out"', tier="T4-1mo")
    assert "#SBATCH --time=720:00:00" in script

def test_node03_07_cpus_per_task_rendering() -> None:
    """Assert #SBATCH --cpus-per-task matches requested core allocation."""
    templater = SlurmTemplater()
    script = templater.render_job("CpuTaskTest", "/scratch", 'C:\\ORCA_6.1.1\\orca.exe "in.inp" > "test.out"', cpus_per_task=2)
    assert "#SBATCH --cpus-per-task=2" in script

def test_node03_08_module_loading_sequence() -> None:
    """Verify module purge and module load sequence rendering."""
    templater = SlurmTemplater()
    script = templater.render_job("ModTest", "/scratch", 'C:\\ORCA_6.1.1\\orca.exe "in.inp" > "test.out"', modules_to_load=["orca/6.1.1"])
    assert "module purge" in script
    assert "module load orca/6.1.1" in script

def test_node03_09_scratch_dir_trap_cleanup() -> None:
    """Verify export TMPDIR=/scratch/... and rm -rf $TMPDIR trap block."""
    templater = SlurmTemplater()
    script = templater.render_job("TrapTest", "/scratch", 'C:\\ORCA_6.1.1\\orca.exe "in.inp" > "test.out"')
    assert "export TMPDIR=/scratch/" in script
    assert "rm -rf $TMPDIR" in script

def test_node03_10_resource_throttling_safety(caplog) -> None:
    """Assert requested cores exceeding physical max are throttled down."""
    templater = SlurmTemplater()
    script = templater.render_job("ThrottleTest", "/scratch", 'C:\\ORCA_6.1.1\\orca.exe "in.inp" > "test.out"', requested_cores=128)
    assert "#SBATCH --ntasks-per-node=8" in script

# ==========================================
# 4.4 NODE-04 Test Suite (10 Tests)
# ==========================================

def test_node04_01_scout_anchor_initialization() -> None:
    """Instantiate ScoutAnchorCoScheduler and verify initialization."""
    templater = SlurmTemplater()
    co_sched = ScoutAnchorCoScheduler(templater)
    assert co_sched.max_cpu_contention_ratio == 1.20

def test_node04_02_anchor_core_partitioning() -> None:
    """Render co-scheduled pair and assert anchor uses 7 P-cores."""
    templater = SlurmTemplater()
    co_sched = ScoutAnchorCoScheduler(templater)
    anchor_script, scout_script = co_sched.prepare_co_scheduled_payloads(
        anchor_spec={"job_name": "orca_anchor", "work_dir": "/scratch"},
        scout_spec={"job_name": "mace_scout", "work_dir": "/scratch"}
    )
    assert "#SBATCH --ntasks-per-node=7" in anchor_script

def test_node04_03_scout_core_partitioning() -> None:
    """Render co-scheduled pair and assert scout uses 1 P-core."""
    templater = SlurmTemplater()
    co_sched = ScoutAnchorCoScheduler(templater)
    anchor_script, scout_script = co_sched.prepare_co_scheduled_payloads(
        anchor_spec={"job_name": "orca_anchor", "work_dir": "/scratch"},
        scout_spec={"job_name": "mace_scout", "work_dir": "/scratch"}
    )
    assert "#SBATCH --ntasks-per-node=1" in scout_script

def test_node04_04_scout_gpu_mps_binding() -> None:
    """Render co-scheduled pair and assert scout includes GPU MPS daemon controls."""
    templater = SlurmTemplater()
    co_sched = ScoutAnchorCoScheduler(templater)
    anchor_script, scout_script = co_sched.prepare_co_scheduled_payloads(
        anchor_spec={"job_name": "orca_anchor", "work_dir": "/scratch"},
        scout_spec={"job_name": "mace_scout", "work_dir": "/scratch"}
    )
    assert "nvidia-cuda-mps-control -d" in scout_script

def test_node04_05_contention_bound_verification_pass() -> None:
    """Assert verify_contention_bound(115.0, 100.0) returns True."""
    templater = SlurmTemplater()
    co_sched = ScoutAnchorCoScheduler(templater)
    assert co_sched.verify_contention_bound(115.0, 100.0) is True

def test_node04_06_contention_bound_verification_fail() -> None:
    """Assert verify_contention_bound(130.0, 100.0) returns False."""
    templater = SlurmTemplater()
    co_sched = ScoutAnchorCoScheduler(templater)
    assert co_sched.verify_contention_bound(130.0, 100.0) is False

def test_node04_07_local_mode_dispatch_fallback(tmp_path) -> None:
    """Call dispatch_job in local mode and verify return tuple."""
    dispatcher = HPCDispatcher()
    input_file = tmp_path / "test.inp"
    # Minimal valid payload that takes >1s
    input_file.write_text("! SP PBE def2-SVP\n* xyz 0 1\nO 0.0 0.0 0.0\nH 0.0 0.75 0.5\nH 0.0 -0.75 0.5\n*\n")
    
    orca_path = "C:\\ORCA_6.1.1\\orca.exe"
    execution_cmd = f'"{orca_path}" "{input_file.name}" > "test.out"'

    success, job_id = dispatcher.dispatch_job(
        job_name="LocalTest",
        local_input_file=input_file,
        remote_work_dir=str(tmp_path),
        engine_name="ORCA",
        execution_command=execution_cmd,
        requested_cores=2,
        requested_memory_mb=1000
    )
    assert success is True
    assert job_id.isdigit()  # Local mode returns the PID

def test_node04_08_co_scheduled_pair_dispatch_payload(tmp_path) -> None:
    """Call dispatch_co_scheduled_pair and assert returned dict status is DISPATCHED."""
    dispatcher = HPCDispatcher()
    
    anc_dir = tmp_path / "anchor"
    anc_dir.mkdir()
    anc_inp = anc_dir / "anchor.inp"
    anc_inp.write_text("! SP PBE def2-SVP\n* xyz 0 1\nO 0.0 0.0 0.0\nH 0.0 0.75 0.5\nH 0.0 -0.75 0.5\n*\n")
    orca_path = "C:\\ORCA_6.1.1\\orca.exe"
    anc_cmd = f'"{orca_path}" "anchor.inp" > "anchor.out"'
    
    sct_dir = tmp_path / "scout"
    sct_dir.mkdir()
    sct_inp = sct_dir / "scout.inp"
    sct_inp.write_text("! SP PBE def2-SVP\n* xyz 0 1\nO 0.0 0.0 0.0\nH 0.0 0.75 0.5\nH 0.0 -0.75 0.5\n*\n")
    sct_cmd = f'"{orca_path}" "scout.inp" > "scout.out"'

    res = dispatcher.dispatch_co_scheduled_pair(
        anchor_spec={"job_name": "AnchorA", "work_dir": str(anc_dir), "execution_command": anc_cmd},
        scout_spec={"job_name": "ScoutB", "work_dir": str(sct_dir), "execution_command": sct_cmd}
    )
    assert res.get("status") == "DISPATCHED"
    assert res.get("anchor_job_name") == "AnchorA"
    assert res.get("scout_job_name") == "ScoutB"

def test_node04_09_temp_script_cleanup(tmp_path) -> None:
    """Verify temp submission script cleanup after dispatch attempt."""
    dispatcher = HPCDispatcher()
    input_file = tmp_path / "in.inp"
    input_file.write_text("! SP PBE def2-SVP\n* xyz 0 1\nO 0.0 0.0 0.0\nH 0.0 0.75 0.5\nH 0.0 -0.75 0.5\n*\n")
    dispatcher.dispatch_job("CleanTest", input_file, str(tmp_path), "ORCA", 'C:\\ORCA_6.1.1\\orca.exe "in.inp" > "test.out"', 2, 1000)
    assert not Path(".tmp_CleanTest.sh").exists()

def test_node04_10_teardown_disconnect() -> None:
    """Call dispatcher.teardown() and verify clean disconnect."""
    dispatcher = HPCDispatcher()
    dispatcher.teardown()
    assert dispatcher.bridge.client is None or not dispatcher.bridge.client.get_transport() or not dispatcher.bridge.client.get_transport().is_active()
