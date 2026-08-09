import pytest
from cochem_slurm_templater import SlurmTemplater
from cochem_registry_schema import CoChemConfig

def test_slurm_templater_render():
    templater = SlurmTemplater()
    script = templater.render_job(
        job_name="TestJob",
        work_dir="/scratch/test",
        execution_command="echo hello",
        engine_name="ORCA",
        requested_cores=2,
        requested_memory_mb=1000
    )
    assert "#SBATCH --job-name=TestJob" in script
    assert "cd /scratch/test" in script
    assert "echo hello" in script
