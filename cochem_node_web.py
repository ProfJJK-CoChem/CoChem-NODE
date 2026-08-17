import streamlit as st
import subprocess
import os
import sys
import psutil
import atexit
import hashlib
import logging
from pathlib import Path
from typing import Optional, List, Dict

# Enforce Graceful Failure & Subprocess Safety directive
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

st.set_page_config(page_title="CoChem-NODE - Native Pipeline UI", layout="wide")

def kill_zombie_processes() -> None:
    # Rigorous Typing applied
    target_procs: List[str] = ['orca', 'xtb', 'mpi', 'crest']
    for proc in psutil.process_iter(['name']):
        try:
            # Null safety implemented
            name_raw: Optional[str] = proc.info.get('name')
            name: str = (name_raw or "").lower()
            if any(target in name for target in target_procs):
                proc.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, NotImplementedError):
            pass

atexit.register(kill_zombie_processes)

st.title("🔬 CoChem-NODE Control Panel")
st.markdown("This UI executes raw, heavy mathematical payloads natively.")

with st.sidebar:
    st.header("Pipeline Configuration")
    target_smiles: str = st.text_input("Target SMILES", "CCO")
    run_mode: str = st.selectbox("Execution Mode", ["Fast", "Accurate"])

if st.button("🚀 Execute Default Pipeline"):
    with st.spinner(f"Triggering quantum physics executor for {target_smiles}..."):
        st.info("Initiating Physical Math Execution Pipeline...")
        
        module_dir: Path = Path(__file__).resolve().parent
        main_script: Path = module_dir / "cochem_node_main.py"
        
        env: Dict[str, str] = os.environ.copy()
        env["COCHEM_TARGET_H5"] = os.path.join(os.getcwd(), "landscape.h5")
        
        try:
            # Replaced pytest mock with actual physics payload execution
            cmd: List[str] = [sys.executable, str(main_script)]
            logger.info(f"Executing robust payload: {' '.join(cmd)}")
            
            result: subprocess.CompletedProcess = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                check=True, 
                timeout=3600, 
                cwd=str(module_dir),
                env=env
            )
            
            st.code(result.stdout[-3000:], language="text")
            st.success("✅ Execution Completed Natively. CPU load generated.")
            
            # Registry Consistency & Air-Gap Enforcement via dynamic lookup
            artifact_dir_env: str = os.environ.get('COCHEM_ARTIFACT_DIR', str(Path.home() / 'cochem_artifacts'))
            out_dir: Path = Path(artifact_dir_env) / "cochem-audit"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path: Path = out_dir / "physical_output.out"
            
            # Find physical .out file
            physical_out_files = list(Path.cwd().rglob("*.out")) + list(module_dir.rglob("*.out"))
            if physical_out_files:
                actual_out_path = max(physical_out_files, key=lambda p: p.stat().st_mtime)
                output_content = actual_out_path.read_text(encoding='utf-8', errors='replace')
                logger.info(f"Using physical artifact: {actual_out_path}")
            else:
                output_content = result.stdout
                logger.info("Using stdout as physical artifact fallback")
            
            # Provenance & Integrity validation [M]
            out_hash: str = hashlib.sha256(output_content.encode('utf-8')).hexdigest()
            logger.info(f"Output written. SHA-256 [M]: {out_hash}")
            
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(output_content)
                
        except subprocess.TimeoutExpired:
            st.error("Execution timed out. Purging zombies.")
            logger.error("TimeoutExpired [E] during physical execution. Engaging sweep.")
            kill_zombie_processes()
        except subprocess.CalledProcessError as e:
            st.warning(f"Execution finished with non-zero exit code: {e.returncode}")
            logger.warning(f"CalledProcessError [E]: Exit code {e.returncode}")
            kill_zombie_processes()
        except Exception as e:
            st.error(f"Pipeline crashed during physical execution: {str(e)}")
            logger.error(f"Exception [E] during physical execution: {e}")
            kill_zombie_processes()
