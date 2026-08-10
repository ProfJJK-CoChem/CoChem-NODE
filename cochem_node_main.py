# cochem_canvas_target: cochem_node_main.py
"""
Main orchestrator module for CoChem-NODE.
Central entry point connecting dispatcher, watchdog, retriever, and healer (NODE-20).
"""

import os
import sys
import json
from pathlib import Path

try:
    from cochem_node_connection_bridge import NodeBridge, CoChemHPCError
    from cochem_hpc_dispatch import HPCDispatcher
    from cochem_hpc_poll import SlurmWatchdog
    from cochem_hpc_sync import ArtifactRetriever
    from cochem_hpc_heal import RegistryHealer
    from cochem_registry_manager import RegistryManager
except ImportError:
    from Libraries.cochem_node_connection_bridge import NodeBridge, CoChemHPCError
    from Libraries.cochem_hpc_dispatch import HPCDispatcher
    from Libraries.cochem_hpc_poll import SlurmWatchdog
    from Libraries.cochem_hpc_sync import ArtifactRetriever
    from Libraries.cochem_hpc_heal import RegistryHealer
    from Libraries.cochem_registry_manager import RegistryManager

class NODEOrchestrator:
    """
    The main orchestrator that coordinates all NODE activities (NODE-20).
    """
    
    def __init__(self, config_file: str = "cochem_system_config.json"):
        """Initialize the NODE orchestrator."""
        self.config_file = config_file
        self.registry_manager = RegistryManager(config_file)
        self.config = self._load_config()
        self.is_initialized = False
        self.dispatcher = None
        self.watchdog = None
        self.retriever = None
        self.healer = None
        
    def _load_config(self) -> dict:
        """Load configuration from JSON file."""
        try:
            cfg = self.registry_manager.get_config()
            return cfg.model_dump()
        except Exception:
            return {
                "project_name": "CoChem-NODE",
                "version": "0.1.0",
                "data_dir": "./cochem_node_data"
            }
            
    def initialize(self):
        """Initialize the NODE system and core services."""
        print("🚀 Initializing CoChem-NODE System...")
        
        # Create data directories
        data_dir = Path(self.config.get('data_dir', './cochem_node_data'))
        data_dir.mkdir(parents=True, exist_ok=True)
        
        (data_dir / "jobs").mkdir(parents=True, exist_ok=True)
        (data_dir / "registry").mkdir(parents=True, exist_ok=True)
        (data_dir / "logs").mkdir(parents=True, exist_ok=True)
        (data_dir / "templates").mkdir(parents=True, exist_ok=True)
        
        # Initialize core sub-services (NODE-20)
        try:
            bridge = NodeBridge(self.registry_manager.get_config())
            self.dispatcher = HPCDispatcher(bridge=bridge)
            self.watchdog = SlurmWatchdog(bridge=bridge)
            self.retriever = ArtifactRetriever(bridge=bridge)
            self.healer = RegistryHealer(bridge=bridge, registry_manager=self.registry_manager)
        except Exception as e:
            print(f"⚠️ Service initialization warning (running in localized mode): {e}")

        self.is_initialized = True
        print("✅ CoChem-NODE initialized successfully")
        
    def run_node_management(self, action: str, params: dict) -> dict:
        """Run a specific node management action connected to live engines (NODE-20)."""
        if not self.is_initialized:
            raise RuntimeError("NODE system must be initialized before running actions")
            
        print(f"⚙️ Running {action} on node...")
        results = {"action": action, "status": "COMPLETED"}
        
        action_lower = action.lower()
        if action_lower == "dispatch" and self.dispatcher:
            job_name = params.get("job_name", "job_default")
            input_file = Path(params.get("input_file", "input.inp"))
            work_dir = params.get("work_dir", "/scratch/default")
            engine = params.get("engine", "MPQC")
            cmd = params.get("cmd", "mpqc input.inp > output.out")
            cores = params.get("cores", 4)
            mem = params.get("mem", 4000)
            success, job_id_or_err = self.dispatcher.dispatch_job(
                job_name=job_name, local_input_file=input_file, remote_work_dir=work_dir,
                engine_name=engine, execution_command=cmd, requested_cores=cores, requested_memory_mb=mem
            )
            results["success"] = success
            results["job_id_or_err"] = job_id_or_err
            
        elif action_lower == "poll" and self.watchdog:
            job_id = params.get("job_id", "12345")
            status = self.watchdog.check_job_status(job_id)
            results["job_status"] = status
            
        elif action_lower == "retrieve" and self.retriever:
            remote_dir = params.get("remote_dir", "/scratch/default")
            local_dir = Path(params.get("local_dir", "./cochem_node_data/outputs"))
            success, files = self.retriever.retrieve_artifacts(remote_dir, local_dir)
            results["success"] = success
            results["files"] = [str(f) for f in files]
            
        elif action_lower == "heal" and self.healer:
            local_jobs = params.get("local_jobs", {})
            healed = self.healer.heal_registry(local_jobs)
            results["healed_jobs"] = healed
        else:
            results["info"] = f"Executed localized node action: {action}"
            
        print(f"✅ {action} completed")
        return results
        
    def generate_node_report(self, output_dir: str = "./reports") -> str:
        """Generate comprehensive report of node operations."""
        print(f"📄 Generating NODE report in {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        report_path = Path(output_dir) / "cochem_node_report.json"
        
        report_data = {
            "project": "CoChem-NODE",
            "status": "OPERATIONAL" if self.is_initialized else "UNINITIALIZED",
            "services": {
                "dispatcher": self.dispatcher is not None,
                "watchdog": self.watchdog is not None,
                "retriever": self.retriever is not None,
                "healer": self.healer is not None
            }
        }
        
        with open(report_path, "w") as f:
            json.dump(report_data, f, indent=4)
            
        print(f"✅ NODE report generated at {report_path}")
        return str(report_path)

def main():
    """Main entry point for CoChem-NODE."""
    print("Starting CoChem-NODE Orchestrator")
    
    orchestrator = NODEOrchestrator()
    orchestrator.initialize()
    
    # Example usage
    orchestrator.run_node_management("heal", {"local_jobs": {"101": "RUNNING"}})
    orchestrator.generate_node_report("./reports")
    
if __name__ == "__main__":
    main()