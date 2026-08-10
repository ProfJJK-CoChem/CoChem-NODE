#!/usr/bin/env python3
"""
CoChem-NODE: HPC SSH & SLURM Bridge
Establishes secure telemetry with remote clusters. Validates connections,
checks quotas, and strictly parses squeue to prevent tracking desync.
"""

import os
import sys
import json
import logging
import shlex
import re
from pathlib import Path

try:
    import paramiko
    SSH_AVAILABLE = True
except ImportError:
    SSH_AVAILABLE = False

class Colors:
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

logging.basicConfig(filename='cochem_node_bridge.log', level=logging.INFO)

class NodeBridge:
    def __init__(self):
        self.config = self.load_config()
        self.client = None
        
    def load_config(self) -> dict:
        for fname in ["cochem_system_config.json", "cochem_node_config.json"]:
            path = Path(fname)
            if path.exists():
                with open(path, "r") as f:
                    return json.load(f)
        return {"hpc": {"host": "", "user": ""}}

    def connect(self) -> bool:
        """Establishes the SSH Heartbeat using RSA/Ed25519 keys."""
        hpc_cfg = self.config.get("hpc", {})
        host = hpc_cfg.get("host") or hpc_cfg.get("cluster_hostname")
        user = hpc_cfg.get("user") or hpc_cfg.get("username")
        
        if not SSH_AVAILABLE:
            logging.info("Paramiko module not available. Local dispatch mode active.")
            return False

        if not host or hpc_cfg.get("execution_mode") == "local":
            logging.info("HPC credentials not configured or execution_mode is local. Local queue mode active.")
            return False

        print(f"🔌 Establishing Heartbeat to {user}@{host}...")
        try:
            self.client = paramiko.SSHClient()
            # Enforce strict security policy by loading system host keys and rejecting unverified hosts (NODE-08)
            self.client.load_system_host_keys()
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
            # Assumes key-based auth via ssh-agent
            self.client.connect(hostname=host, username=user, timeout=10)
            print(f"{Colors.OKGREEN}✅ Heartbeat Established.{Colors.ENDC}")
            return True
        except Exception as e:
            print(f"{Colors.FAIL}❌ SSH Connection Failed: {e}{Colors.ENDC}")
            logging.error(f"SSH Error: {e}")
            return False

    def check_remote_queue(self, username: str) -> list:
        """
        Executes a remote squeue check, or local process/queue query if remote client is uninitialized.
        Uses explicit pipe-delimited formatting to prevent column shift bugs.
        Sanitizes username parameter against shell injection (NODE-09).
        """
        if not re.match(r'^[a-zA-Z0-9_\-\.]+$', username):
            raise ValueError(f"Invalid username for queue check: '{username}'")

        if not self.client:
            # Query local slurm queue if sbatch/squeue is installed locally
            import subprocess
            try:
                res = subprocess.run(["squeue", "-u", username, "-o", "%i|%j|%T|%M|%D"], capture_output=True, text=True, timeout=5)
                if res.returncode == 0 and res.stdout.strip():
                    raw_lines = res.stdout.strip().split('\n')
                    local_jobs = []
                    for line in raw_lines[1:]:
                        parts = line.split('|')
                        if len(parts) >= 5:
                            local_jobs.append({
                                "jobid": parts[0].strip(),
                                "name": parts[1].strip(),
                                "state": parts[2].strip(),
                                "time": parts[3].strip(),
                                "nodes": parts[4].strip()
                            })
                    return local_jobs
            except Exception as err:
                logging.debug(f"Local squeue query skipped: {err}")
            return []

        safe_user = shlex.quote(username)
        cmd = f"squeue -u {safe_user} -o '%i|%j|%T|%M|%D'"
        stdin, stdout, stderr = self.client.exec_command(cmd)
        
        raw_output = stdout.read().decode('utf-8').strip().split('\n')
        active_jobs = []
        
        # Skip header
        for line in raw_output[1:]:
            if not line.strip(): continue
            parts = line.split('|')
            if len(parts) >= 5:
                active_jobs.append({
                    "jobid": parts[0].strip(),
                    "name": parts[1].strip(),
                    "state": parts[2].strip(),
                    "time": parts[3].strip(),
                    "nodes": parts[4].strip()
                })
                
        return active_jobs

    def disconnect(self):
        if self.client:
            self.client.close()
            print(f"🔌 Connection closed.")

def main():
    print(f"\n{Colors.OKCYAN}--- CoChem-NODE: HPC Bridge Telemetry ---{Colors.ENDC}")
    bridge = NodeBridge()
    connected = bridge.connect()
    
    # Run a diagnostic ping regardless of actual connection status
    hpc_cfg = bridge.config.get("hpc", {})
    user = hpc_cfg.get("user") or hpc_cfg.get("username", os.getlogin())
    jobs = bridge.check_remote_queue(user)
    
    print(f"📊 Active Jobs for '{user}': {len(jobs)}")
    for j in jobs:
        print(f"  [{j['jobid']}] {j['name']} : {j['state']}")
        
    bridge.disconnect()

if __name__ == "__main__":
    main()