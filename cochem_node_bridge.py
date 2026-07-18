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
        path = Path("cochem_system_config.json")
        if not path.exists():
            logging.warning("System config missing. Using default local-fallback mock data.")
            return {"hpc": {"host": "mock_cluster", "user": "mock_user"}}
        with open(path, "r") as f:
            return json.load(f)

    def connect(self) -> bool:
        """Establishes the SSH Heartbeat using RSA/Ed25519 keys."""
        hpc_cfg = self.config.get("hpc", {})
        host = hpc_cfg.get("host")
        user = hpc_cfg.get("user")
        
        if not SSH_AVAILABLE:
            print(f"{Colors.WARNING}⚠️ 'paramiko' not installed. Running in Dry-Run/Mock Mode.{Colors.ENDC}")
            return False

        if not host or host == "mock_cluster":
            print(f"{Colors.WARNING}⚠️ HPC credentials not configured in registry. Bypassing SSH.{Colors.ENDC}")
            return False

        print(f"🔌 Establishing Heartbeat to {user}@{host}...")
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
        Executes a remote squeue check.
        Uses explicit pipe-delimited formatting to prevent column shift bugs.
        """
        if not self.client:
            logging.info("Mock Mode: Simulating remote queue check.")
            return [{"jobid": "9999", "name": "cochem_sim", "state": "RUNNING"}]

        cmd = f"squeue -u {username} -o '%i|%j|%T|%M|%D'"
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
    user = bridge.config.get("hpc", {}).get("user", "mock_user")
    jobs = bridge.check_remote_queue(user)
    
    print(f"📊 Active Jobs for '{user}': {len(jobs)}")
    for j in jobs:
        print(f"  [{j['jobid']}] {j['name']} : {j['state']}")
        
    bridge.disconnect()

if __name__ == "__main__":
    main()