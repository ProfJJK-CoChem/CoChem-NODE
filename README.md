# **CoChem-NODE: HPC Bridge & Telemetry Manager**

## **Overview**

**CoChem-NODE** is the bridge between your local Jupyter/Codespace environment and remote High-Performance Computing (HPC) clusters. Running 10,000 ORCA calculations locally is impossible; NODE solves this by dynamically translating local Python pipeline states into thousands of physical .sbatch scripts, dispatching them via SSH, and tracking their UUIDs.

Crucially, NODE features the **Registry Reconciliation Engine**. If your local laptop dies or the Jupyter kernel crashes while 500 jobs are running on the cluster, NODE's cochem\_hpc\_heal.py script will poll the SLURM queue upon restart, detect the "orphaned" cluster jobs, and dynamically re-sync them back into the local landscape.h5 database without dropping a single data point.

## **Scientific & Technical Trade-offs**

* **Polling Latency vs. Cluster Bans:** Constantly polling an HPC head node (e.g., squeue every 5 seconds) will likely get your IP banned by system administrators. NODE trades immediate real-time updates for administrative safety, utilizing an exponential backoff polling mechanism (defaulting to 60-second intervals for large arrays).  
* **Asynchronous SCP Handoffs:** Retrieving massive .gbw (wavefunction) files over SSH can stall the pipeline. NODE only syncs the minimal required .out and .h5 property arrays during active execution, deferring heavy wavefunction transfers to a background cleanup thread.

## **Installation & Setup**

NODE requires SSH keys to be pre-configured for passwordless entry to your cluster.

git clone \[https://github.com/CoChem/CoChem-NODE.git\](https://github.com/CoChem/CoChem-NODE.git)  
cd CoChem-NODE

## **How to Run**

1. **Configure the HPC Bridge:**  
   Update your cochem\_system\_config.json with your cluster alias and partition parameters via the UI or by running:  
   python cochem\_node\_setup.py  
2. **Launch the Job Batcher:**  
   python cochem\_job\_batcher.py \--target landscape.h5  
   *(Generates optimized .sbatch arrays based on the molecular load).*  
3. **Execute the Fault-Tolerance Healer (If a crash occurs):**  
   python cochem\_hpc\_heal.py  
   *(Scans SLURM for orphaned jobs and re-binds them to the local registry).*