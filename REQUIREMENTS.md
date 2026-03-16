# **pvc-transfer** 

# 

Technical and Functional Requirements: for “pvc-transfer” Kubernetes PVC Transfer Utility

## **1\. Overview**

The goal is to develop a Go-based utility that orchestrates the migration of data from a Persistent Volume Claim (PVC) in a source Kubernetes cluster to a PVC in a destination cluster using an intermediate S3 bucket as a staging area.

### **1.1 High-Level Workflow**

1. **Source Phase**: A Kubernetes Job is spawned in the source cluster. It mounts the source PVC, archives the data using tar, and streams/uploads it to a specified S3 bucket.  
2. **Destination Phase**: A Kubernetes Job is spawned in the destination cluster. It mounts the destination PVC, downloads the archive from S3, and extracts it using tar.

## **2\. Functional Requirements**

### **2.1 Configuration Management**

* The utility must accept a YAML configuration file.  
* Support for S3 access keys, secret keys, and custom API endpoints directly in the config.  
* Support for environment variable overrides for sensitive data (S3 Credentials).  
* Validation of PVC names, namespaces, and S3 bucket connectivity before starting the Job.

### **2.2 Orchestration Logic (Golang)**

* **Context Management**: Handle timeouts and cancellations gracefully.  
* **Job Lifecycle**: The utility must create the Job, monitor its status (Pending \-\> Running \-\> Succeeded/Failed), and stream logs to the console.  
* **Cleanup**: Option to delete the Kubernetes Job and S3 intermediate objects upon successful completion or failure.  
* **Integrity Check**: Support for an optional MD5-based integrity check after transfer.

### **2.3 Data Transfer Specs**

* **Compression**: Use gzip via tar to reduce S3 storage costs and transfer time.  
* **Buffering**: Integrate mbuffer between tar and the aws-cli to smooth out I/O spikes and improve transfer reliability.  
* **Streaming**: Use pipes to avoid using local disk space within the Job container for temporary storage.  
* **Tooling**: Use aws-cli, tar, gzip, and mbuffer inside the Alpine container.

## **3\. Technical Requirements**

### **3.1 YAML Configuration Schema**

The utility expects a configuration file in the following format:

version: "v1"  
s3:  
  bucket: "migration-bridge-bucket"  
  region: "us-east-1"  
  endpoint: "\[https://s3.amazonaws.com\](https://s3.amazonaws.com)"  
  accessKey: "AKIA..."  
  secretKey: "SECRET..."  
  objectKey: "migrations/pvc-data-archive.tar.gz"

source:  
  clusterContext: "source-ctx"  
  namespace: "production"  
  pvcs:
    - name: "data-pvc"  
      mountPath: "/data"

destination:  
  clusterContext: "dest-ctx"  
  namespace: "staging"  
  pvcs:
    - name: "data-pvc-new"  
      mountPath: "/data"

job:  
  image: "alpine:latest"  
  backoffLimit: 0  
  ttlSecondsAfterFinished: 3600  
  verifyMd5: false \# Optional integrity check

### **3.2 Kubernetes Job Specifications**

#### **Source Job (Export)**

* **Command**:  
  apk add \--no-cache aws-cli tar gzip mbuffer  
  tar \-cvzf \- \-C /data . | mbuffer \-m 128M | aws s3 cp \- s3://\<bucket\>/\<key\>

* **Volume**: Mount source.pvcName to /data in readOnly mode.

#### **Destination Job (Import)**

* **Command**:  
  apk add \--no-cache aws-cli tar gzip mbuffer  
  aws s3 cp s3://\<bucket\>/\<key\> \- | mbuffer \-m 128M | tar \-xvzf \- \-C /data

* **Volume**: Mount destination.pvcName to /data in readWrite mode.

### **3.3 Security & RBAC**

* **S3 Credentials**: Passed into the Job via Kubernetes Secrets created dynamically by the Go utility or mapped from the config.  
* **RBAC Definitions**: The project must include a /rbac directory containing YAML manifests for:  
  * ClusterRole: Permissions to create, get, list, watch, and delete Jobs, Pods, and Pod logs.  
  * ServiceAccount: The identity used by the Go utility or the Jobs.  
  * ClusterRoleBinding: To associate the account with the required permissions.

## **4\. Implementation Details (Golang)**

### **4.1 Libraries**

* k8s.io/client-go: For interacting with Kubernetes APIs.  
* gopkg.in/yaml.v3: For parsing the configuration file.  
* github.com/aws/aws-sdk-go-v2: To verify S3 connectivity/permissions before launching Jobs.

### **4.2 Error Handling & Resiliency**

* **Retry Mechanism**: The utility should implement a retry loop for transient S3 errors.  
* **Log Streaming**: Use the GetLogs method in the K8s API to provide real-time feedback. The \-v flag in tar will ensure every file name is visible in the logs.  
* **State Tracking**: The utility should check if an archive already exists in S3 and prompt for overwrite.

## **5\. Success Criteria**

1. **Integrity**: Files in the destination PVC match the source (verified via optional MD5 check).  
2. **Performance**: Improved throughput via mbuffer and reduced network overhead via gzip.  
3. **Observability**: Clear, verbose logs of file-by-file progress.

## **6\. E2E Tests**

The utility must include an automated end-to-end testing suite to validate the full migration path across simulated clusters.

### **6.1 Test Environment Architecture**

The E2E suite will use docker-compose to orchestrate the following components:

* **Cluster A (Source)**: A k3d (K3s in Docker) cluster.  
* **Cluster B (Destination)**: A second k3d cluster.  
* **S3 Storage**: A MinIO container configured with a default bucket.  
* **Test Runner**: A containerized version of the Go utility or a script that orchestrates the test.

### **6.2 Test Scenario Workflow**

1. **Setup**:  
   * Spin up MinIO and create the migration bucket.  
   * Spin up both k3d clusters and merge their kubeconfig into the test runner's environment.  
   * Create a source PVC in Cluster A and populate it with a known set of files/directories.  
   * Create an empty destination PVC in Cluster B.  
2. **Execution**:  
   * Run the PVC Transfer Utility with a generated YAML config pointing to the local MinIO endpoint and the two k3d clusters.  
3. **Verification**:  
   * Compare the file tree and file contents of Cluster B's PVC against Cluster A's PVC.  
   * Verify that the intermediate S3 object was created (and optionally deleted if cleanup is enabled).  
4. **Teardown**:  
   * Delete both k3d clusters and the MinIO container.

## **7\. Tooling & Environment Bootstrapping**

To facilitate development and E2E testing, the project must provide automated scripts to prepare the environment.

### **7.1 Development Prerequisites**

* **Go SDK**: Version 1.21 or higher.  
* **Docker**: Required for building images and running the E2E suite.  
* **k3d**: For local Kubernetes cluster management.  
* **kubectl**: For manual verification and cluster interaction.  
* **Task**: A task runner (go-task/task) to manage commands.

### **7.2 Bootstrapping E2E Infrastructure**

The utility should include a scripts/bootstrap-e2e.sh or a Taskfile.yml target that:

1. **Creates k3d Clusters**: Provisions pvc-source and pvc-dest clusters.  
2. **Kubeconfig Merging**: Merges context into a temporary file or specific named contexts to allow the Go utility to switch between them.  
3. **MinIO Initialization**: Starts MinIO via Docker, waits for readiness, and uses the mc (MinIO Client) to create the required test buckets.  
4. **Data Generation**: Populates the source PVC in the source cluster with a randomized but deterministic set of files (e.g., using dd for size tests and touch for directory depth tests).

### **7.3 Tooling for Local Development**

* **Mock S3**: Provide a docker-compose.dev.yaml to run MinIO locally for testing the Go logic without full Kubernetes jobs.  
* **Dependency Management**: Standard go mod tidy and go mod vendor patterns.  
* **Linting**: Integration with golangci-lint for code quality checks.