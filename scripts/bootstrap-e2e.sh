#!/usr/bin/env bash
set -euo pipefail

# Bootstraps MinIO + two k3d clusters for the e2e test suite.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="${ROOT_DIR}/.tmp"
KUBECONFIG_MERGED="${TMP_DIR}/kubeconfig"
CONFIG_PATH="${TMP_DIR}/e2e-config.yaml"
NETWORK_NAME="pvc-transfer-net"
SRC_CONFIG="${ROOT_DIR}/e2e/k3d/pvc-source/k3d.yaml"
DST_CONFIG="${ROOT_DIR}/e2e/k3d/pvc-dest/k3d.yaml"

mkdir -p "${TMP_DIR}"

command -v k3d >/dev/null 2>&1 || { echo "k3d is required"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required"; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "docker is required"; exit 1; }

docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1 || docker network create "${NETWORK_NAME}"
NETWORK_GATEWAY="$(docker network inspect "${NETWORK_NAME}" -f '{{(index .IPAM.Config 0).Gateway}}')"

echo "Starting MinIO via docker-compose..."
docker compose -f "${ROOT_DIR}/docker-compose.e2e.yaml" up -d minio mc

echo "Waiting for MinIO to be ready..."
until curl -sSf http://localhost:9000/minio/health/live >/dev/null; do
  sleep 2
done

create_cluster() {
  local name=$1
  local config=$2
  if k3d cluster list | grep -q "${name}"; then
    echo "k3d cluster ${name} already exists"
  else
    pushd "${ROOT_DIR}" >/dev/null
    k3d cluster create --config "${config}"
    popd >/dev/null
  fi
  k3d kubeconfig get "${name}" >"${TMP_DIR}/${name}.kubeconfig"
}

create_cluster "pvc-source" "${SRC_CONFIG}"
create_cluster "pvc-dest" "${DST_CONFIG}"

echo "Merging kubeconfigs..."
KUBECONFIG="${TMP_DIR}/pvc-source.kubeconfig:${TMP_DIR}/pvc-dest.kubeconfig" kubectl config view --flatten >"${KUBECONFIG_MERGED}"
export KUBECONFIG="${KUBECONFIG_MERGED}"

SRC_CTX="k3d-pvc-source"
DST_CTX="k3d-pvc-dest"
SRC_NS="pvc-source-ns"
DST_NS="pvc-dest-ns"
SA_NAME="pvc-transfer-sa"

wait_for_namespace() {
  local ctx=$1
  local ns=$2
  for _ in $(seq 1 30); do
    if kubectl --context "${ctx}" get namespace "${ns}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for namespace ${ns} in context ${ctx}"
  exit 1
}

wait_for_job_complete() {
  local ctx=$1
  local ns=$2
  local name=$3
  for _ in $(seq 1 30); do
    if kubectl --context "${ctx}" -n "${ns}" get job "${name}" >/dev/null 2>&1; then
      kubectl --context "${ctx}" -n "${ns}" wait --for=condition=complete "job/${name}" --timeout=120s
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for job ${name} in ${ns}"
  exit 1
}

echo "Waiting for manifests to apply..."
wait_for_namespace "${SRC_CTX}" "${SRC_NS}"
wait_for_namespace "${DST_CTX}" "${DST_NS}"

echo "Waiting for source PVC seed job to complete..."
wait_for_job_complete "${SRC_CTX}" "${SRC_NS}" "pvc-seed"

cat <<EOF >"${CONFIG_PATH}"
version: "v1"
s3:
  bucket: "migration-bridge-bucket"
  region: "us-east-1"
  endpoint: "http://localhost:9000"
  jobEndpoint: "http://${NETWORK_GATEWAY}:9000"
  accessKey: "minioadmin"
  secretKey: "minioadmin"
  objectKey: "migrations/pvc-data-archive.tar.gz"
source:
  clusterContext: "${SRC_CTX}"
  namespace: "${SRC_NS}"
  pvcName: "data-pvc"
  mountPath: "/data"
destination:
  clusterContext: "${DST_CTX}"
  namespace: "${DST_NS}"
  pvcName: "data-pvc-new"
  mountPath: "/data"
job:
  image: "alpine:3.19"
  serviceAccount: "${SA_NAME}"
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  verifyMd5: true
cleanup: true
overwrite: true
EOF

echo "E2E config written to ${CONFIG_PATH}"
echo "export E2E_CONFIG=${CONFIG_PATH}"
