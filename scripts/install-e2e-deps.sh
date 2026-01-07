#!/usr/bin/env bash
set -euo pipefail

# Installs k3d and kubectl for running e2e tests locally.

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required; please install Docker Desktop or Docker Engine before running e2e tests."
  exit 1
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "Installing kubectl..."
  curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/$(uname | tr '[:upper:]' '[:lower:]')/amd64/kubectl"
  chmod +x kubectl
  sudo mv kubectl /usr/local/bin/kubectl
fi

if ! command -v k3d >/dev/null 2>&1; then
  echo "Installing k3d..."
  curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
fi

echo "e2e dependencies installed (docker, kubectl, k3d)."
