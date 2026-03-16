# pvc-transfer

Golang utility that moves data between Kubernetes PVCs across clusters using an intermediate S3 bucket. Jobs stream data with `tar | mbuffer | aws s3 cp` and can optionally verify MD5 integrity after restore.

## Installation

Grab the latest release binary for your platform:

```bash
OS=$(uname | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$ARCH" in
  x86_64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "unsupported arch: $ARCH" && exit 1 ;;
esac
BIN="pvc-transfer-${OS}-${ARCH}"
curl -L "https://github.com/zeppelinen/pvc-transfer/releases/latest/download/${BIN}" -o pvc-transfer
chmod +x pvc-transfer
sudo mv pvc-transfer /usr/local/bin/
echo "'pvc-transfer' installed"
```

Available artifacts: `pvc-transfer-linux-amd64`, `pvc-transfer-linux-arm64`, `pvc-transfer-darwin-arm64`.

## Configuration

Create a YAML file matching `REQUIREMENTS.md`, e.g.:

```yaml
version: "v1"
s3:
  bucket: "migration-bridge-bucket"
  region: "us-east-1"
  endpoint: "http://localhost:9000"
  jobEndpoint: "http://minio:9000"
  accessKey: "minioadmin"
  secretKey: "minioadmin"
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
  image: "alpine:3.19"
  serviceAccount: "pvc-transfer-sa"
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  verifyMd5: true
  keepIntermediateObject: false
cleanup: true
overwrite: false
timeoutMinutes: 60
retryBackoff:
  attempts: 3
  seconds: 5
```

Environment overrides: `PVC_TRANSFER_S3_ACCESS_KEY`, `PVC_TRANSFER_S3_SECRET_KEY`, `PVC_TRANSFER_S3_ENDPOINT`. Use `s3.jobEndpoint` when the in-cluster URL differs from the local endpoint (e.g., MinIO on a Docker network).

## Usage

Run with default settings:
```bash
go run ./cmd/pvc-transfer --config ./config.yaml
```

Override key settings from the command line:

```bash
go run ./cmd/pvc-transfer \
  --config ./config.yaml \
  --s3-object-key "migrations/custom-archive.tar.gz" \
  --source-pvc "data-pvc" \
  --dest-pvc "data-pvc-new" \
  --source-namespace "production" \
  --dest-namespace "staging" \
  --overwrite \
  --no-cleanup
```

Split the workflow into two phases when needed:
- `--export-only` runs just the export to S3 and leaves the object intact for later use.
- `--import-only` restores from an existing S3 object without re-exporting.
These flags are mutually exclusive.

Requires kubeconfig contexts for both clusters. Jobs use the `serviceAccount` configured above; apply the manifests in `rbac/` (adjust namespace as needed) to grant permissions to create Jobs/Pods and stream logs.

## Tests & Tooling

- Unit tests: `go test ./...`
- Local MinIO: `docker compose -f docker-compose.dev.yaml up -d`
- Bootstrap k3d + MinIO + PVCs for e2e: `./scripts/bootstrap-e2e.sh`
- E2E tests (after bootstrap): `E2E_CONFIG=.tmp/e2e-config.yaml go test -tags e2e ./e2e -v`

GitHub workflows:
- `.github/workflows/ci.yml` for unit tests.
- `.github/workflows/e2e.yml` provisions k3d/MinIO and runs e2e tagged tests.
