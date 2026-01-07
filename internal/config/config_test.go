package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesEnvOverrides(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "config.yaml")
	if err := os.WriteFile(path, []byte(`
version: "v1"
s3:
  bucket: "b"
  region: "us-east-1"
  endpoint: "https://s3.example.com"
  accessKey: "cfgKey"
  secretKey: "cfgSecret"
  objectKey: "obj"
source:
  clusterContext: "c1"
  namespace: "default"
  pvcName: "pvc1"
  mountPath: "/data"
destination:
  clusterContext: "c2"
  namespace: "default"
  pvcName: "pvc2"
  mountPath: "/data"
job:
  image: "alpine:latest"
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	os.Setenv("PVC_TRANSFER_S3_ACCESS_KEY", "envKey")
	os.Setenv("PVC_TRANSFER_S3_SECRET_KEY", "envSecret")
	defer os.Clearenv()

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if cfg.S3.AccessKey != "envKey" || cfg.S3.SecretKey != "envSecret" {
		t.Fatalf("env overrides not applied: %+v", cfg.S3)
	}
}

func TestLoadSetsDefaults(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "config.yaml")
	if err := os.WriteFile(path, []byte(`
version: "v1"
s3:
  bucket: "b"
  region: "us-east-1"
source:
  clusterContext: "c1"
  namespace: "default"
  pvcName: "pvc1"
  mountPath: "/data"
destination:
  clusterContext: "c2"
  namespace: "default"
  pvcName: "pvc2"
  mountPath: "/data"
job:
  image: "alpine:latest"
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	os.Setenv("PVC_TRANSFER_S3_ACCESS_KEY", "key")
	os.Setenv("PVC_TRANSFER_S3_SECRET_KEY", "secret")
	defer os.Clearenv()

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if cfg.Job.ServiceAccount != "pvc-transfer-sa" {
		t.Fatalf("expected default service account set, got %s", cfg.Job.ServiceAccount)
	}
	if cfg.Job.TTLSecondsAfterFinish != 3600 {
		t.Fatalf("expected TTL default, got %d", cfg.Job.TTLSecondsAfterFinish)
	}
	if cfg.TimeoutMins != 60 {
		t.Fatalf("expected timeout default, got %d", cfg.TimeoutMins)
	}
	if cfg.Cleanup == nil || !*cfg.Cleanup {
		t.Fatalf("expected cleanup to default true")
	}
	if cfg.S3.ObjectKey != DefaultObjectKey("default", "pvc1") {
		t.Fatalf("expected default object key, got %s", cfg.S3.ObjectKey)
	}
}

func TestValidateFailsOnBadPVC(t *testing.T) {
	cfg := Config{
		Version: "v1",
		S3:      S3Config{Bucket: "b", Region: "r", AccessKey: "a", SecretKey: "s"},
		Source:  ClusterConfig{ClusterContext: "ctx", Namespace: "default", PVCName: "BAD*", MountPath: "/data"},
		Destination: ClusterConfig{
			ClusterContext: "ctx2", Namespace: "default", PVCName: "ok", MountPath: "/data",
		},
		Job: JobConfig{Image: "alpine", ServiceAccount: "sa"},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation failure")
	}
}
