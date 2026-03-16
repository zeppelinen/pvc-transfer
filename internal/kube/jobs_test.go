package kube

import (
	"strings"
	"testing"

	"github.com/zeppelinen/pvc-transfer/internal/config"
)

func baseConfig() config.Config {
	cleanup := true
	return config.Config{
		Version: "v1",
		S3: config.S3Config{
			Bucket:      "bucket",
			Region:      "us-east-1",
			Endpoint:    "http://localhost:9000",
			JobEndpoint: "http://minio:9000",
			AccessKey:   "AKIA",
			SecretKey:   "SECRET",
			ObjectKey:   "obj.tar.gz",
		},
		Source: config.ClusterConfig{
			ClusterContext: "src",
			Namespace:      "default",
			PVCName:        "src-pvc",
			MountPath:      "/data",
			PVCs:           []config.PVCConfig{{Name: "src-pvc", MountPath: "/data"}},
		},
		Destination: config.ClusterConfig{
			ClusterContext: "dst",
			Namespace:      "default",
			PVCName:        "dst-pvc",
			MountPath:      "/data",
			PVCs:           []config.PVCConfig{{Name: "dst-pvc", MountPath: "/data"}},
		},
		Job: config.JobConfig{
			Image:          "alpine",
			ServiceAccount: "sa",
		},
		Cleanup: &cleanup,
	}
}

func TestBuildImportJob(t *testing.T) {
	cfg := baseConfig()
	job := BuildImportJob(cfg, "import", "default")
	if job.Spec.Template.Spec.Containers[0].VolumeMounts[0].ReadOnly {
		t.Fatalf("import job should mount PVC writable")
	}
	cmd := strings.Join(job.Spec.Template.Spec.Containers[0].Command, " ")
	if !strings.Contains(cmd, "tar -xvzf - -C /data") {
		t.Fatalf("unexpected import command: %s", cmd)
	}
}

func TestSecretNameDeterministic(t *testing.T) {
	if SecretName("jobA") != "jobA-s3-credentials" {
		t.Fatalf("unexpected secret name")
	}
}

func TestBuildExportJob(t *testing.T) {
	cfg := baseConfig()
	job := BuildExportJob(cfg, "export", "default")
	if job.Spec.Template.Spec.ServiceAccountName != cfg.Job.ServiceAccount {
		t.Fatalf("expected service account %s", cfg.Job.ServiceAccount)
	}
	cmd := strings.Join(job.Spec.Template.Spec.Containers[0].Command, " ")
	if !strings.Contains(cmd, "aws s3 cp - s3://bucket/obj.tar.gz") {
		t.Fatalf("unexpected export command: %s", cmd)
	}
	env := job.Spec.Template.Spec.Containers[0].Env
	foundEndpoint := false
	for _, e := range env {
		if e.Name == "AWS_ENDPOINT_URL" && e.Value == cfg.S3.JobEndpoint {
			foundEndpoint = true
		}
	}
	if !foundEndpoint {
		t.Fatalf("expected AWS_ENDPOINT_URL env var")
	}
}
