package main

import (
	"testing"

	"github.com/zeppelinen/pvc-transfer/internal/config"
)

func TestApplyOverridesRecomputesObjectKey(t *testing.T) {
	cfg := config.Config{
		Version: "v1",
		S3: config.S3Config{
			Bucket:           "b",
			Region:           "us-east-1",
			ObjectKey:        config.DefaultObjectKey("ns1", "pvc1"),
			ObjectKeyDerived: true,
		},
		Source: config.ClusterConfig{
			ClusterContext: "ctx",
			Namespace:      "ns1",
			PVCName:        "pvc1",
			MountPath:      "/data",
		},
		Destination: config.ClusterConfig{
			ClusterContext: "ctx2",
			Namespace:      "ns2",
			PVCName:        "pvc2",
			MountPath:      "/data",
		},
		Job: config.JobConfig{Image: "alpine", ServiceAccount: "sa"},
	}

	applyOverrides(&cfg, overrides{
		sourceNS: "override-ns",
	})

	if cfg.S3.ObjectKey != config.DefaultObjectKey("override-ns", "pvc1") {
		t.Fatalf("expected object key recompute, got %s", cfg.S3.ObjectKey)
	}
}

func TestApplyOverridesNamespaces(t *testing.T) {
	cfg := config.Config{
		Version: "v1",
		S3: config.S3Config{
			Bucket:    "b",
			Region:    "us-east-1",
			ObjectKey: "obj",
		},
		Source: config.ClusterConfig{
			ClusterContext: "ctx",
			Namespace:      "ns1",
			PVCName:        "pvc1",
			MountPath:      "/data",
		},
		Destination: config.ClusterConfig{
			ClusterContext: "ctx2",
			Namespace:      "ns2",
			PVCName:        "pvc2",
			MountPath:      "/data",
		},
		Job: config.JobConfig{Image: "alpine", ServiceAccount: "sa"},
	}

	applyOverrides(&cfg, overrides{
		sourceNS: "new-src",
		destNS:   "new-dest",
	})

	if cfg.Source.Namespace != "new-src" {
		t.Fatalf("expected source namespace override, got %s", cfg.Source.Namespace)
	}
	if cfg.Destination.Namespace != "new-dest" {
		t.Fatalf("expected destination namespace override, got %s", cfg.Destination.Namespace)
	}
}
