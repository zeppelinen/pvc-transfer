package main

import (
	"testing"

	"github.com/zeppelinen/pvc-transfer/internal/config"
	"github.com/zeppelinen/pvc-transfer/internal/orchestrator"
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
			PVCs:           []config.PVCConfig{{Name: "pvc1", MountPath: "/data"}},
		},
		Destination: config.ClusterConfig{
			ClusterContext: "ctx2",
			Namespace:      "ns2",
			PVCName:        "pvc2",
			MountPath:      "/data",
			PVCs:           []config.PVCConfig{{Name: "pvc2", MountPath: "/data"}},
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

func TestApplyOverridesPreservesMountPath(t *testing.T) {
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
			MountPath:      "/mnt/source",
		},
		Destination: config.ClusterConfig{
			ClusterContext: "ctx2",
			Namespace:      "ns2",
			PVCName:        "pvc2",
			MountPath:      "/mnt/dest",
		},
		Job: config.JobConfig{Image: "alpine", ServiceAccount: "sa"},
	}

	applyOverrides(&cfg, overrides{
		sourcePVC: "new-src-pvc",
		destPVC:   "new-dst-pvc",
	})

	if len(cfg.Source.PVCs) != 1 || cfg.Source.PVCs[0].MountPath != "/mnt/source" {
		t.Fatalf("expected source MountPath /mnt/source preserved, got %+v", cfg.Source.PVCs)
	}
	if len(cfg.Destination.PVCs) != 1 || cfg.Destination.PVCs[0].MountPath != "/mnt/dest" {
		t.Fatalf("expected dest MountPath /mnt/dest preserved, got %+v", cfg.Destination.PVCs)
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
			PVCs:           []config.PVCConfig{{Name: "pvc1", MountPath: "/data"}},
		},
		Destination: config.ClusterConfig{
			ClusterContext: "ctx2",
			Namespace:      "ns2",
			PVCName:        "pvc2",
			MountPath:      "/data",
			PVCs:           []config.PVCConfig{{Name: "pvc2", MountPath: "/data"}},
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

func TestParseRunOptionsMutuallyExclusive(t *testing.T) {
	if _, err := parseRunOptions(true, true); err == nil {
		t.Fatalf("expected error when both export-only and import-only are set")
	}
}

func TestParseRunOptionsValues(t *testing.T) {
	opts, err := parseRunOptions(true, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts != (orchestrator.RunOptions{ExportOnly: true}) {
		t.Fatalf("expected export-only to be true and import-only false, got %#v", opts)
	}

	opts, err = parseRunOptions(false, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts != (orchestrator.RunOptions{ImportOnly: true}) {
		t.Fatalf("expected export-only to be false and import-only true, got %#v", opts)
	}

	opts, err = parseRunOptions(false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts != (orchestrator.RunOptions{}) {
		t.Fatalf("expected both flags false by default, got %#v", opts)
	}
}
