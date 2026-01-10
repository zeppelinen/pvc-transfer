package main

import (
	"testing"

	"github.com/zeppelinen/pvc-transfer/internal/config"
)

func TestApplyOverridesBoolFlagsOverrideConfig(t *testing.T) {
	cfg := config.Config{
		Source:      config.ClusterConfig{Namespace: "ns", PVCName: "pvc"},
		Destination: config.ClusterConfig{Namespace: "ns2", PVCName: "pvc2"},
		S3:          config.S3Config{Bucket: "b", Region: "r"},
		Job:         config.JobConfig{Image: "img", ServiceAccount: "sa"},
		Overwrite:   true,
	}
	cfg.Cleanup = boolPtr(true)
	applyOverrides(&cfg, overrides{
		overwrite: boolPtr(false),
		cleanup:   boolPtr(false),
	})
	if cfg.Overwrite {
		t.Fatalf("expected overwrite to be overridden to false")
	}
	if cfg.Cleanup == nil || *cfg.Cleanup {
		t.Fatalf("expected cleanup overridden to false")
	}
}

func TestApplyOverridesUpdatesObjectKeyWhenSourceChanges(t *testing.T) {
	cfg := config.Config{
		Source:      config.ClusterConfig{Namespace: "ns", PVCName: "pvc"},
		Destination: config.ClusterConfig{Namespace: "ns2", PVCName: "pvc2"},
		S3:          config.S3Config{Bucket: "b", Region: "r", ObjectKeyDerived: true},
		Job:         config.JobConfig{Image: "img", ServiceAccount: "sa"},
	}
	applyOverrides(&cfg, overrides{sourcePVC: "newpvc"})
	if cfg.S3.ObjectKey == "" {
		t.Fatalf("expected object key to be derived after source change")
	}
	if cfg.Source.PVCName != "newpvc" {
		t.Fatalf("expected source pvc override")
	}
}

func boolPtr(v bool) *bool { return &v }
