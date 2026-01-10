package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/zeppelinen/pvc-transfer/internal/config"
	"github.com/zeppelinen/pvc-transfer/internal/orchestrator"
)

func main() {
	var (
		configPath   string
		s3ObjectKey  string
		sourcePVC    string
		destPVC      string
		sourceNS     string
		destNS       string
		overwriteObj boolFlag
		skipCleanup  boolFlag
	)

	flag.StringVar(&configPath, "config", "config.yaml", "Path to YAML configuration file")
	flag.Var(&overwriteObj, "overwrite", "Allow overwriting existing S3 object")
	flag.Var(&skipCleanup, "no-cleanup", "Skip cleanup of Jobs/S3 object")
	flag.StringVar(&s3ObjectKey, "s3-object-key", "", "Override S3 object key")
	flag.StringVar(&sourcePVC, "source-pvc", "", "Override source PVC name")
	flag.StringVar(&destPVC, "dest-pvc", "", "Override destination PVC name")
	flag.StringVar(&sourceNS, "source-namespace", "", "Override source namespace")
	flag.StringVar(&destNS, "dest-namespace", "", "Override destination namespace")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	applyOverrides(&cfg, overrides{
		s3ObjectKey: s3ObjectKey,
		sourcePVC:   sourcePVC,
		destPVC:     destPVC,
		sourceNS:    sourceNS,
		destNS:      destNS,
	})
	if overwriteObj.set {
		applyOverrides(&cfg, overrides{overwrite: &overwriteObj.value})
	}
	if skipCleanup.set {
		cleanup := !skipCleanup.value
		applyOverrides(&cfg, overrides{cleanup: &cleanup})
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	orch := orchestrator.New()
	runCtx, timeoutCancel := context.WithTimeout(ctx, 90*time.Minute)
	defer timeoutCancel()

	if err := orch.Run(runCtx, cfg); err != nil {
		log.Fatalf("transfer failed: %v", err)
	}

	fmt.Println("PVC transfer completed successfully")
}

type overrides struct {
	s3ObjectKey string
	sourcePVC   string
	destPVC     string
	sourceNS    string
	destNS      string
	overwrite   *bool
	cleanup     *bool
}

func applyOverrides(cfg *config.Config, ov overrides) {
	sourceChanged := false
	if ov.sourcePVC != "" && ov.sourcePVC != cfg.Source.PVCName {
		cfg.Source.PVCName = ov.sourcePVC
		sourceChanged = true
	}
	if ov.sourceNS != "" && ov.sourceNS != cfg.Source.Namespace {
		cfg.Source.Namespace = ov.sourceNS
		sourceChanged = true
	}
	if ov.destPVC != "" {
		cfg.Destination.PVCName = ov.destPVC
	}
	if ov.destNS != "" {
		cfg.Destination.Namespace = ov.destNS
	}
	if ov.s3ObjectKey != "" {
		cfg.S3.ObjectKey = ov.s3ObjectKey
		cfg.S3.ObjectKeyDerived = false
	} else if sourceChanged && cfg.S3.ObjectKeyDerived {
		cfg.S3.ObjectKey = config.DefaultObjectKey(cfg.Source.Namespace, cfg.Source.PVCName)
	}
	if ov.overwrite != nil {
		cfg.Overwrite = *ov.overwrite
	}
	if ov.cleanup != nil {
		cfg.Cleanup = ov.cleanup
	}
}

// boolFlag tracks whether a bool flag was explicitly set.
type boolFlag struct {
	set   bool
	value bool
}

func (b *boolFlag) String() string {
	return strconv.FormatBool(b.value)
}

func (b *boolFlag) Set(s string) error {
	v, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	b.value = v
	b.set = true
	return nil
}

func (b *boolFlag) IsBoolFlag() bool { return true }
