package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/zeppelinen/pvc-transfer/internal/config"
	"github.com/zeppelinen/pvc-transfer/internal/orchestrator"
)

func main() {
	var (
		configPath   string
		overwriteObj bool
		skipCleanup  bool
		s3ObjectKey  string
		sourcePVC    string
		destPVC      string
		sourceNS     string
		destNS       string
		exportOnly   bool
		importOnly   bool
	)

	flag.StringVar(&configPath, "config", "config.yaml", "Path to YAML configuration file")
	flag.BoolVar(&overwriteObj, "overwrite", false, "Allow overwriting existing S3 object")
	flag.BoolVar(&skipCleanup, "no-cleanup", false, "Skip cleanup of Jobs/S3 object")
	flag.StringVar(&s3ObjectKey, "s3-object-key", "", "Override S3 object key")
	flag.StringVar(&sourcePVC, "source-pvc", "", "Override source PVC name")
	flag.StringVar(&destPVC, "dest-pvc", "", "Override destination PVC name")
	flag.StringVar(&sourceNS, "source-namespace", "", "Override source namespace")
	flag.StringVar(&destNS, "dest-namespace", "", "Override destination namespace")
	flag.BoolVar(&exportOnly, "export-only", false, "Run only the data export to S3")
	flag.BoolVar(&importOnly, "import-only", false, "Run only the data import from S3")
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
		overwrite:   overwriteObj,
		noCleanup:   skipCleanup,
	})

	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	runOpts, err := parseRunOptions(exportOnly, importOnly)
	if err != nil {
		log.Fatalf("invalid flags: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	orch := orchestrator.New()
	runCtx, timeoutCancel := context.WithTimeout(ctx, 90*time.Minute)
	defer timeoutCancel()

	if err := orch.Run(runCtx, cfg, runOpts); err != nil {
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
	overwrite   bool
	noCleanup   bool
}

func applyOverrides(cfg *config.Config, ov overrides) {
	sourceChanged := false
	if ov.sourcePVC != "" && ov.sourcePVC != cfg.Source.PVCName {
		cfg.Source.PVCName = ov.sourcePVC
		if len(cfg.Source.PVCs) > 0 {
			cfg.Source.PVCs[0].Name = ov.sourcePVC
		} else {
			cfg.Source.PVCs = []config.PVCConfig{{Name: ov.sourcePVC}}
		}
		sourceChanged = true
	}
	if ov.sourceNS != "" && ov.sourceNS != cfg.Source.Namespace {
		cfg.Source.Namespace = ov.sourceNS
		sourceChanged = true
	}
	if ov.destPVC != "" {
		cfg.Destination.PVCName = ov.destPVC
		if len(cfg.Destination.PVCs) > 0 {
			cfg.Destination.PVCs[0].Name = ov.destPVC
		} else {
			cfg.Destination.PVCs = []config.PVCConfig{{Name: ov.destPVC}}
		}
	}
	if ov.destNS != "" {
		cfg.Destination.Namespace = ov.destNS
	}
	if ov.s3ObjectKey != "" {
		cfg.S3.ObjectKey = ov.s3ObjectKey
		cfg.S3.ObjectKeyDerived = false
	} else if sourceChanged && cfg.S3.ObjectKeyDerived {
		firstPVC := "unknown"
		if len(cfg.Source.PVCs) > 0 {
			firstPVC = cfg.Source.PVCs[0].Name
		}
		cfg.S3.ObjectKey = config.DefaultObjectKey(cfg.Source.Namespace, firstPVC)
	}
	cfg.Overwrite = cfg.Overwrite || ov.overwrite
	if ov.noCleanup {
		value := false
		cfg.Cleanup = &value
	}
}

func parseRunOptions(exportOnly, importOnly bool) (orchestrator.RunOptions, error) {
	if exportOnly && importOnly {
		return orchestrator.RunOptions{}, fmt.Errorf("--export-only and --import-only cannot be used together")
	}
	return orchestrator.RunOptions{
		ExportOnly: exportOnly,
		ImportOnly: importOnly,
	}, nil
}
