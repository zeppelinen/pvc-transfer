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
	)

	flag.StringVar(&configPath, "config", "config.yaml", "Path to YAML configuration file")
	flag.BoolVar(&overwriteObj, "overwrite", false, "Allow overwriting existing S3 object")
	flag.BoolVar(&skipCleanup, "no-cleanup", false, "Skip cleanup of Jobs/S3 object")
	flag.StringVar(&s3ObjectKey, "s3-object-key", "", "Override S3 object key")
	flag.StringVar(&sourcePVC, "source-pvc", "", "Override source PVC name")
	flag.StringVar(&destPVC, "dest-pvc", "", "Override destination PVC name")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if s3ObjectKey != "" {
		cfg.S3.ObjectKey = s3ObjectKey
	}
	if sourcePVC != "" {
		cfg.Source.PVCName = sourcePVC
	}
	if destPVC != "" {
		cfg.Destination.PVCName = destPVC
	}
	cfg.Overwrite = cfg.Overwrite || overwriteObj
	if skipCleanup {
		value := false
		cfg.Cleanup = &value
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
