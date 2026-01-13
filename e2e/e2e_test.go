//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zeppelinen/pvc-transfer/internal/config"
	"github.com/zeppelinen/pvc-transfer/internal/orchestrator"
	"gopkg.in/yaml.v3"
	"k8s.io/client-go/tools/clientcmd"
)

// This test expects k3d clusters and MinIO to already be provisioned by scripts/bootstrap-e2e.sh.
func TestPVCTransferEndToEnd(t *testing.T) {
	configPath := os.Getenv("E2E_CONFIG")
	if configPath == "" {
		t.Skip("E2E_CONFIG not set; skipping e2e test")
	}
	path, err := filepath.Abs(configPath)
	if err != nil {
		t.Fatalf("resolve config path: %v", err)
	}
	resolveKubeconfigEnv(t, path)
	logKubeconfigInfo(t)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()

	orch := orchestrator.New()
	if err := orch.Run(ctx, cfg, orchestrator.RunOptions{}); err != nil {
		t.Fatalf("orchestrator run failed: %v", err)
	}
}

// This test verifies CLI overrides for S3 object key and PVC names.
func TestPVCTransferCLIOverrides(t *testing.T) {
	configPath := os.Getenv("E2E_CONFIG")
	if configPath == "" {
		t.Skip("E2E_CONFIG not set; skipping e2e test")
	}
	path, err := filepath.Abs(configPath)
	if err != nil {
		t.Fatalf("resolve config path: %v", err)
	}
	resolveKubeconfigEnv(t, path)
	logKubeconfigInfo(t)

	baseCfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	overrideKey := fmt.Sprintf("migrations/override-%d.tar.gz", time.Now().UnixNano())
	overrideCfg := baseCfg
	overrideCfg.S3.ObjectKey = "migrations/invalid-key.tar.gz"
	overrideCfg.Source.PVCName = "invalid-source-pvc"
	overrideCfg.Destination.PVCName = "invalid-dest-pvc"
	overrideCfg.Source.Namespace = "invalid-source-ns"
	overrideCfg.Destination.Namespace = "invalid-dest-ns"

	data, err := yaml.Marshal(&overrideCfg)
	if err != nil {
		t.Fatalf("marshal override config: %v", err)
	}

	tmpConfig := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpConfig, data, 0o644); err != nil {
		t.Fatalf("write override config: %v", err)
	}

	repoRoot := repoRootFromConfig(path)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()

	runCLI := func(args ...string) {
		cmd := exec.CommandContext(ctx, "go", "run", "./cmd/pvc-transfer", args...)
		cmd.Dir = repoRoot
		cmd.Env = os.Environ()

		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output

		if err := cmd.Run(); err != nil {
			t.Fatalf("cli override run failed: %v\noutput:\n%s", err, output.String())
		}
	}

	runCLI(
		"--config", tmpConfig,
		"--s3-object-key", overrideKey,
		"--source-pvc", baseCfg.Source.PVCName,
		"--dest-pvc", baseCfg.Destination.PVCName,
		"--source-namespace", baseCfg.Source.Namespace,
		"--dest-namespace", baseCfg.Destination.Namespace,
		"--overwrite",
		"--export-only",
	)

	runCLI(
		"--config", tmpConfig,
		"--s3-object-key", overrideKey,
		"--source-pvc", baseCfg.Source.PVCName,
		"--dest-pvc", baseCfg.Destination.PVCName,
		"--source-namespace", baseCfg.Source.Namespace,
		"--dest-namespace", baseCfg.Destination.Namespace,
		"--import-only",
	)
}

func TestPVCTransferDefaultObjectKey(t *testing.T) {
	configPath := os.Getenv("E2E_CONFIG")
	if configPath == "" {
		t.Skip("E2E_CONFIG not set; skipping e2e test")
	}
	path, err := filepath.Abs(configPath)
	if err != nil {
		t.Fatalf("resolve config path: %v", err)
	}
	resolveKubeconfigEnv(t, path)
	logKubeconfigInfo(t)

	baseCfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	baseCfg.S3.ObjectKey = ""
	baseCfg.S3.ObjectKeyDerived = false

	data, err := yaml.Marshal(&baseCfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}

	tmpConfig := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpConfig, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	repoRoot := repoRootFromConfig(path)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/pvc-transfer",
		"--config", tmpConfig,
		"--overwrite",
	)
	cmd.Dir = repoRoot
	cmd.Env = os.Environ()

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Run(); err != nil {
		t.Fatalf("default object key run failed: %v\noutput:\n%s", err, output.String())
	}
}

func logKubeconfigInfo(t *testing.T) {
	t.Helper()
	kubeconfig := os.Getenv("KUBECONFIG")
	t.Logf("KUBECONFIG=%q", kubeconfig)
	if kubeconfig != "" {
		if _, err := os.Stat(kubeconfig); err != nil {
			t.Logf("KUBECONFIG path error: %v", err)
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	cfg, err := rules.Load()
	if err != nil {
		t.Logf("failed to load kubeconfig: %v", err)
		return
	}
	t.Logf("currentContext=%q", cfg.CurrentContext)
	for name, ctx := range cfg.Contexts {
		t.Logf("context=%q cluster=%q user=%q", name, ctx.Cluster, ctx.AuthInfo)
	}
}

func resolveKubeconfigEnv(t *testing.T, configPath string) {
	t.Helper()
	configDir := filepath.Dir(configPath)
	env := os.Getenv("KUBECONFIG")
	if env == "" {
		defaultPath := filepath.Join(configDir, "kubeconfig")
		if fileExists(defaultPath) {
			if err := os.Setenv("KUBECONFIG", defaultPath); err != nil {
				t.Fatalf("set KUBECONFIG: %v", err)
			}
		}
		return
	}
	parts := filepath.SplitList(env)
	changed := false
	resolved := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if filepath.IsAbs(part) {
			resolved = append(resolved, part)
			continue
		}
		if candidate := filepath.Join(configDir, part); fileExists(candidate) {
			resolved = append(resolved, candidate)
			changed = true
			continue
		}
		if filepath.Base(configDir) == ".tmp" {
			repoRoot := filepath.Dir(configDir)
			if candidate := filepath.Join(repoRoot, part); fileExists(candidate) {
				resolved = append(resolved, candidate)
				changed = true
				continue
			}
		}
		if cwd, err := os.Getwd(); err == nil {
			if candidate := filepath.Join(cwd, part); fileExists(candidate) {
				resolved = append(resolved, candidate)
				changed = true
				continue
			}
		}
		resolved = append(resolved, part)
	}
	if changed {
		if err := os.Setenv("KUBECONFIG", strings.Join(resolved, string(os.PathListSeparator))); err != nil {
			t.Fatalf("set KUBECONFIG: %v", err)
		}
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func repoRootFromConfig(path string) string {
	dir := filepath.Dir(path)
	if filepath.Base(dir) == ".tmp" {
		return filepath.Dir(dir)
	}
	if cwd, err := os.Getwd(); err == nil {
		return cwd
	}
	return dir
}
