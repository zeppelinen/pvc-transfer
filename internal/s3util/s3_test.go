package s3util

import (
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestBuildObjectURL(t *testing.T) {
	out := BuildObjectURL("http://localhost:9000", "bucket", "path/to/key.tar.gz")
	want := "http://localhost:9000/bucket/path%2Fto%2Fkey.tar.gz"
	if out != want {
		t.Fatalf("expected %s, got %s", want, out)
	}
	out = BuildObjectURL("", "bucket", "key")
	if out != "s3://bucket/key" {
		t.Fatalf("expected s3 scheme fallback, got %s", out)
	}
}

func TestGenerateProbeKey(t *testing.T) {
	// Test that generateProbeKey produces valid keys
	key, err := generateProbeKey()
	if err != nil {
		t.Fatalf("generateProbeKey failed: %v", err)
	}

	// Verify key format: pvc-transfer-probe-{timestamp}-{pid}-{random}
	// Expected pattern: pvc-transfer-probe-<digits>-<digits>-<hex>
	pattern := regexp.MustCompile(`^pvc-transfer-probe-\d+-\d+-[0-9a-f]{16}$`)
	if !pattern.MatchString(key) {
		t.Errorf("key format mismatch, got: %s", key)
	}

	// Verify it starts with the expected prefix
	if !strings.HasPrefix(key, "pvc-transfer-probe-") {
		t.Errorf("key missing expected prefix, got: %s", key)
	}
}

func TestGenerateProbeKeyUniqueness(t *testing.T) {
	// Generate multiple keys and verify they're all unique
	keys := make(map[string]bool)
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		key, err := generateProbeKey()
		if err != nil {
			t.Fatalf("generateProbeKey failed on iteration %d: %v", i, err)
		}
		if keys[key] {
			t.Errorf("duplicate key generated: %s", key)
		}
		keys[key] = true
	}

	if len(keys) != iterations {
		t.Errorf("expected %d unique keys, got %d", iterations, len(keys))
	}
}

func TestGenerateProbeKeyConcurrency(t *testing.T) {
	// Test concurrent key generation to ensure no collisions
	const goroutines = 100
	const keysPerGoroutine = 100
	
	keys := make(chan string, goroutines*keysPerGoroutine)
	var wg sync.WaitGroup
	
	// Launch multiple goroutines generating keys concurrently
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < keysPerGoroutine; j++ {
				key, err := generateProbeKey()
				if err != nil {
					t.Errorf("generateProbeKey failed: %v", err)
					return
				}
				keys <- key
			}
		}()
	}
	
	wg.Wait()
	close(keys)
	
	// Check for duplicates
	seen := make(map[string]bool)
	totalKeys := 0
	for key := range keys {
		if seen[key] {
			t.Errorf("duplicate key detected in concurrent generation: %s", key)
		}
		seen[key] = true
		totalKeys++
	}
	
	expectedKeys := goroutines * keysPerGoroutine
	if totalKeys != expectedKeys {
		t.Errorf("expected %d keys, got %d", expectedKeys, totalKeys)
	}
}
