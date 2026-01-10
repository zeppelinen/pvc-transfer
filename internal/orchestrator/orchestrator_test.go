package orchestrator

import "testing"

func TestResolveServiceAccount(t *testing.T) {
	name, err := resolveServiceAccount("sa", "ns")
	if err != nil || name != "sa" {
		t.Fatalf("expected plain service account name, got %q err=%v", name, err)
	}

	name, err = resolveServiceAccount("ns/sa", "ns")
	if err != nil || name != "sa" {
		t.Fatalf("expected namespaced service account to resolve name, got %q err=%v", name, err)
	}

	if _, err := resolveServiceAccount("other/sa", "ns"); err == nil {
		t.Fatalf("expected mismatch namespace to error")
	}

	if _, err := resolveServiceAccount("bad/format/extra", "ns"); err == nil {
		t.Fatalf("expected invalid format to error")
	}
}
