package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zeppelinen/pvc-transfer/internal/kube"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestEnsurePVC(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns"},
	})
	if err := ensurePVC(context.Background(), client, "ns", "data"); err != nil {
		t.Fatalf("expected pvc to be found: %v", err)
	}
	if err := ensurePVC(context.Background(), client, "ns", "missing"); err == nil {
		t.Fatalf("expected error for missing pvc")
	}
}

func TestEnsureServiceAccount(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "ns"},
	})
	if err := ensureServiceAccount(context.Background(), client, "ns", "sa"); err != nil {
		t.Fatalf("expected sa to be found: %v", err)
	}
	if err := ensureServiceAccount(context.Background(), client, "ns", "other"); err == nil {
		t.Fatalf("expected error for missing service account")
	}
}

func TestCleanupJobDeletesResources(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "transfer", Namespace: "ns"}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: kube.SecretName(job.Name), Namespace: "ns"}}
	client := fake.NewSimpleClientset(job, secret)

	if err := cleanupJob(context.Background(), client, job); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if _, err := client.BatchV1().Jobs("ns").Get(context.Background(), job.Name, metav1.GetOptions{}); err == nil {
		t.Fatalf("expected job to be deleted")
	}
	if _, err := client.CoreV1().Secrets("ns").Get(context.Background(), secret.Name, metav1.GetOptions{}); err == nil {
		t.Fatalf("expected secret to be deleted")
	}
}

func TestRetryRespectsAttemptsAndContext(t *testing.T) {
	var calls int
	err := retry(context.Background(), 1*time.Millisecond, 3, func() error {
		calls++
		if calls == 2 {
			return nil
		}
		return errors.New("fail")
	})
	if err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = retry(ctx, 1*time.Millisecond, 3, func() error {
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestResolvePhases(t *testing.T) {
	export, importPhase, err := resolvePhases(RunOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !export || !importPhase {
		t.Fatalf("expected both phases to run by default")
	}

	export, importPhase, err = resolvePhases(RunOptions{ExportOnly: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !export || importPhase {
		t.Fatalf("expected export only, got export=%t import=%t", export, importPhase)
	}

	export, importPhase, err = resolvePhases(RunOptions{ImportOnly: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if export || !importPhase {
		t.Fatalf("expected import only, got export=%t import=%t", export, importPhase)
	}

	if _, _, err := resolvePhases(RunOptions{ExportOnly: true, ImportOnly: true}); err == nil {
		t.Fatalf("expected error when both phases requested")
	}
}
