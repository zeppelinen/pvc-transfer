package orchestrator

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

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

func TestContainerFromErrorSelectsWorker(t *testing.T) {
	err := fmt.Errorf("a container name must be specified for pod foo, choose one of: [istio-init istio-proxy worker]")
	if c, ok := containerFromError(err, "worker"); !ok || c != "worker" {
		t.Fatalf("expected to pick worker, got %s ok=%v", c, ok)
	}
	err = fmt.Errorf("a container name must be specified for pod foo, choose one of: [sidecar main]")
	if c, ok := containerFromError(err, "worker"); !ok || c != "main" {
		t.Fatalf("expected to pick preferred main, got %s ok=%v", c, ok)
	}
}

func TestContainerDone(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "worker",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}},
			}},
		},
	})
	done, err := containerDone(context.Background(), client, "ns", "p", "worker")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !done {
		t.Fatalf("expected container done")
	}
}
