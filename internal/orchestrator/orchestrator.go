package orchestrator

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/zeppelinen/pvc-transfer/internal/config"
	"github.com/zeppelinen/pvc-transfer/internal/kube"
	"github.com/zeppelinen/pvc-transfer/internal/s3util"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Orchestrator manages the end-to-end transfer workflow.
type Orchestrator struct{}

const workerContainerName = "worker"

// New constructs an orchestrator instance.
func New() *Orchestrator {
	return &Orchestrator{}
}

// Run executes the transfer per configuration.
func (o *Orchestrator) Run(ctx context.Context, cfg config.Config) error {
	log.Printf("starting transfer: source %s/%s (cluster=%s) -> destination %s/%s (cluster=%s); object=%s; overwrite=%t",
		cfg.Source.Namespace, cfg.Source.PVCName, cfg.Source.ClusterContext,
		cfg.Destination.Namespace, cfg.Destination.PVCName, cfg.Destination.ClusterContext,
		s3util.BuildObjectURL(cfg.S3.Endpoint, cfg.S3.Bucket, cfg.S3.ObjectKey), cfg.Overwrite)

	s3Client, err := s3util.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init s3 client: %w", err)
	}

	// Verify bucket connectivity with retry.
	if err := retry(ctx, time.Duration(cfg.RetryBackoff.Seconds)*time.Second, cfg.RetryBackoff.Attempts, func() error {
		return s3Client.VerifyBucket(ctx, cfg.S3.Bucket)
	}); err != nil {
		return fmt.Errorf("bucket verification failed: %w", err)
	}

	sourceSA, err := resolveServiceAccount(cfg.Job.ServiceAccount, cfg.Source.Namespace)
	if err != nil {
		return err
	}
	destSA, err := resolveServiceAccount(cfg.Job.ServiceAccount, cfg.Destination.Namespace)
	if err != nil {
		return err
	}

	exists, err := s3Client.ObjectExists(ctx, cfg.S3.Bucket, cfg.S3.ObjectKey)
	if err != nil {
		return fmt.Errorf("check object existence: %w", err)
	}
	if exists && !cfg.Overwrite {
		return fmt.Errorf("object s3://%s/%s already exists; rerun with --overwrite or overwrite: true", cfg.S3.Bucket, cfg.S3.ObjectKey)
	}

	sourceClient, err := kube.NewClientset(cfg.Source.ClusterContext)
	if err != nil {
		return err
	}
	destClient, err := kube.NewClientset(cfg.Destination.ClusterContext)
	if err != nil {
		return err
	}

	if err := ensurePVC(ctx, sourceClient, cfg.Source.Namespace, cfg.Source.PVCName); err != nil {
		return err
	}
	if err := ensurePVC(ctx, destClient, cfg.Destination.Namespace, cfg.Destination.PVCName); err != nil {
		return err
	}

	var srcRBAC, dstRBAC *rbacResources
	if cfg.RBAC.AutoCreate {
		srcRBAC, err = ensureRBAC(ctx, sourceClient, cfg.Source.Namespace, sourceSA)
		if err != nil {
			return err
		}
		dstRBAC, err = ensureRBAC(ctx, destClient, cfg.Destination.Namespace, destSA)
		if err != nil {
			return err
		}
	} else {
		if err := ensureServiceAccount(ctx, sourceClient, cfg.Source.Namespace, sourceSA); err != nil {
			return err
		}
		if err := ensureServiceAccount(ctx, destClient, cfg.Destination.Namespace, destSA); err != nil {
			return err
		}
	}

	exportJobName := "pvc-transfer-export"
	importJobName := "pvc-transfer-import"
	verifyJobName := "pvc-transfer-verify"

	if err := prepareSecret(ctx, sourceClient, cfg.Source.Namespace, kube.SecretName(exportJobName), cfg); err != nil {
		return err
	}
	if err := prepareSecret(ctx, destClient, cfg.Destination.Namespace, kube.SecretName(importJobName), cfg); err != nil {
		return err
	}

	exportJob := kube.BuildExportJob(cfg, exportJobName, cfg.Source.Namespace, sourceSA)
	if err := applyJob(ctx, sourceClient, exportJob); err != nil {
		return fmt.Errorf("create export job: %w", err)
	}
	log.Printf("started export job %s in %s/%s", exportJobName, cfg.Source.ClusterContext, cfg.Source.Namespace)
	if err := monitorJob(ctx, sourceClient, exportJob, "export"); err != nil {
		return err
	}

	importJob := kube.BuildImportJob(cfg, importJobName, cfg.Destination.Namespace, destSA)
	if err := prepareSecret(ctx, destClient, cfg.Destination.Namespace, kube.SecretName(importJobName), cfg); err != nil {
		return err
	}
	if err := applyJob(ctx, destClient, importJob); err != nil {
		return fmt.Errorf("create import job: %w", err)
	}
	log.Printf("started import job %s in %s/%s", importJobName, cfg.Destination.ClusterContext, cfg.Destination.Namespace)
	if err := monitorJob(ctx, destClient, importJob, "import"); err != nil {
		return err
	}

	if cfg.Job.VerifyMd5 {
		verifyJob := kube.BuildVerifyJob(cfg, verifyJobName, cfg.Destination.Namespace, destSA)
		if err := prepareSecret(ctx, destClient, cfg.Destination.Namespace, kube.SecretName(verifyJobName), cfg); err != nil {
			return err
		}
		if err := applyJob(ctx, destClient, verifyJob); err != nil {
			return fmt.Errorf("create verify job: %w", err)
		}
		log.Printf("started verify job %s in %s/%s", verifyJobName, cfg.Destination.ClusterContext, cfg.Destination.Namespace)
		if err := monitorJob(ctx, destClient, verifyJob, "verify"); err != nil {
			return err
		}
	}

	if cfg.Cleanup != nil && *cfg.Cleanup {
		if err := cleanupJob(ctx, sourceClient, exportJob); err != nil {
			log.Printf("cleanup export job failed: %v", err)
		}
		if err := cleanupJob(ctx, destClient, importJob); err != nil {
			log.Printf("cleanup import job failed: %v", err)
		}
		if cfg.Job.VerifyMd5 {
			if err := destClient.BatchV1().Jobs(cfg.Destination.Namespace).Delete(ctx, verifyJobName, metav1.DeleteOptions{}); err != nil {
				log.Printf("cleanup verify job failed: %v", err)
			}
			if err := destClient.CoreV1().Secrets(cfg.Destination.Namespace).Delete(ctx, kube.SecretName(verifyJobName), metav1.DeleteOptions{}); err != nil {
				log.Printf("cleanup verify secret failed: %v", err)
			}
		}
		if !cfg.Job.KeepIntermediateObject {
			if err := s3Client.DeleteObject(ctx, cfg.S3.Bucket, cfg.S3.ObjectKey); err != nil {
				log.Printf("failed deleting S3 object: %v", err)
			} else {
				_ = s3Client.WaitForDeletion(ctx, cfg.S3.Bucket, cfg.S3.ObjectKey)
			}
		}
	}

	if cfg.RBAC.AutoCreate && cfg.RBAC.Cleanup != nil && *cfg.RBAC.Cleanup {
		if err := cleanupRBAC(ctx, sourceClient, srcRBAC); err != nil {
			log.Printf("cleanup source RBAC failed: %v", err)
		}
		if err := cleanupRBAC(ctx, destClient, dstRBAC); err != nil {
			log.Printf("cleanup destination RBAC failed: %v", err)
		}
	}

	return nil
}

func ensurePVC(ctx context.Context, client kubernetes.Interface, ns, name string) error {
	_, err := client.CoreV1().PersistentVolumeClaims(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("pvc %s/%s not found: %w", ns, name, err)
	}
	return nil
}

func ensureServiceAccount(ctx context.Context, client kubernetes.Interface, ns, name string) error {
	_, err := client.CoreV1().ServiceAccounts(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("serviceaccount %s/%s not found: %w", ns, name, err)
	}
	return nil
}

// resolveServiceAccount accepts either "name" or "namespace/name" and returns the name scoped to the provided namespace.
func resolveServiceAccount(sa, ns string) (string, error) {
	if sa == "" {
		return "", errors.New("serviceAccount is required")
	}
	parts := strings.Split(sa, "/")
	if len(parts) == 1 {
		return sa, nil
	}
	if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
		if parts[0] != ns {
			return "", fmt.Errorf("serviceAccount namespace %q must match job namespace %q", parts[0], ns)
		}
		return parts[1], nil
	}
	return "", fmt.Errorf("serviceAccount %q must be in form name or namespace/name", sa)
}

type rbacResources struct {
	namespace             string
	serviceAccount        string
	clusterRole           string
	clusterRoleBinding    string
	createdServiceAccount bool
	createdRole           bool
	createdBinding        bool
}

func ensureRBAC(ctx context.Context, client kubernetes.Interface, ns, sa string) (*rbacResources, error) {
	res := &rbacResources{namespace: ns, serviceAccount: sa}

	if _, err := client.CoreV1().ServiceAccounts(ns).Get(ctx, sa, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			_, createErr := client.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      sa,
					Namespace: ns,
				},
			}, metav1.CreateOptions{})
			if createErr != nil {
				return nil, fmt.Errorf("create serviceaccount %s/%s: %w", ns, sa, createErr)
			}
			res.createdServiceAccount = true
		} else {
			return nil, fmt.Errorf("get serviceaccount %s/%s: %w", ns, sa, err)
		}
	}

	roleName := fmt.Sprintf("pvc-transfer-%s-role", ns)
	res.clusterRole = roleName
	if _, err := client.RbacV1().ClusterRoles().Get(ctx, roleName, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			_, createErr := client.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
				ObjectMeta: metav1.ObjectMeta{Name: roleName},
				Rules: []rbacv1.PolicyRule{
					{APIGroups: []string{""}, Resources: []string{"pods", "pods/log", "persistentvolumeclaims"}, Verbs: []string{"get", "list", "watch", "create", "delete"}},
					{APIGroups: []string{"batch"}, Resources: []string{"jobs"}, Verbs: []string{"get", "list", "watch", "create", "delete"}},
				},
			}, metav1.CreateOptions{})
			if createErr != nil {
				return nil, fmt.Errorf("create clusterrole %s: %w", roleName, createErr)
			}
			res.createdRole = true
		} else {
			return nil, fmt.Errorf("get clusterrole %s: %w", roleName, err)
		}
	}

	bindingName := fmt.Sprintf("pvc-transfer-%s-binding", ns)
	res.clusterRoleBinding = bindingName
	if _, err := client.RbacV1().ClusterRoleBindings().Get(ctx, bindingName, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			_, createErr := client.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: bindingName},
				RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: roleName},
				Subjects: []rbacv1.Subject{
					{Kind: "ServiceAccount", Name: sa, Namespace: ns},
				},
			}, metav1.CreateOptions{})
			if createErr != nil {
				return nil, fmt.Errorf("create clusterrolebinding %s: %w", bindingName, createErr)
			}
			res.createdBinding = true
		} else {
			return nil, fmt.Errorf("get clusterrolebinding %s: %w", bindingName, err)
		}
	}

	return res, nil
}

func cleanupRBAC(ctx context.Context, client kubernetes.Interface, res *rbacResources) error {
	if res == nil {
		return nil
	}
	var errs []string
	if res.createdBinding {
		if err := client.RbacV1().ClusterRoleBindings().Delete(ctx, res.clusterRoleBinding, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Sprintf("delete binding %s: %v", res.clusterRoleBinding, err))
		}
	}
	if res.createdRole {
		if err := client.RbacV1().ClusterRoles().Delete(ctx, res.clusterRole, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Sprintf("delete role %s: %v", res.clusterRole, err))
		}
	}
	if res.createdServiceAccount {
		if err := client.CoreV1().ServiceAccounts(res.namespace).Delete(ctx, res.serviceAccount, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Sprintf("delete serviceaccount %s/%s: %v", res.namespace, res.serviceAccount, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("rbac cleanup errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func prepareSecret(ctx context.Context, client kubernetes.Interface, ns, name string, cfg config.Config) error {
	data := map[string][]byte{
		"accessKey": []byte(cfg.S3.AccessKey),
		"secretKey": []byte(cfg.S3.SecretKey),
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	_, err := client.CoreV1().Secrets(ns).Create(ctx, secret, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	// On conflict, replace to keep credentials current.
	_, err = client.CoreV1().Secrets(ns).Update(ctx, secret, metav1.UpdateOptions{})
	return err
}

func applyJob(ctx context.Context, client kubernetes.Interface, job *batchv1.Job) error {
	_, err := client.BatchV1().Jobs(job.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	// Replace existing job to allow reruns.
	if apierrors.IsAlreadyExists(err) {
		if delErr := client.BatchV1().Jobs(job.Namespace).Delete(ctx, job.Name, metav1.DeleteOptions{PropagationPolicy: propagationPtr(metav1.DeletePropagationBackground)}); delErr != nil {
			return fmt.Errorf("failed deleting stale job %s: %w", job.Name, delErr)
		}
		// wait a moment for deletion to propagate
		time.Sleep(2 * time.Second)
		_, createErr := client.BatchV1().Jobs(job.Namespace).Create(ctx, job, metav1.CreateOptions{})
		return createErr
	}
	return err
}

func monitorJob(ctx context.Context, client kubernetes.Interface, job *batchv1.Job, phase string) error {
	podName, err := waitForPod(ctx, client, job.Namespace, job.Name)
	if err != nil {
		return err
	}
	log.Printf("%s job pod %s running - streaming logs", phase, podName)
	stopLogs := make(chan struct{})
	go streamLogs(ctx, client, job.Namespace, podName, stopLogs)

	defer close(stopLogs)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
			status, err := client.BatchV1().Jobs(job.Namespace).Get(ctx, job.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get job status: %w", err)
			}
			if term, err := workerTermination(ctx, client, job.Namespace, job.Name, workerContainerName); err == nil && term != nil {
				if term.ExitCode == 0 {
					log.Printf("%s job %s container %s completed", phase, job.Name, workerContainerName)
					return nil
				}
				return fmt.Errorf("%s job %s failed: container %s exit %d (%s)", phase, job.Name, workerContainerName, term.ExitCode, term.Reason)
			} else if err != nil {
				log.Printf("warning: checking worker termination for job %s: %v", job.Name, err)
			}
			if status.Status.Succeeded > 0 {
				log.Printf("%s job %s succeeded", phase, job.Name)
				return nil
			}
			if status.Status.Failed > 0 && status.Spec.BackoffLimit != nil && status.Status.Failed > *status.Spec.BackoffLimit {
				return fmt.Errorf("%s job %s failed", phase, job.Name)
			}
		}
	}
}

func waitForPod(ctx context.Context, client kubernetes.Interface, ns, jobName string) (string, error) {
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			podList, err := client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("job-name=%s", jobName)})
			if err != nil {
				return "", err
			}
			for _, p := range podList.Items {
				if p.Status.Phase == corev1.PodRunning || p.Status.Phase == corev1.PodPending || p.Status.Phase == corev1.PodSucceeded {
					return p.Name, nil
				}
			}
			time.Sleep(2 * time.Second)
		}
	}
}

func streamLogs(ctx context.Context, client kubernetes.Interface, ns, pod string, stop <-chan struct{}) {
	tail := int64(20)
	container := "worker"
	opts := &corev1.PodLogOptions{Follow: true, TailLines: &tail, Container: container}

	stopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		req := client.CoreV1().Pods(ns).GetLogs(pod, opts)
		stream, err := req.Stream(stopCtx)
		if err != nil {
			if c, ok := containerFromError(err, container); ok {
				container = c
				opts.Container = container
				log.Printf("log stream error for pod %s: %v; selecting container %s", pod, err, container)
				continue
			}
			log.Printf("log stream error for pod %s: %v; retrying in 1s", pod, err)
			if waitOrStop(stopCtx, 1*time.Second) {
				return
			}
			continue
		}
		opts.TailLines = nil // avoid replaying history on subsequent retries
		reader := bufio.NewScanner(stream)
		reader.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
		for {
			if reader.Scan() {
				select {
				case <-stopCtx.Done():
					stream.Close()
					return
				default:
				}
				log.Printf("[%s] %s", pod, reader.Text())
				continue
			}
			stream.Close()
			if err := reader.Err(); err != nil && !errors.Is(err, io.EOF) {
				log.Printf("log stream ended for pod %s: %v", pod, err)
			}

			done, statusErr := containerDone(stopCtx, client, ns, pod, container)
			if statusErr != nil {
				log.Printf("log stream status check failed for pod %s: %v", pod, statusErr)
			}
			if done {
				return
			}

			break
		}

		if waitOrStop(stopCtx, 1*time.Second) {
			return
		}
	}
}

func waitOrStop(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(d):
		return false
	}
}

func containerFromError(err error, current string) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := err.Error()
	if !strings.Contains(msg, "choose one of:") {
		return "", false
	}
	start := strings.Index(msg, "[")
	end := strings.Index(msg, "]")
	if start == -1 || end == -1 || end <= start {
		return "", false
	}
	trimmed := strings.TrimSpace(msg[start+1 : end])
	if trimmed == "" {
		return "", false
	}
	parts := strings.Fields(trimmed)
	if len(parts) == 0 {
		return "", false
	}
	for _, p := range parts {
		if p == current {
			return current, true
		}
	}
	for _, preferred := range []string{"worker", "main"} {
		for _, p := range parts {
			if p == preferred {
				return p, true
			}
		}
	}
	return parts[0], true
}

func containerDone(ctx context.Context, client kubernetes.Interface, ns, pod, container string) (bool, error) {
	p, err := client.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return true, nil
	}
	for _, st := range append(p.Status.InitContainerStatuses, p.Status.ContainerStatuses...) {
		if st.Name != container {
			continue
		}
		if st.State.Terminated != nil {
			return true, nil
		}
	}
	return false, nil
}

func workerTermination(ctx context.Context, client kubernetes.Interface, ns, jobName, container string) (*corev1.ContainerStateTerminated, error) {
	podList, err := client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("job-name=%s", jobName)})
	if err != nil {
		return nil, err
	}
	for _, p := range podList.Items {
		for _, st := range append(p.Status.InitContainerStatuses, p.Status.ContainerStatuses...) {
			if st.Name != container {
				continue
			}
			if st.State.Terminated != nil {
				return st.State.Terminated, nil
			}
		}
	}
	return nil, nil
}

func cleanupJob(ctx context.Context, client kubernetes.Interface, job *batchv1.Job) error {
	policy := metav1.DeletePropagationBackground
	if err := client.BatchV1().Jobs(job.Namespace).Delete(ctx, job.Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
		return err
	}
	if err := client.CoreV1().Secrets(job.Namespace).Delete(ctx, kube.SecretName(job.Name), metav1.DeleteOptions{}); err != nil {
		return err
	}
	return nil
}

func retry(ctx context.Context, delay time.Duration, attempts int, f func() error) error {
	var last error
	for i := 0; i < attempts; i++ {
		if err := f(); err != nil {
			last = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				continue
			}
		} else {
			return nil
		}
	}
	return last
}

func propagationPtr(pol metav1.DeletionPropagation) *metav1.DeletionPropagation {
	return &pol
}

func int64Ptr(i int64) *int64 { return &i }
