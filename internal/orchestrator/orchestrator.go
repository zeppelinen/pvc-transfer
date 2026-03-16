package orchestrator

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/zeppelinen/pvc-transfer/internal/config"
	"github.com/zeppelinen/pvc-transfer/internal/kube"
	"github.com/zeppelinen/pvc-transfer/internal/s3util"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Orchestrator manages the end-to-end transfer workflow.
type Orchestrator struct{}

// New constructs an orchestrator instance.
func New() *Orchestrator {
	return &Orchestrator{}
}

// RunOptions controls which phases of the transfer are executed.
type RunOptions struct {
	ExportOnly bool
	ImportOnly bool
}

func resolvePhases(opts RunOptions) (bool, bool, error) {
	if opts.ExportOnly && opts.ImportOnly {
		return false, false, fmt.Errorf("export-only and import-only cannot both be set")
	}
	return !opts.ImportOnly, !opts.ExportOnly, nil
}

// Run executes the transfer per configuration.
func (o *Orchestrator) Run(ctx context.Context, cfg config.Config, opts RunOptions) error {
	shouldExport, shouldImport, err := resolvePhases(opts)
	if err != nil {
		return err
	}

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

	exists, err := s3Client.ObjectExists(ctx, cfg.S3.Bucket, cfg.S3.ObjectKey)
	if err != nil {
		return fmt.Errorf("check object existence: %w", err)
	}
	if shouldExport && exists && !cfg.Overwrite {
		return fmt.Errorf("object s3://%s/%s already exists; rerun with --overwrite or overwrite: true", cfg.S3.Bucket, cfg.S3.ObjectKey)
	}

	if !shouldExport && !exists {
		return fmt.Errorf("object s3://%s/%s not found; run export first or provide an existing archive", cfg.S3.Bucket, cfg.S3.ObjectKey)
	}

	var sourceClient, destClient kubernetes.Interface
	if shouldExport {
		sourceClient, err = kube.NewClientset(cfg.Source.ClusterContext)
		if err != nil {
			return err
		}
		for _, pvc := range cfg.Source.PVCs {
			if err := ensurePVC(ctx, sourceClient, cfg.Source.Namespace, pvc.Name); err != nil {
				return err
			}
		}
		if err := ensureServiceAccount(ctx, sourceClient, cfg.Source.Namespace, cfg.Job.ServiceAccount); err != nil {
			return err
		}
	}
	if shouldImport {
		destClient, err = kube.NewClientset(cfg.Destination.ClusterContext)
		if err != nil {
			return err
		}
		for _, pvc := range cfg.Destination.PVCs {
			if err := ensurePVC(ctx, destClient, cfg.Destination.Namespace, pvc.Name); err != nil {
				return err
			}
		}
		if err := ensureServiceAccount(ctx, destClient, cfg.Destination.Namespace, cfg.Job.ServiceAccount); err != nil {
			return err
		}
	}

	exportJobName := "pvc-transfer-export"
	importJobName := "pvc-transfer-import"
	verifyJobName := "pvc-transfer-verify"

	var exportJob *batchv1.Job
	if shouldExport {
		if err := prepareSecret(ctx, sourceClient, cfg.Source.Namespace, kube.SecretName(exportJobName), cfg); err != nil {
			return err
		}

		exportJob = kube.BuildExportJob(cfg, exportJobName, cfg.Source.Namespace)
		if err := applyJob(ctx, sourceClient, exportJob); err != nil {
			return fmt.Errorf("create export job: %w", err)
		}
		log.Printf("started export job %s in %s/%s", exportJobName, cfg.Source.ClusterContext, cfg.Source.Namespace)
		if err := monitorJob(ctx, sourceClient, exportJob, "export"); err != nil {
			return err
		}
	}

	var importJob *batchv1.Job
	if shouldImport {
		if err := prepareSecret(ctx, destClient, cfg.Destination.Namespace, kube.SecretName(importJobName), cfg); err != nil {
			return err
		}

		importJob = kube.BuildImportJob(cfg, importJobName, cfg.Destination.Namespace)
		if err := applyJob(ctx, destClient, importJob); err != nil {
			return fmt.Errorf("create import job: %w", err)
		}
		log.Printf("started import job %s in %s/%s", importJobName, cfg.Destination.ClusterContext, cfg.Destination.Namespace)
		if err := monitorJob(ctx, destClient, importJob, "import"); err != nil {
			return err
		}
	}

	var verifyJob *batchv1.Job
	if shouldImport && cfg.Job.VerifyMd5 {
		verifyJob = kube.BuildVerifyJob(cfg, verifyJobName, cfg.Destination.Namespace)
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
		if shouldExport && exportJob != nil {
			if err := cleanupJob(ctx, sourceClient, exportJob); err != nil {
				log.Printf("cleanup export job failed: %v", err)
			}
		}
		if shouldImport && importJob != nil {
			if err := cleanupJob(ctx, destClient, importJob); err != nil {
				log.Printf("cleanup import job failed: %v", err)
			}
		}
		if shouldImport && cfg.Job.VerifyMd5 && verifyJob != nil {
			if err := destClient.BatchV1().Jobs(cfg.Destination.Namespace).Delete(ctx, verifyJobName, metav1.DeleteOptions{}); err != nil {
				log.Printf("cleanup verify job failed: %v", err)
			}
			if err := destClient.CoreV1().Secrets(cfg.Destination.Namespace).Delete(ctx, kube.SecretName(verifyJobName), metav1.DeleteOptions{}); err != nil {
				log.Printf("cleanup verify secret failed: %v", err)
			}
		}
		if shouldImport && !cfg.Job.KeepIntermediateObject {
			if err := s3Client.DeleteObject(ctx, cfg.S3.Bucket, cfg.S3.ObjectKey); err != nil {
				log.Printf("failed deleting S3 object: %v", err)
			} else {
				_ = s3Client.WaitForDeletion(ctx, cfg.S3.Bucket, cfg.S3.ObjectKey)
			}
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
	req := client.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Follow: true, TailLines: int64Ptr(20)})
	stream, err := req.Stream(ctx)
	if err != nil {
		log.Printf("log stream error for pod %s: %v", pod, err)
		return
	}
	defer stream.Close()
	reader := bufio.NewReader(stream)
	for {
		select {
		case <-stop:
			return
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					log.Printf("log stream ended: %v", err)
				}
				return
			}
			log.Printf("[%s] %s", pod, line)
		}
	}
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
