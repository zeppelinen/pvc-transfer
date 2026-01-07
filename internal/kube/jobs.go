package kube

import (
	"fmt"

	"github.com/zeppelinen/pvc-transfer/internal/config"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BuildExportJob defines the Job spec that streams PVC contents to S3.
func BuildExportJob(cfg config.Config, name string, ns string) *batchv1.Job {
	return buildJob(cfg, name, ns, cfg.Source.PVCName, cfg.Source.MountPath, true, exportCommand(cfg))
}

// BuildImportJob defines the Job spec that restores PVC contents from S3.
func BuildImportJob(cfg config.Config, name string, ns string) *batchv1.Job {
	return buildJob(cfg, name, ns, cfg.Destination.PVCName, cfg.Destination.MountPath, false, importCommand(cfg))
}

// BuildVerifyJob creates a verification job to compare MD5 hashes between S3 object and destination PVC.
func BuildVerifyJob(cfg config.Config, name string, ns string) *batchv1.Job {
	command := []string{
		"/bin/sh", "-c",
		fmt.Sprintf(`
apk add --no-cache aws-cli tar gzip mbuffer >/tmp/setup.log &&
dest=$(tar -cvzf - -C %[1]s . | md5sum | awk '{print $1}') &&
s3=$(aws s3 cp s3://%[2]s/%[3]s - | md5sum | awk '{print $1}') &&
echo "DEST_MD5=$dest" && echo "S3_MD5=$s3" &&
if [ "$dest" = "$s3" ]; then echo "MD5_MATCH"; else echo "MD5_MISMATCH"; exit 1; fi
`, cfg.Destination.MountPath, cfg.S3.Bucket, cfg.S3.ObjectKey),
	}
	return buildJob(cfg, name, ns, cfg.Destination.PVCName, cfg.Destination.MountPath, false, command)
}

func buildJob(cfg config.Config, name, ns, pvcName, mountPath string, readOnly bool, command []string) *batchv1.Job {
	labels := map[string]string{
		"app":   "pvc-transfer",
		"job":   name,
		"phase": "transfer",
	}
	backoff := cfg.Job.BackoffLimit
	ttl := cfg.Job.TTLSecondsAfterFinish
	env := []corev1.EnvVar{
		{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: SecretName(name)}, Key: "accessKey"}}},
		{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: SecretName(name)}, Key: "secretKey"}}},
		{Name: "AWS_DEFAULT_REGION", Value: cfg.S3.Region},
		{Name: "AWS_EC2_METADATA_DISABLED", Value: "true"},
	}
	endpoint := cfg.S3.Endpoint
	if cfg.S3.JobEndpoint != "" {
		endpoint = cfg.S3.JobEndpoint
	}
	if endpoint != "" {
		env = append(env, corev1.EnvVar{Name: "AWS_ENDPOINT_URL", Value: endpoint})
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: cfg.Job.ServiceAccount,
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "worker",
							Image:   cfg.Job.Image,
							Command: command,
							Env:     env,
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: mountPath, ReadOnly: readOnly},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
									ReadOnly:  readOnly,
								},
							},
						},
					},
				},
			},
		},
	}
}

func exportCommand(cfg config.Config) []string {
	return []string{"/bin/sh", "-c", fmt.Sprintf(`
apk add --no-cache aws-cli tar gzip mbuffer >/tmp/setup.log &&
tar -cvzf - -C %[1]s . | mbuffer -m 128M | aws s3 cp - s3://%[2]s/%[3]s
`, cfg.Source.MountPath, cfg.S3.Bucket, cfg.S3.ObjectKey)}
}

func importCommand(cfg config.Config) []string {
	return []string{"/bin/sh", "-c", fmt.Sprintf(`
apk add --no-cache aws-cli tar gzip mbuffer >/tmp/setup.log &&
aws s3 cp s3://%[2]s/%[3]s - | mbuffer -m 128M | tar -xvzf - -C %[1]s
`, cfg.Destination.MountPath, cfg.S3.Bucket, cfg.S3.ObjectKey)}
}

// SecretName returns a deterministic secret name per job.
func SecretName(job string) string {
	return fmt.Sprintf("%s-s3-credentials", job)
}
