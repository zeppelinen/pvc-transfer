package kube

import (
	"fmt"
	"strings"

	"github.com/zeppelinen/pvc-transfer/internal/config"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BuildExportJob defines the Job spec that streams PVC contents to S3.
func BuildExportJob(cfg config.Config, name string, ns string) *batchv1.Job {
	return buildJob(cfg, name, ns, cfg.Source.PVCs, true, exportCommand(cfg))
}

// BuildImportJob defines the Job spec that restores PVC contents from S3.
func BuildImportJob(cfg config.Config, name string, ns string) *batchv1.Job {
	return buildJob(cfg, name, ns, cfg.Destination.PVCs, false, importCommand(cfg))
}

// BuildVerifyJob creates a verification job to compare MD5 hashes between S3 object and destination PVC.
func BuildVerifyJob(cfg config.Config, name string, ns string) *batchv1.Job {
	cmds := []string{"apk add --no-cache aws-cli tar gzip mbuffer >/tmp/setup.log"}
	for i, p := range cfg.Destination.PVCs {
		key := cfg.GetObjectKey(i)
		tarPaths := fmt.Sprintf("-C %s .", p.MountPath)
		verifyCmd := fmt.Sprintf(`dest_%d=$(tar -cvzf - %[2]s | md5sum | awk '{print $1}') && `+
			`s3_%d=$(aws s3 cp s3://%[4]s/%[5]s - | md5sum | awk '{print $1}') && `+
			`echo "DEST_MD5_%d=$dest_%d" && echo "S3_MD5_%d=$s3_%d" && `+
			`if [ "$dest_%d" = "$s3_%d" ]; then echo "MD5_MATCH_%d"; else echo "MD5_MISMATCH_%d"; exit 1; fi`,
			i, tarPaths, i, cfg.S3.Bucket, key, i, i, i, i, i, i, i, i)
		cmds = append(cmds, verifyCmd)
	}

	command := []string{
		"/bin/sh", "-c",
		strings.Join(cmds, " &&\n"),
	}
	return buildJob(cfg, name, ns, cfg.Destination.PVCs, false, command)
}

func buildJob(cfg config.Config, name, ns string, pvcs []config.PVCConfig, readOnly bool, command []string) *batchv1.Job {
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

	var mounts []corev1.VolumeMount
	var vols []corev1.Volume
	for i, pvc := range pvcs {
		volName := fmt.Sprintf("data-%d", i)
		mounts = append(mounts, corev1.VolumeMount{
			Name:      volName,
			MountPath: pvc.MountPath,
			ReadOnly:  readOnly,
		})
		vols = append(vols, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.Name,
					ReadOnly:  readOnly,
				},
			},
		})
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
							Name:         "worker",
							Image:        cfg.Job.Image,
							Command:      command,
							Env:          env,
							VolumeMounts: mounts,
						},
					},
					Volumes: vols,
				},
			},
		},
	}
}

func exportCommand(cfg config.Config) []string {
	cmds := []string{"apk add --no-cache aws-cli tar gzip mbuffer >/tmp/setup.log"}
	for i, p := range cfg.Source.PVCs {
		key := cfg.GetObjectKey(i)
		tarCmd := fmt.Sprintf(`tar -cvzf - -C %s . | mbuffer -m 128M | aws s3 cp - s3://%s/%s`, p.MountPath, cfg.S3.Bucket, key)
		cmds = append(cmds, tarCmd)
	}
	return []string{"/bin/sh", "-c", strings.Join(cmds, " &&\n")}
}

func importCommand(cfg config.Config) []string {
	cmds := []string{"apk add --no-cache aws-cli tar gzip mbuffer >/tmp/setup.log"}
	for i, p := range cfg.Destination.PVCs {
		key := cfg.GetObjectKey(i)
		extractCmd := fmt.Sprintf(`aws s3 cp s3://%s/%s - | mbuffer -m 128M | tar -xvzf - -C %s`, cfg.S3.Bucket, key, p.MountPath)
		cmds = append(cmds, extractCmd)
	}
	return []string{"/bin/sh", "-c", strings.Join(cmds, " &&\n")}
}

// SecretName returns a deterministic secret name per job.
func SecretName(job string) string {
	return fmt.Sprintf("%s-s3-credentials", job)
}
