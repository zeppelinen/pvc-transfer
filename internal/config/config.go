package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// Config represents the full PVC transfer configuration.
type Config struct {
	Version      string        `yaml:"version"`
	S3           S3Config      `yaml:"s3"`
	Source       ClusterConfig `yaml:"source"`
	Destination  ClusterConfig `yaml:"destination"`
	Job          JobConfig     `yaml:"job"`
	Cleanup      *bool         `yaml:"cleanup"`
	Overwrite    bool          `yaml:"overwrite"`
	TimeoutMins  int           `yaml:"timeoutMinutes"`
	RetryBackoff RetryBackoff  `yaml:"retryBackoff"`
}

// S3Config holds the bucket connectivity settings.
type S3Config struct {
	Bucket           string `yaml:"bucket"`
	Region           string `yaml:"region"`
	Endpoint         string `yaml:"endpoint"`
	JobEndpoint      string `yaml:"jobEndpoint"`
	AccessKey        string `yaml:"accessKey"`
	SecretKey        string `yaml:"secretKey"`
	ObjectKey        string `yaml:"objectKey"`
	ObjectKeyDerived bool   `yaml:"-"`
}

// PVCConfig defines a PVC name and its mount path.
type PVCConfig struct {
	Name      string `yaml:"name"`
	MountPath string `yaml:"mountPath"`
}

// ClusterConfig defines source/destination cluster inputs.
type ClusterConfig struct {
	ClusterContext string      `yaml:"clusterContext"`
	Namespace      string      `yaml:"namespace"`
	PVCName        string      `yaml:"pvcName"`
	MountPath      string      `yaml:"mountPath"`
	PVCs           []PVCConfig `yaml:"pvcs"`
}

// JobConfig sets job defaults.
type JobConfig struct {
	Image                  string `yaml:"image"`
	ServiceAccount         string `yaml:"serviceAccount"`
	BackoffLimit           int32  `yaml:"backoffLimit"`
	TTLSecondsAfterFinish  int32  `yaml:"ttlSecondsAfterFinished"`
	VerifyMd5              bool   `yaml:"verifyMd5"`
	KeepIntermediateObject bool   `yaml:"keepIntermediateObject"`
}

// RetryBackoff controls retry behavior for transient S3 errors.
type RetryBackoff struct {
	Attempts int `yaml:"attempts"`
	Seconds  int `yaml:"seconds"`
}

// Load reads and decodes YAML config, applying environment overrides.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, err
	}
	cfg.applyEnvOverrides()
	cfg.setDefaults()
	return cfg, nil
}

// Validate ensures required fields are set and well-formed.
func (c Config) Validate() error {
	if c.Version != "v1" {
		return fmt.Errorf("unsupported version %q", c.Version)
	}
	if c.S3.Bucket == "" || c.S3.Region == "" {
		return errors.New("s3.bucket and s3.region are required")
	}
	if c.S3.AccessKey == "" || c.S3.SecretKey == "" {
		return errors.New("s3 accessKey/secretKey are required (or via env overrides)")
	}
	for name, cluster := range map[string]ClusterConfig{"source": c.Source, "destination": c.Destination} {
		if err := validateCluster(cluster); err != nil {
			return fmt.Errorf("%s cluster invalid: %w", name, err)
		}
	}
	if c.Job.Image == "" {
		return errors.New("job.image is required")
	}
	if c.Job.ServiceAccount == "" {
		return errors.New("job.serviceAccount is required")
	}
	return nil
}

func validateCluster(c ClusterConfig) error {
	if c.ClusterContext == "" || c.Namespace == "" {
		return errors.New("clusterContext and namespace are required")
	}
	if c.PVCName == "" && len(c.PVCs) == 0 {
		return errors.New("either pvcName/mountPath or pvcs list is required")
	}
	if c.PVCName != "" && c.MountPath == "" {
		return errors.New("mountPath is required when pvcName is specified")
	}

	pvcs := c.PVCs
	if len(pvcs) == 0 {
		pvcs = []PVCConfig{{Name: c.PVCName, MountPath: c.MountPath}}
	}

	validK8sName := regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	if !validK8sName.MatchString(c.Namespace) {
		return fmt.Errorf("namespace %q is invalid", c.Namespace)
	}
	for _, p := range pvcs {
		if !validK8sName.MatchString(p.Name) {
			return fmt.Errorf("pvcName %q is invalid", p.Name)
		}
		if p.MountPath == "" {
			return fmt.Errorf("mountPath is required for pvc %q", p.Name)
		}
	}
	return nil
}

func (c *Config) applyEnvOverrides() {
	access := os.Getenv("PVC_TRANSFER_S3_ACCESS_KEY")
	secret := os.Getenv("PVC_TRANSFER_S3_SECRET_KEY")
	endpoint := os.Getenv("PVC_TRANSFER_S3_ENDPOINT")
	if access != "" {
		c.S3.AccessKey = access
	}
	if secret != "" {
		c.S3.SecretKey = secret
	}
	if endpoint != "" {
		c.S3.Endpoint = endpoint
	}
}

func (c *Config) setDefaults() {
	if c.Job.BackoffLimit == 0 {
		c.Job.BackoffLimit = 0
	}
	if c.Job.TTLSecondsAfterFinish == 0 {
		c.Job.TTLSecondsAfterFinish = 3600
	}
	if c.TimeoutMins == 0 {
		c.TimeoutMins = 60
	}
	if c.RetryBackoff.Attempts == 0 {
		c.RetryBackoff.Attempts = 3
	}
	if c.RetryBackoff.Seconds == 0 {
		c.RetryBackoff.Seconds = 5
	}
	if c.Cleanup == nil {
		def := true
		c.Cleanup = &def
	}
	if c.Job.ServiceAccount == "" {
		c.Job.ServiceAccount = "pvc-transfer-sa"
	}
	
	if len(c.Source.PVCs) == 0 && c.Source.PVCName != "" {
		c.Source.PVCs = []PVCConfig{{Name: c.Source.PVCName, MountPath: c.Source.MountPath}}
	}
	if len(c.Destination.PVCs) == 0 && c.Destination.PVCName != "" {
		c.Destination.PVCs = []PVCConfig{{Name: c.Destination.PVCName, MountPath: c.Destination.MountPath}}
	}

	if c.S3.ObjectKey == "" {
		firstPVC := "unknown"
		if len(c.Source.PVCs) > 0 {
			firstPVC = c.Source.PVCs[0].Name
		}
		c.S3.ObjectKey = DefaultObjectKey(c.Source.Namespace, firstPVC)
		if len(c.Source.PVCs) > 1 {
			c.S3.ObjectKey = DefaultObjectKey(c.Source.Namespace, "multi-"+firstPVC)
		}
		c.S3.ObjectKeyDerived = true
	} else {
		c.S3.ObjectKeyDerived = false
	}
}

// DefaultObjectKey builds a deterministic object key based on namespace and PVC.
func DefaultObjectKey(namespace, pvc string) string {
	return fmt.Sprintf("migrations/%s-%s.tar.gz", namespace, pvc)
}
