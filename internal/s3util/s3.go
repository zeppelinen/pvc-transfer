package s3util

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	cfg "github.com/zeppelinen/pvc-transfer/internal/config"
)

// Client wraps the AWS S3 client and config.
type Client struct {
	Inner *s3.Client
}

// New creates an S3 client honoring custom endpoints and credentials.
func New(ctx context.Context, c cfg.Config) (*Client, error) {
	resolver := aws.EndpointResolverWithOptions(nil)
	if c.S3.Endpoint != "" {
		resolver = aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:               c.S3.Endpoint,
					HostnameImmutable: true,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
	}
	logMode := aws.ClientLogMode(0)
	var logger logging.Logger = logging.Nop{}
	if strings.EqualFold(c.LogLevel, "debug") {
		logMode = aws.LogRequest | aws.LogResponse
		logger = logging.LoggerFunc(func(classification logging.Classification, format string, args ...interface{}) {
			if classification != "" {
				format = string(classification) + " " + format
			}
			log.Printf(format, args...)
		})
	}
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(c.S3.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.S3.AccessKey, c.S3.SecretKey, "")),
		awsconfig.WithEndpointResolverWithOptions(resolver),
		awsconfig.WithLogger(logger),
	}
	if logMode != 0 {
		opts = append(opts, awsconfig.WithClientLogMode(logMode))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{Inner: s3.NewFromConfig(awsCfg)}, nil
}

// VerifyBucket checks that the bucket is reachable.
func (c *Client) VerifyBucket(ctx context.Context, bucket string) error {
	_, err := c.Inner.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucket})
	if err == nil {
		return nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "AccessDenied" || apiErr.ErrorCode() == "Forbidden") {
		// Some providers (e.g. Cloudflare R2 with bucket-scoped tokens) block HeadBucket.
		// Fall back to a minimal write probe to confirm the bucket is reachable with the
		// credentials we actually need for the transfer workflow.
		key := fmt.Sprintf("pvc-transfer-probe-%d", time.Now().UnixNano())
		_, putErr := c.Inner.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(nil),
		})
		if putErr == nil {
			// Best-effort cleanup; ignore errors because the probe is harmless.
			_, _ = c.Inner.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
			return nil
		}
		return fmt.Errorf("head bucket denied and probe write failed: %w", putErr)
	}

	return err
}

// ObjectExists returns true when the object is present in the bucket.
func (c *Client) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := c.Inner.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DeleteObject removes the object (best-effort).
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.Inner.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	return err
}

// WaitForDeletion waits until an object is gone.
func (c *Client) WaitForDeletion(ctx context.Context, bucket, key string) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			exists, err := c.ObjectExists(ctx, bucket, key)
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
		}
	}
}

// BuildObjectURL returns a user friendly URL for logging.
func BuildObjectURL(endpoint, bucket, key string) string {
	trimmed := strings.TrimSuffix(endpoint, "/")
	if trimmed == "" {
		return fmt.Sprintf("s3://%s/%s", bucket, key)
	}
	return fmt.Sprintf("%s/%s/%s", trimmed, bucket, url.PathEscape(key))
}
