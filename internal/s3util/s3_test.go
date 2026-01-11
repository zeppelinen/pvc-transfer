package s3util

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
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

// mockS3Client is a mock implementation for testing
type mockS3Client struct {
	headBucketFunc   func(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	headObjectFunc   func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	putObjectFunc    func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	deleteObjectFunc func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

func (m *mockS3Client) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	if m.headBucketFunc != nil {
		return m.headBucketFunc(ctx, params, optFns...)
	}
	return &s3.HeadBucketOutput{}, nil
}

func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.headObjectFunc != nil {
		return m.headObjectFunc(ctx, params, optFns...)
	}
	return &s3.HeadObjectOutput{}, nil
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putObjectFunc != nil {
		return m.putObjectFunc(ctx, params, optFns...)
	}
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.deleteObjectFunc != nil {
		return m.deleteObjectFunc(ctx, params, optFns...)
	}
	return &s3.DeleteObjectOutput{}, nil
}

// apiError implements smithy.APIError for testing
type apiError struct {
	code    string
	message string
}

func (e *apiError) Error() string {
	return e.message
}

func (e *apiError) ErrorCode() string {
	return e.code
}

func (e *apiError) ErrorMessage() string {
	return e.message
}

func (e *apiError) ErrorFault() smithy.ErrorFault {
	return smithy.FaultUnknown
}

func TestVerifyBucket_Success(t *testing.T) {
	mock := &mockS3Client{
		headBucketFunc: func(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
			return &s3.HeadBucketOutput{}, nil
		},
	}
	
	client := &Client{Inner: mock}
	err := client.VerifyBucket(context.Background(), "test-bucket")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestVerifyBucket_HeadBucketFailsWithFallbackSuccess(t *testing.T) {
	putObjectCalled := false
	deleteObjectCalled := false
	
	mock := &mockS3Client{
		headBucketFunc: func(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
			// Simulate generic error that triggers fallback
			return nil, errors.New("some error")
		},
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			// Verify the probe key format
			if params.Key == nil || !contains(*params.Key, "pvc-transfer-probe-") {
				t.Errorf("expected probe key to contain 'pvc-transfer-probe-', got: %v", params.Key)
			}
			// Verify body is provided
			if params.Body == nil {
				t.Error("expected Body to be provided")
			} else {
				// Verify empty body
				data, err := io.ReadAll(params.Body)
				if err != nil {
					t.Errorf("failed to read body: %v", err)
				}
				if len(data) != 0 {
					t.Errorf("expected empty body, got %d bytes", len(data))
				}
			}
			return &s3.PutObjectOutput{}, nil
		},
		deleteObjectFunc: func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteObjectCalled = true
			return &s3.DeleteObjectOutput{}, nil
		},
	}
	
	client := &Client{Inner: mock}
	err := client.VerifyBucket(context.Background(), "test-bucket")
	if err != nil {
		t.Fatalf("expected no error with fallback, got: %v", err)
	}
	if !putObjectCalled {
		t.Error("expected PutObject to be called for fallback")
	}
	if !deleteObjectCalled {
		t.Error("expected DeleteObject to be called for cleanup")
	}
}

func TestVerifyBucket_BothHeadBucketAndFallbackFail(t *testing.T) {
	headBucketErr := errors.New("head bucket error")
	putObjectErr := errors.New("put object error")
	
	mock := &mockS3Client{
		headBucketFunc: func(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
			return nil, headBucketErr
		},
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, putObjectErr
		},
	}
	
	client := &Client{Inner: mock}
	err := client.VerifyBucket(context.Background(), "test-bucket")
	if err == nil {
		t.Fatal("expected error when both HeadBucket and fallback fail")
	}
	// Verify error message contains both errors
	errMsg := err.Error()
	if !contains(errMsg, "head bucket failed") {
		t.Errorf("expected error to mention head bucket failure, got: %s", errMsg)
	}
	if !contains(errMsg, "probe write failed") {
		t.Errorf("expected error to mention probe write failure, got: %s", errMsg)
	}
}

func TestVerifyBucket_FallbackWithAccessDenied(t *testing.T) {
	mock := &mockS3Client{
		headBucketFunc: func(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
			return nil, errors.New("some error")
		},
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, &apiError{code: "AccessDenied", message: "Access Denied"}
		},
	}
	
	client := &Client{Inner: mock}
	err := client.VerifyBucket(context.Background(), "test-bucket")
	if err == nil {
		t.Fatal("expected error when fallback returns AccessDenied")
	}
	errMsg := err.Error()
	if !contains(errMsg, "write probe denied") {
		t.Errorf("expected error to mention write probe denied, got: %s", errMsg)
	}
}

func TestProbeWrite_Success(t *testing.T) {
	putObjectCalled := false
	deleteObjectCalled := false
	
	mock := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			if params.Bucket == nil || *params.Bucket != "test-bucket" {
				t.Errorf("expected bucket 'test-bucket', got: %v", params.Bucket)
			}
			return &s3.PutObjectOutput{}, nil
		},
		deleteObjectFunc: func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteObjectCalled = true
			return &s3.DeleteObjectOutput{}, nil
		},
	}
	
	client := &Client{Inner: mock}
	err := client.probeWrite(context.Background(), "test-bucket")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if !putObjectCalled {
		t.Error("expected PutObject to be called")
	}
	if !deleteObjectCalled {
		t.Error("expected DeleteObject to be called")
	}
}

func TestProbeWrite_PutObjectFails(t *testing.T) {
	putErr := errors.New("put failed")
	
	mock := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, putErr
		},
	}
	
	client := &Client{Inner: mock}
	err := client.probeWrite(context.Background(), "test-bucket")
	if err == nil {
		t.Fatal("expected error when PutObject fails")
	}
	if !errors.Is(err, putErr) {
		t.Errorf("expected error to wrap putErr, got: %v", err)
	}
}

func TestProbeWrite_AccessDeniedError(t *testing.T) {
	accessDeniedErr := &apiError{code: "AccessDenied", message: "Access Denied"}
	
	mock := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, accessDeniedErr
		},
	}
	
	client := &Client{Inner: mock}
	err := client.probeWrite(context.Background(), "test-bucket")
	if err == nil {
		t.Fatal("expected error when PutObject returns AccessDenied")
	}
	errMsg := err.Error()
	if !contains(errMsg, "write probe denied") {
		t.Errorf("expected error to mention write probe denied, got: %s", errMsg)
	}
}

func TestProbeWrite_DeleteObjectFailsButIgnored(t *testing.T) {
	deleteErr := errors.New("delete failed")
	
	mock := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return &s3.PutObjectOutput{}, nil
		},
		deleteObjectFunc: func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return nil, deleteErr
		},
	}
	
	client := &Client{Inner: mock}
	err := client.probeWrite(context.Background(), "test-bucket")
	// Delete error should be ignored (best-effort cleanup)
	if err != nil {
		t.Fatalf("expected no error when delete fails (best-effort), got: %v", err)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || indexOfSubstring(s, substr) >= 0)
}

func indexOfSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
