package s3util

import "testing"

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
