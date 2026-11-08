package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestRunCLIReportsUsage(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{nil, {"one", "two"}, {"--unknown"}} {
		var stdout, stderr bytes.Buffer
		if code := runCLI(context.Background(), args, &stdout, &stderr); code != 2 {
			t.Fatalf("runCLI(%v) code = %d, want 2", args, code)
		}
		if stdout.Len() != 0 || stderr.Len() == 0 {
			t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
		}
	}
}

func TestRunCLIReportsStructuredLoadError(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := runCLI(context.Background(), []string{filepath.Join(t.TempDir(), "missing.yaml")}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("runCLI() code = %d, want 1", code)
	}
	var envelope map[string]string
	if err := json.Unmarshal(stderr.Bytes(), &envelope); err != nil || envelope["error"] == "" {
		t.Fatalf("stderr = %q, error = %v", stderr.String(), err)
	}
}
