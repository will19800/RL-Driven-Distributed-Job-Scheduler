package inference

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/policyv1"
)

// The Go test binary doubles as a controlled worker for transport faults.
func TestWorkerHelper(t *testing.T) {
	mode := os.Getenv("INFERENCE_HELPER")
	if mode == "" {
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		switch mode {
		case "hang":
			time.Sleep(10 * time.Second)
		case "malformed":
			fmt.Println(`{"unexpected":1}`)
		case "exit":
			os.Exit(2)
		default:
			fmt.Println(`{"logits":[0,1,0]}`)
		}
	}
	os.Exit(0)
}
func helperProcess(t *testing.T, mode string) *Process {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestWorkerHelper$")
	command.Env = append(os.Environ(), "INFERENCE_HELPER="+mode)
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	p := &Process{command: command, input: input, scanner: bufio.NewScanner(output)}
	t.Cleanup(func() { _ = p.Close() })
	return p
}
func TestProcessPersistentRequestsAndTimeoutCleanup(t *testing.T) {
	observation := policyv1.Observation{ActionMask: []bool{false, true, true}}
	p := helperProcess(t, "healthy")
	for range 3 {
		logits, err := p.Predict(context.Background(), observation)
		if err != nil || len(logits) != 3 {
			t.Fatalf("logits %v, %v", logits, err)
		}
	}
	for _, mode := range []string{"hang", "malformed", "exit"} {
		t.Run(mode, func(t *testing.T) {
			p := helperProcess(t, mode)
			timeout := time.Second
			if mode == "hang" {
				timeout = 50 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			_, err := p.Predict(ctx, observation)
			if err == nil {
				t.Fatal("expected transport error")
			}
			if mode == "hang" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error %v", err)
			}
			if !p.closed || p.command.ProcessState == nil {
				t.Fatal("worker was not reaped")
			}
			if _, err := p.Predict(context.Background(), observation); err == nil {
				t.Fatal("closed worker accepted request")
			}
		})
	}
}

func TestStartRejectsMetadataBeforeLaunching(t *testing.T) {
	directory := t.TempDir()
	config := testConfig()
	contract, _ := config.Schema()
	metadata := Metadata{Format: "inference/v999", Runtime: "torchscript", Schema: contract}
	data, _ := json.Marshal(metadata)
	if err := os.WriteFile(filepath.Join(directory, "metadata.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(context.Background(), "must-not-execute", directory); err == nil {
		t.Fatal("incompatible schema accepted")
	}
	metadata.Format = "inference/v1"
	data, _ = json.Marshal(metadata)
	if err := os.WriteFile(filepath.Join(directory, "metadata.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(context.Background(), "must-not-execute", directory); err == nil {
		t.Fatal("invalid hashes accepted")
	}
}

func TestRealExportParity(t *testing.T) {
	directory := os.Getenv("SCHEDULER_TEST_MODEL")
	python := os.Getenv("SCHEDULER_TEST_PYTHON")
	if directory == "" || python == "" {
		t.Skip("set SCHEDULER_TEST_MODEL and SCHEDULER_TEST_PYTHON for real TorchScript integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), StartupTimeout)
	defer cancel()
	p, err := Start(ctx, python, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Metadata.Format != "inference/v1" {
		t.Fatalf("metadata %+v", p.Metadata)
	}
}

func TestStartupWaitCancellationReapsWorker(t *testing.T) {
	p := helperProcess(t, "healthy")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := p.receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %v", err)
	}
	if !p.closed || p.command.ProcessState == nil {
		t.Fatal("startup worker not reaped")
	}
}
