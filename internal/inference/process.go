package inference

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/policyv1"
)

type Metadata struct {
	Format           string          `json:"format"`
	Runtime          string          `json:"runtime"`
	Schema           policyv1.Schema `json:"schema"`
	Torch            string          `json:"torch"`
	ModelSHA256      string          `json:"model_sha256"`
	CheckpointSHA256 string          `json:"checkpoint_sha256"`
	ParitySHA256     string          `json:"parity_sha256"`
}

func (m Metadata) Validate() error {
	config := policyv1.Config{MaxJobs: m.Schema.MaxJobs, MaxNodes: m.Schema.MaxNodes, Normalization: m.Schema.Normalization}
	expected, err := config.Schema()
	if err != nil || m.Format != "inference/v1" || m.Runtime != "torchscript" || m.Torch == "" || !reflect.DeepEqual(m.Schema, expected) {
		return errors.New("incompatible inference metadata/schema")
	}
	for _, hash := range []string{m.ModelSHA256, m.CheckpointSHA256, m.ParitySHA256} {
		if decoded, err := hex.DecodeString(hash); err != nil || len(decoded) != sha256.Size {
			return errors.New("invalid artifact hash")
		}
	}
	return nil
}

type Process struct {
	mu       sync.Mutex
	command  *exec.Cmd
	input    io.WriteCloser
	scanner  *bufio.Scanner
	closed   bool
	Metadata Metadata
	Config   policyv1.Config
}

type request struct {
	Features []float64 `json:"features"`
	Mask     []bool    `json:"mask"`
}

// Start verifies the artifact before launching a persistent worker. Startup
// includes fixed-vector output parity; a failure is fatal, never silent fallback.
func Start(ctx context.Context, python, directory string) (*Process, error) {
	var metadata Metadata
	if err := readJSON(filepath.Join(directory, "metadata.json"), &metadata); err != nil {
		return nil, err
	}
	config := policyv1.Config{MaxJobs: metadata.Schema.MaxJobs, MaxNodes: metadata.Schema.MaxNodes, Normalization: metadata.Schema.Normalization}
	if err := metadata.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for name, hash := range map[string]string{"policy.ts": metadata.ModelSHA256, "parity.json": metadata.ParitySHA256} {
		if err := checkHash(filepath.Join(directory, name), hash); err != nil {
			return nil, err
		}
	}
	p := &Process{Metadata: metadata, Config: config}
	p.command = exec.Command(python, "-m", "scheduler_rl.inference_worker", directory)
	p.command.Stderr = io.Discard
	var err error
	p.input, err = p.command.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := p.command.StdoutPipe()
	if err != nil {
		_ = p.input.Close()
		return nil, err
	}
	p.scanner = bufio.NewScanner(output)
	p.scanner.Buffer(make([]byte, 4096), 32<<20)
	if err := p.command.Start(); err != nil {
		_ = p.input.Close()
		_ = output.Close()
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			_ = p.Close()
		}
	}()
	line, err := p.receive(ctx)
	if err != nil {
		return nil, fmt.Errorf("worker startup: %w", err)
	}
	var ready struct {
		Ready string `json:"ready"`
	}
	if err := strictJSON(line, &ready); err != nil || ready.Ready != "inference/v1" {
		return nil, errors.New("invalid worker handshake")
	}
	var fixture struct {
		Observation policyv1.Observation `json:"observation"`
		Features    [][]float64          `json:"features"`
		Mask        [][]bool             `json:"mask"`
		Logits      [][]float64          `json:"logits"`
		Values      []float64            `json:"values"`
	}
	if err := readJSON(filepath.Join(directory, "parity.json"), &fixture); err != nil {
		return nil, err
	}
	if len(fixture.Features) != 1 || len(fixture.Mask) != 1 || len(fixture.Logits) != 1 || !sameFloat32(Flatten(fixture.Observation), fixture.Features[0]) || !reflect.DeepEqual(fixture.Observation.ActionMask, fixture.Mask[0]) || len(fixture.Logits[0]) != config.MaxJobs*config.MaxNodes+1 {
		return nil, errors.New("invalid parity fixture")
	}
	logits, err := p.Predict(ctx, fixture.Observation)
	if err != nil {
		return nil, err
	}
	if len(logits) != len(fixture.Logits[0]) {
		return nil, errors.New("parity shape mismatch")
	}
	for i, value := range logits {
		want := fixture.Logits[0][i]
		if math.IsNaN(value) || math.IsInf(value, 0) || math.Abs(value-want) > 1e-5*math.Max(1, math.Abs(want)) {
			return nil, errors.New("model parity mismatch")
		}
	}
	failed = false
	return p, nil
}

func sameFloat32(left, right []float64) bool {
	if len(left) != len(right) {
		return false
	}
	for index, value := range left {
		if float64(float32(value)) != right[index] {
			return false
		}
	}
	return true
}

func Flatten(observation policyv1.Observation) []float64 {
	output := []float64{}
	for _, row := range observation.NodeFeatures {
		output = append(output, row...)
	}
	for _, row := range observation.JobFeatures {
		output = append(output, row...)
	}
	return append(output, observation.GlobalFeatures...)
}

func (p *Process) Predict(ctx context.Context, observation policyv1.Observation) ([]float64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("inference worker is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(request{Features: Flatten(observation), Mask: observation.ActionMask})
	if err != nil {
		return nil, err
	}
	// The write and read share one deadline. Killing the process closes both pipes.
	type result struct {
		line []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		_, err := p.input.Write(append(data, '\n'))
		if err != nil {
			done <- result{err: err}
			return
		}
		line, err := p.scan()
		done <- result{line, err}
	}()
	var response result
	select {
	case response = <-done:
	case <-ctx.Done():
		p.stop()
		<-done
		return nil, ctx.Err()
	}
	if response.err != nil {
		p.stop()
		return nil, response.err
	}
	var value struct {
		Logits []float64 `json:"logits"`
		Error  string    `json:"error,omitempty"`
	}
	if err := strictJSON(response.line, &value); err != nil {
		p.stop()
		return nil, err
	}
	if value.Error != "" {
		return nil, fmt.Errorf("model runtime: %s", value.Error)
	}
	return value.Logits, nil
}

func (p *Process) scan() ([]byte, error) {
	if !p.scanner.Scan() {
		if err := p.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	return append([]byte(nil), p.scanner.Bytes()...), nil
}

func (p *Process) receive(ctx context.Context) ([]byte, error) {
	type result struct {
		line []byte
		err  error
	}
	done := make(chan result, 1)
	go func() { line, err := p.scan(); done <- result{line, err} }()
	select {
	case value := <-done:
		return value.line, value.err
	case <-ctx.Done():
		p.stop()
		<-done
		return nil, ctx.Err()
	}
}

func (p *Process) stop() {
	if !p.closed {
		p.closed = true
		_ = p.input.Close()
		_ = p.command.Process.Kill()
		_ = p.command.Wait()
	}
}
func (p *Process) Close() error { p.mu.Lock(); defer p.mu.Unlock(); p.stop(); return nil }

func readJSON(path string, value any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 32<<20+1))
	if err != nil {
		return err
	}
	if len(data) > 32<<20 {
		return errors.New("inference JSON too large")
	}
	return strictJSON(data, value)
}
func strictJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}
func checkHash(path, expected string) error {
	if len(expected) != 64 {
		return errors.New("invalid artifact hash")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	count, err := io.Copy(hash, io.LimitReader(file, 128<<20+1))
	if err != nil {
		return err
	}
	if count > 128<<20 || hex.EncodeToString(hash.Sum(nil)) != expected {
		return fmt.Errorf("artifact hash mismatch: %s", path)
	}
	return nil
}

// StartupTimeout is separate from the per-decision inference budget.
const StartupTimeout = 30 * time.Second
