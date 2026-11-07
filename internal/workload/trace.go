package workload

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/will19800/RL-Driven-Distributed-Job-Scheduler/internal/domain"
)

const MaxTraceSize = 32 << 20

var ErrInvalidTrace = errors.New("invalid workload trace")

var traceHeader = []string{"id", "submit_time", "cpu_millis", "memory_mib", "gpus", "ephemeral_disk_mib", "duration", "priority", "deadline"}

type traceJob struct {
	ID               string `json:"id"`
	SubmitTime       string `json:"submit_time"`
	CPUMillis        int64  `json:"cpu_millis"`
	MemoryMiB        int64  `json:"memory_mib"`
	GPUs             int64  `json:"gpus"`
	EphemeralDiskMiB int64  `json:"ephemeral_disk_mib"`
	Duration         string `json:"duration"`
	Priority         int32  `json:"priority"`
	Deadline         string `json:"deadline,omitempty"`
}

// LoadTrace reads a bounded CSV or JSON trace. Format may be csv, json, or
// empty to infer it from the file extension.
func LoadTrace(path, format string) ([]domain.Job, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read trace %q: %w", path, err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, MaxTraceSize+1))
	if err != nil {
		return nil, fmt.Errorf("read trace %q: %w", path, err)
	}
	if len(data) > MaxTraceSize {
		return nil, fmt.Errorf("%w: trace exceeds %d bytes", ErrInvalidTrace, MaxTraceSize)
	}
	if format == "" {
		format = strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), ".")
	}
	var records []traceJob
	switch strings.ToLower(format) {
	case "csv":
		records, err = decodeCSV(data)
	case "json":
		records, err = decodeJSON(data)
	default:
		return nil, fmt.Errorf("%w: unsupported trace format %q", ErrInvalidTrace, format)
	}
	if err != nil {
		return nil, err
	}
	return convertTrace(records)
}

func decodeCSV(data []byte) ([]traceJob, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.ReuseRecord = true
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("%w: read CSV header: %v", ErrInvalidTrace, err)
	}
	if strings.Join(header, "\x00") != strings.Join(traceHeader, "\x00") {
		return nil, fmt.Errorf("%w: CSV header must be %s", ErrInvalidTrace, strings.Join(traceHeader, ","))
	}
	var output []traceJob
	for row := 2; ; row++ {
		fields, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("%w: CSV row %d: %v", ErrInvalidTrace, row, readErr)
		}
		record, parseErr := parseCSVRow(fields)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: CSV row %d: %v", ErrInvalidTrace, row, parseErr)
		}
		output = append(output, record)
		if len(output) > MaxJobs {
			return nil, fmt.Errorf("%w: more than %d jobs", ErrInvalidTrace, MaxJobs)
		}
	}
	return output, nil
}

func parseCSVRow(fields []string) (traceJob, error) {
	if len(fields) != len(traceHeader) {
		return traceJob{}, fmt.Errorf("got %d fields, want %d", len(fields), len(traceHeader))
	}
	parse64 := func(index int) (int64, error) { return strconv.ParseInt(fields[index], 10, 64) }
	cpu, err := parse64(2)
	if err != nil {
		return traceJob{}, fmt.Errorf("cpu_millis: %v", err)
	}
	memory, err := parse64(3)
	if err != nil {
		return traceJob{}, fmt.Errorf("memory_mib: %v", err)
	}
	gpus, err := parse64(4)
	if err != nil {
		return traceJob{}, fmt.Errorf("gpus: %v", err)
	}
	disk, err := parse64(5)
	if err != nil {
		return traceJob{}, fmt.Errorf("ephemeral_disk_mib: %v", err)
	}
	priority, err := strconv.ParseInt(fields[7], 10, 32)
	if err != nil {
		return traceJob{}, fmt.Errorf("priority: %v", err)
	}
	return traceJob{fields[0], fields[1], cpu, memory, gpus, disk, fields[6], int32(priority), fields[8]}, nil
}

func decodeJSON(data []byte) ([]traceJob, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var records []traceJob
	if err := decoder.Decode(&records); err != nil {
		return nil, fmt.Errorf("%w: decode JSON: %v", ErrInvalidTrace, err)
	}
	if len(records) > MaxJobs {
		return nil, fmt.Errorf("%w: more than %d jobs", ErrInvalidTrace, MaxJobs)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing JSON data", ErrInvalidTrace)
	}
	return records, nil
}

func convertTrace(records []traceJob) ([]domain.Job, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("%w: trace must contain at least one job", ErrInvalidTrace)
	}
	jobs := make([]domain.Job, 0, len(records))
	seen := make(map[domain.JobID]struct{}, len(records))
	var previous time.Duration
	for index, record := range records {
		submit, err := time.ParseDuration(record.SubmitTime)
		if err != nil {
			return nil, traceFieldError(index, "submit_time", err)
		}
		duration, err := time.ParseDuration(record.Duration)
		if err != nil {
			return nil, traceFieldError(index, "duration", err)
		}
		var deadline *time.Duration
		if record.Deadline != "" {
			parsed, parseErr := time.ParseDuration(record.Deadline)
			if parseErr != nil {
				return nil, traceFieldError(index, "deadline", parseErr)
			}
			deadline = &parsed
		}
		job := domain.Job{ID: domain.JobID(record.ID), SubmitTime: submit, Request: domain.ResourceVector{CPUMillis: record.CPUMillis, MemoryMiB: record.MemoryMiB, GPUs: record.GPUs, EphemeralDiskMiB: record.EphemeralDiskMiB}, Duration: duration, Priority: record.Priority, Deadline: deadline, State: domain.JobStateQueued}
		if err := job.Validate(); err != nil {
			return nil, fmt.Errorf("%w: job %d: %v", ErrInvalidTrace, index, err)
		}
		if index > 0 && submit < previous {
			return nil, fmt.Errorf("%w: job %d submit_time is out of order", ErrInvalidTrace, index)
		}
		if _, exists := seen[job.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate job id %q", ErrInvalidTrace, job.ID)
		}
		seen[job.ID] = struct{}{}
		previous = submit
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func traceFieldError(index int, field string, err error) error {
	return fmt.Errorf("%w: job %d %s: %v", ErrInvalidTrace, index, field, err)
}
