package workload

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const validTraceCSV = `id,submit_time,cpu_millis,memory_mib,gpus,ephemeral_disk_mib,duration,priority,deadline
job-1,0s,1000,512,0,100,2s,1,5s
job-2,250ms,2000,4096,1,500,4s,0,
`

const validTraceJSON = `[
  {"id":"job-1","submit_time":"0s","cpu_millis":1000,"memory_mib":512,"gpus":0,"ephemeral_disk_mib":100,"duration":"2s","priority":1,"deadline":"5s"},
  {"id":"job-2","submit_time":"250ms","cpu_millis":2000,"memory_mib":4096,"gpus":1,"ephemeral_disk_mib":500,"duration":"4s","priority":0}
]`

func TestLoadTraceCSVAndJSONAgree(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	csvPath := writeTrace(t, directory, "jobs.csv", validTraceCSV)
	jsonPath := writeTrace(t, directory, "jobs.json", validTraceJSON)
	csvJobs, err := LoadTrace(csvPath, "")
	if err != nil {
		t.Fatalf("LoadTrace(CSV): %v", err)
	}
	jsonJobs, err := LoadTrace(jsonPath, "json")
	if err != nil {
		t.Fatalf("LoadTrace(JSON): %v", err)
	}
	if !reflect.DeepEqual(csvJobs, jsonJobs) {
		t.Fatalf("CSV and JSON jobs differ:\n%+v\n%+v", csvJobs, jsonJobs)
	}
	if len(csvJobs) != 2 || csvJobs[1].SubmitTime != 250*time.Millisecond || csvJobs[1].Request.GPUs != 1 {
		t.Fatalf("jobs = %+v, want parsed heterogeneous trace", csvJobs)
	}
}

func TestLoadTraceRejectsMalformedInput(t *testing.T) {
	t.Parallel()
	tests := map[string]struct{ name, format, data string }{
		"unknown JSON field": {"jobs.json", "", strings.Replace(validTraceJSON, `"priority":1`, `"priority":1,"mystery":true`, 1)},
		"trailing JSON":      {"jobs.json", "", validTraceJSON + ` {}`},
		"wrong CSV header":   {"jobs.csv", "", strings.Replace(validTraceCSV, "id,submit_time", "job_id,submit_time", 1)},
		"invalid integer":    {"jobs.csv", "", strings.Replace(validTraceCSV, ",1000,512,", ",many,512,", 1)},
		"duplicate id":       {"jobs.csv", "", strings.Replace(validTraceCSV, "job-2,", "job-1,", 1)},
		"unordered":          {"jobs.csv", "", strings.Replace(validTraceCSV, "job-2,250ms", "job-2,-1ms", 1)},
		"empty":              {"jobs.json", "", `[]`},
		"unsupported":        {"jobs.txt", "toml", "jobs = []"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := writeTrace(t, t.TempDir(), test.name, test.data)
			if _, err := LoadTrace(path, test.format); !errors.Is(err, ErrInvalidTrace) {
				t.Fatalf("LoadTrace() error = %v, want ErrInvalidTrace", err)
			}
		})
	}
}

func TestLoadTraceReportsMissingFile(t *testing.T) {
	t.Parallel()
	_, err := LoadTrace(filepath.Join(t.TempDir(), "missing.csv"), "")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadTrace() error = %v, want os.ErrNotExist", err)
	}
}

func writeTrace(t *testing.T, directory, name, contents string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
