package results

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrInvalidBundle indicates a schema, semantic, or integrity failure.
	ErrInvalidBundle = errors.New("invalid result bundle")
	// ErrBundleExists prevents accidental replacement of prior results.
	ErrBundleExists = errors.New("result bundle already exists")
)

const (
	manifestName   = "manifest.json"
	summaryName    = "summary.json"
	jobsName       = "jobs.csv"
	timeSeriesName = "timeseries.csv"
)

var jobHeader = []string{
	"id", "state", "assigned_node", "submit_time", "start_time", "end_time", "duration",
	"queue_wait", "latency", "deadline", "priority", "cpu_millis", "memory_mib", "gpus",
	"ephemeral_disk_mib", "rejection_reason",
}

var timeSeriesHeader = []string{
	"simulation_time", "event_sequence", "event_type", "queued", "running", "completed", "rejected",
	"cpu_utilization", "memory_utilization", "gpu_utilization", "disk_utilization", "fragmentation",
}

// NewManifest captures reproducibility metadata available to the running binary.
func NewManifest(configPath string, configData []byte, policy string, seed int64, commandVersion string, startedAt, finishedAt time.Time) Manifest {
	configHash := hashBytes(configData)
	identity := fmt.Sprintf("%s\x00%s\x00%d\x00%s", configHash, policy, seed, startedAt.UTC().Format(time.RFC3339Nano))
	runHash := sha256.Sum256([]byte(identity))
	manifest := Manifest{
		SchemaVersion: SchemaVersion,
		RunID:         hex.EncodeToString(runHash[:16]),
		StartedAt:     startedAt.UTC(), FinishedAt: finishedAt.UTC(), Policy: policy, Seed: seed,
		CommandVersion: commandVersion,
		Configuration:  InputArtifact{Path: filepath.Clean(configPath), SHA256: configHash},
		Runtime:        RuntimeProvenance{GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH},
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				manifest.Git.Revision, manifest.Git.Available = setting.Value, setting.Value != ""
			case "vcs.modified":
				manifest.Git.Modified = setting.Value == "true"
			}
		}
		for _, dependency := range info.Deps {
			version := dependency.Version
			if dependency.Replace != nil {
				version = dependency.Replace.Version
			}
			manifest.Dependencies = append(manifest.Dependencies, Dependency{Path: dependency.Path, Version: version})
		}
		sort.Slice(manifest.Dependencies, func(i, j int) bool { return manifest.Dependencies[i].Path < manifest.Dependencies[j].Path })
	}
	if !manifest.Git.Available {
		if output, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
			manifest.Git.Revision = strings.TrimSpace(string(output))
			manifest.Git.Available = manifest.Git.Revision != ""
		}
		if manifest.Git.Available {
			if output, err := exec.Command("git", "status", "--porcelain").Output(); err == nil {
				manifest.Git.Modified = len(bytes.TrimSpace(output)) != 0
			}
		}
	}
	return manifest
}

// WriteBundle creates a new bundle directory and never overwrites an existing one.
func WriteBundle(directory string, manifest Manifest, summary Summary, jobs []JobRecord, series []TimeSeriesRecord) error {
	if directory == "" {
		return fmt.Errorf("%w: directory is required", ErrInvalidBundle)
	}
	summaryData, err := marshalJSON(summary)
	if err != nil {
		return fmt.Errorf("encode summary: %w", err)
	}
	jobsData, err := encodeJobs(jobs)
	if err != nil {
		return err
	}
	seriesData, err := encodeTimeSeries(series)
	if err != nil {
		return err
	}
	manifest.Outputs = []OutputArtifact{
		{Path: summaryName, SHA256: hashBytes(summaryData)},
		{Path: jobsName, SHA256: hashBytes(jobsData), Rows: len(jobs)},
		{Path: timeSeriesName, SHA256: hashBytes(seriesData), Rows: len(series)},
	}
	if err := validateSummary(summary); err != nil {
		return err
	}
	for index, row := range jobs {
		if err := validateJobRow(jobFields(row)); err != nil {
			return fmt.Errorf("%w: jobs row %d: %v", ErrInvalidBundle, index+1, err)
		}
	}
	for index, row := range series {
		if err := validateTimeSeriesRow(timeSeriesFields(row)); err != nil {
			return fmt.Errorf("%w: timeseries row %d: %v", ErrInvalidBundle, index+1, err)
		}
	}
	if err := validateManifest(manifest); err != nil {
		return err
	}
	manifestData, err := marshalJSON(manifest)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(directory), 0o755); err != nil {
		return fmt.Errorf("create bundle parent: %w", err)
	}
	if err := os.Mkdir(directory, 0o755); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%w: %s", ErrBundleExists, directory)
		}
		return fmt.Errorf("create bundle: %w", err)
	}
	for name, data := range map[string][]byte{
		manifestName: manifestData, summaryName: summaryData, jobsName: jobsData, timeSeriesName: seriesData,
	} {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	return nil
}

// ValidateBundle strictly validates schemas, row counts, and SHA-256 integrity.
func ValidateBundle(directory string) error {
	manifestData, err := os.ReadFile(filepath.Join(directory, manifestName))
	if err != nil {
		return fmt.Errorf("%w: read manifest: %v", ErrInvalidBundle, err)
	}
	var manifest Manifest
	if err := strictJSON(manifestData, &manifest); err != nil {
		return fmt.Errorf("%w: manifest: %v", ErrInvalidBundle, err)
	}
	if err := validateManifest(manifest); err != nil {
		return err
	}
	artifacts := make(map[string]OutputArtifact, len(manifest.Outputs))
	for _, output := range manifest.Outputs {
		artifacts[output.Path] = output
		data, err := os.ReadFile(filepath.Join(directory, output.Path))
		if err != nil {
			return fmt.Errorf("%w: read %s: %v", ErrInvalidBundle, output.Path, err)
		}
		if hashBytes(data) != output.SHA256 {
			return fmt.Errorf("%w: %s hash mismatch", ErrInvalidBundle, output.Path)
		}
	}
	var summary Summary
	if err := decodeStrictFile(filepath.Join(directory, summaryName), &summary); err != nil {
		return fmt.Errorf("%w: summary: %v", ErrInvalidBundle, err)
	}
	if err := validateSummary(summary); err != nil {
		return err
	}
	if summary.Policy != manifest.Policy || summary.Seed != manifest.Seed {
		return fmt.Errorf("%w: manifest and summary policy or seed mismatch", ErrInvalidBundle)
	}
	jobRows, err := validateCSV(filepath.Join(directory, jobsName), jobHeader, validateJobRow)
	if err != nil || jobRows != artifacts[jobsName].Rows || jobRows != summary.Jobs.Total {
		return fmt.Errorf("%w: jobs.csv: rows=%d want=%d error=%v", ErrInvalidBundle, jobRows, artifacts[jobsName].Rows, err)
	}
	if rows, err := validateCSV(filepath.Join(directory, timeSeriesName), timeSeriesHeader, validateTimeSeriesRow); err != nil || rows != artifacts[timeSeriesName].Rows {
		return fmt.Errorf("%w: timeseries.csv: rows=%d want=%d error=%v", ErrInvalidBundle, rows, artifacts[timeSeriesName].Rows, err)
	}
	return nil
}

func validateManifest(manifest Manifest) error {
	if manifest.Policy == "rl" {
		if manifest.Model == nil || manifest.Model.Validate() != nil || manifest.FallbackPolicy == "" {
			return fmt.Errorf("%w: missing or invalid model provenance", ErrInvalidBundle)
		}
		if budget, err := time.ParseDuration(manifest.InferenceTimeout); err != nil || budget <= 0 {
			return fmt.Errorf("%w: invalid inference timeout", ErrInvalidBundle)
		}
	}
	if manifest.SchemaVersion != SchemaVersion || !validRunID(manifest.RunID) || manifest.Policy == "" || manifest.CommandVersion == "" || manifest.Configuration.Path == "" {
		return fmt.Errorf("%w: manifest required fields or version", ErrInvalidBundle)
	}
	if manifest.StartedAt.IsZero() || manifest.FinishedAt.Before(manifest.StartedAt) || !validHash(manifest.Configuration.SHA256) {
		return fmt.Errorf("%w: manifest timestamps or configuration hash", ErrInvalidBundle)
	}
	want := map[string]bool{summaryName: false, jobsName: false, timeSeriesName: false}
	for _, output := range manifest.Outputs {
		if _, exists := want[output.Path]; !exists || want[output.Path] || !validHash(output.SHA256) || output.Rows < 0 {
			return fmt.Errorf("%w: unexpected, duplicate, or invalid output %q", ErrInvalidBundle, output.Path)
		}
		want[output.Path] = true
	}
	for path, found := range want {
		if !found {
			return fmt.Errorf("%w: missing output %q", ErrInvalidBundle, path)
		}
	}
	return nil
}

func validateSummary(summary Summary) error {
	if summary.Policy == "rl" && summary.Inference == nil {
		return fmt.Errorf("%w: missing inference diagnostics", ErrInvalidBundle)
	}
	if diagnostics := summary.Inference; diagnostics != nil {
		if diagnostics.Fallbacks > diagnostics.Decisions || diagnostics.ModelCalls > diagnostics.Decisions {
			return fmt.Errorf("%w: invalid inference counters", ErrInvalidBundle)
		}
		var total uint64
		for _, count := range diagnostics.Reasons {
			if count > diagnostics.Fallbacks-total {
				return fmt.Errorf("%w: invalid fallback reason counts", ErrInvalidBundle)
			}
			total += count
		}
		if total != diagnostics.Fallbacks {
			return fmt.Errorf("%w: fallback reasons do not total", ErrInvalidBundle)
		}
	}
	if summary.SchemaVersion != SchemaVersion || summary.Policy == "" || summary.NodeCount < 0 ||
		summary.Jobs.Total < 0 || summary.Jobs.Queued < 0 || summary.Jobs.Running < 0 || summary.Jobs.Completed < 0 || summary.Jobs.Rejected < 0 {
		return fmt.Errorf("%w: summary required fields or version", ErrInvalidBundle)
	}
	if summary.Jobs.Queued+summary.Jobs.Running+summary.Jobs.Completed+summary.Jobs.Rejected != summary.Jobs.Total {
		return fmt.Errorf("%w: summary job counters do not total", ErrInvalidBundle)
	}
	if summary.Metrics.DeadlineViolations > summary.Metrics.DeadlineJobs || summary.Metrics.DeadlineJobs > uint64(summary.Jobs.Completed) {
		return fmt.Errorf("%w: summary deadline counters are inconsistent", ErrInvalidBundle)
	}
	for _, value := range []float64{
		summary.Metrics.ThroughputPerSecond, summary.Metrics.SuccessRate, summary.Metrics.RejectionRate,
		summary.Metrics.DeadlineViolationRate, summary.Metrics.MeanBoundedSlowdown, summary.Metrics.SlowdownFairness,
		summary.Metrics.Utilization.CPU, summary.Metrics.Utilization.Memory, summary.Metrics.Utilization.GPU,
		summary.Metrics.Utilization.Ephemeral, summary.Metrics.MeanQueueDepth, summary.Metrics.ResourceFragmentation,
	} {
		if mathInvalid(value) {
			return fmt.Errorf("%w: summary contains non-finite or negative metric", ErrInvalidBundle)
		}
	}
	for _, value := range []float64{
		summary.Metrics.SuccessRate, summary.Metrics.RejectionRate, summary.Metrics.DeadlineViolationRate,
		summary.Metrics.SlowdownFairness, summary.Metrics.Utilization.CPU, summary.Metrics.Utilization.Memory,
		summary.Metrics.Utilization.GPU, summary.Metrics.Utilization.Ephemeral, summary.Metrics.ResourceFragmentation,
	} {
		if value > 1 {
			return fmt.Errorf("%w: normalized summary metric exceeds one", ErrInvalidBundle)
		}
	}
	for _, value := range []string{
		summary.SimulationTime, summary.Metrics.Latency.Min, summary.Metrics.Latency.Max, summary.Metrics.Latency.Mean,
		summary.Metrics.Latency.P50, summary.Metrics.Latency.P95, summary.Metrics.Latency.P99,
		summary.Metrics.QueueWait.Min, summary.Metrics.QueueWait.Max, summary.Metrics.QueueWait.Mean,
		summary.Metrics.QueueWait.P50, summary.Metrics.QueueWait.P95, summary.Metrics.QueueWait.P99,
		summary.Metrics.Scheduler.TotalDuration, summary.Metrics.Scheduler.MeanDuration, summary.Metrics.Scheduler.MaxDuration,
	} {
		if duration, err := time.ParseDuration(value); err != nil || duration < 0 {
			return fmt.Errorf("%w: invalid duration %q", ErrInvalidBundle, value)
		}
	}
	return nil
}

func encodeJobs(rows []JobRecord) ([]byte, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	_ = writer.Write(jobHeader)
	for _, row := range rows {
		_ = writer.Write(jobFields(row))
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("encode jobs: %w", err)
	}
	return buffer.Bytes(), nil
}

func encodeTimeSeries(rows []TimeSeriesRecord) ([]byte, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	_ = writer.Write(timeSeriesHeader)
	for _, row := range rows {
		_ = writer.Write(timeSeriesFields(row))
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("encode timeseries: %w", err)
	}
	return buffer.Bytes(), nil
}

func jobFields(row JobRecord) []string {
	return []string{
		row.ID, row.State, row.AssignedNode, row.SubmitTime, row.StartTime, row.EndTime, row.Duration,
		row.QueueWait, row.Latency, row.Deadline, strconv.FormatInt(int64(row.Priority), 10),
		strconv.FormatInt(row.CPUMillis, 10), strconv.FormatInt(row.MemoryMiB, 10), strconv.FormatInt(row.GPUs, 10),
		strconv.FormatInt(row.EphemeralDiskMiB, 10), row.RejectionReason,
	}
}

func timeSeriesFields(row TimeSeriesRecord) []string {
	return []string{
		row.SimulationTime, strconv.FormatUint(row.EventSequence, 10), row.EventType,
		strconv.Itoa(row.Queued), strconv.Itoa(row.Running), strconv.Itoa(row.Completed), strconv.Itoa(row.Rejected),
		formatFloat(row.CPUUtilization), formatFloat(row.MemoryUtilization), formatFloat(row.GPUUtilization),
		formatFloat(row.DiskUtilization), formatFloat(row.Fragmentation),
	}
}

func validateCSV(path string, header []string, validate func([]string) error) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	got, err := reader.Read()
	if err != nil || !equalStrings(got, header) {
		return 0, fmt.Errorf("invalid header")
	}
	rows := 0
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return rows, err
		}
		if len(row) != len(header) {
			return rows, fmt.Errorf("row %d has %d fields", rows+1, len(row))
		}
		if err := validate(row); err != nil {
			return rows, fmt.Errorf("row %d: %w", rows+1, err)
		}
		rows++
	}
	return rows, nil
}

func validateJobRow(row []string) error {
	if row[0] == "" {
		return fmt.Errorf("id is required")
	}
	switch row[1] {
	case "queued", "running", "completed", "rejected":
	default:
		return fmt.Errorf("invalid state %q", row[1])
	}
	parsedDurations := make(map[int]time.Duration)
	for _, index := range []int{3, 6} {
		if value, err := time.ParseDuration(row[index]); err != nil || value < 0 {
			return fmt.Errorf("invalid duration %q", row[index])
		} else {
			parsedDurations[index] = value
		}
	}
	for _, index := range []int{4, 5, 7, 8, 9} {
		if row[index] == "" {
			continue
		}
		if value, err := time.ParseDuration(row[index]); err != nil || value < 0 {
			return fmt.Errorf("invalid optional duration %q", row[index])
		} else {
			parsedDurations[index] = value
		}
	}
	if value, err := strconv.ParseInt(row[10], 10, 32); err != nil || value < 0 {
		return fmt.Errorf("invalid priority %q", row[10])
	}
	resourceNonzero := false
	for _, index := range []int{11, 12, 13, 14} {
		value, err := strconv.ParseInt(row[index], 10, 64)
		if err != nil || value < 0 {
			return fmt.Errorf("invalid non-negative integer %q", row[index])
		}
		resourceNonzero = resourceNonzero || value > 0
	}
	if parsedDurations[6] <= 0 || !resourceNonzero {
		return fmt.Errorf("duration and resource request must be positive")
	}
	if deadline, exists := parsedDurations[9]; exists && deadline < parsedDurations[3] {
		return fmt.Errorf("deadline precedes submission")
	}
	switch row[1] {
	case "completed":
		if row[2] == "" || row[4] == "" || row[5] == "" || row[7] == "" || row[8] == "" || row[15] != "" {
			return fmt.Errorf("completed job lifecycle fields are inconsistent")
		}
		if parsedDurations[4] < parsedDurations[3] || parsedDurations[5] < parsedDurations[4] {
			return fmt.Errorf("completed job times are out of order")
		}
		if parsedDurations[7] != parsedDurations[4]-parsedDurations[3] || parsedDurations[8] != parsedDurations[5]-parsedDurations[3] {
			return fmt.Errorf("completed job derived durations are inconsistent")
		}
	case "rejected":
		if row[2] != "" || row[4] != "" || row[5] == "" || row[15] == "" {
			return fmt.Errorf("rejected job lifecycle fields are inconsistent")
		}
		if parsedDurations[5] < parsedDurations[3] || row[8] == "" || parsedDurations[8] != parsedDurations[5]-parsedDurations[3] {
			return fmt.Errorf("rejected job times are inconsistent")
		}
	case "running":
		if row[2] == "" || row[4] == "" || row[5] != "" {
			return fmt.Errorf("running job lifecycle fields are inconsistent")
		}
		if parsedDurations[4] < parsedDurations[3] || row[7] == "" || parsedDurations[7] != parsedDurations[4]-parsedDurations[3] {
			return fmt.Errorf("running job times are inconsistent")
		}
	case "queued":
		if row[2] != "" || row[4] != "" || row[5] != "" {
			return fmt.Errorf("queued job lifecycle fields are inconsistent")
		}
	}
	return nil
}

func validateTimeSeriesRow(row []string) error {
	if value, err := time.ParseDuration(row[0]); err != nil || value < 0 || row[2] == "" {
		return fmt.Errorf("invalid time or event type")
	}
	if _, err := strconv.ParseUint(row[1], 10, 64); err != nil {
		return fmt.Errorf("invalid event sequence")
	}
	for _, index := range []int{3, 4, 5, 6} {
		value, err := strconv.Atoi(row[index])
		if err != nil || value < 0 {
			return fmt.Errorf("invalid count %q", row[index])
		}
	}
	for _, index := range []int{7, 8, 9, 10, 11} {
		value, err := strconv.ParseFloat(row[index], 64)
		if err != nil || mathInvalid(value) || value > 1 {
			return fmt.Errorf("invalid normalized metric %q", row[index])
		}
	}
	return nil
}

func strictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

func decodeStrictFile(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return strictJSON(data, target)
}

func marshalJSON(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func hashBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func validHash(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
func validRunID(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16
}
func formatFloat(value float64) string { return strconv.FormatFloat(value, 'g', -1, 64) }
func mathInvalid(value float64) bool {
	return value < 0 || value != value || value > 1.7976931348623157e308
}
func equalStrings(left, right []string) bool {
	return strings.Join(left, "\x00") == strings.Join(right, "\x00")
}
