package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	OpWriteHumanMessage    = "write_human_message"
	OpWriteAssistantMsg    = "write_assistant_message"
	OpWriteConvoMetadata   = "write_conversation_metadata"
	OpReadConvoHistory     = "read_conversation_history"
	OpCreateConversation   = "create_conversation"
)

type Sample struct {
	Operation         string
	E2ELatency        time.Duration
	Success           bool
	Timestamp         time.Time
	DocumentSizeBytes int
	Error             string
}

type RecentError struct {
	Timestamp time.Time
	Operation string
	Message   string
}

type HourlyBucket struct {
	Hour       int
	HourStart  time.Time
	HourEnd    time.Time
	E2E        LatencySnapshot
	Ops        int64
	Errors     int64
	BytesMB    float64
}

type OperationMetrics struct {
	mu           sync.Mutex
	e2eHist      *hdrhistogram.Histogram
	totalCount   atomic.Int64
	errorCount   atomic.Int64
	totalBytes   atomic.Int64
	windowHist   *hdrhistogram.Histogram
	windowStart  time.Time

	hourlyHist      *hdrhistogram.Histogram
	hourlyStart     time.Time
	hourlyOps       int64
	hourlyErrors    int64
	hourlyBytes     int64
	hourlyBuckets   []HourlyBucket
	hourIndex       int
}

type Snapshot struct {
	Operation   string
	E2E         LatencySnapshot
	TotalCount  int64
	ErrorCount  int64
	TotalBytes  int64
	WindowOps   int64
	WindowStart time.Time
	WindowEnd   time.Time
}

type LatencySnapshot struct {
	P50    float64
	P75    float64
	P90    float64
	P95    float64
	P99    float64
	P999   float64
	Max    float64
	Mean   float64
	StdDev float64
	Count  int64
}

const maxRecentErrors = 10

type Collector struct {
	operations   map[string]*OperationMetrics
	startTime    time.Time
	recentErrors []RecentError
	errorMu      sync.Mutex
}

func NewCollector() *Collector {
	ops := []string{
		OpWriteHumanMessage,
		OpWriteAssistantMsg,
		OpWriteConvoMetadata,
		OpReadConvoHistory,
		OpCreateConversation,
	}

	c := &Collector{
		operations: make(map[string]*OperationMetrics, len(ops)),
		startTime:  time.Now(),
	}

	for _, op := range ops {
		c.operations[op] = newOperationMetrics()
	}

	return c
}

func newOperationMetrics() *OperationMetrics {
	now := time.Now()
	return &OperationMetrics{
		e2eHist:     hdrhistogram.New(1, 60_000_000, 3),
		windowHist:  hdrhistogram.New(1, 60_000_000, 3),
		windowStart: now,
		hourlyHist:  hdrhistogram.New(1, 60_000_000, 3),
		hourlyStart: now,
		hourIndex:   1,
	}
}

func (c *Collector) Record(s Sample) {
	om, ok := c.operations[s.Operation]
	if !ok {
		return
	}

	e2eMicros := s.E2ELatency.Microseconds()
	if e2eMicros < 1 {
		e2eMicros = 1
	}

	om.mu.Lock()
	om.e2eHist.RecordValue(e2eMicros)
	om.windowHist.RecordValue(e2eMicros)

	if time.Since(om.hourlyStart) >= time.Hour {
		om.rotateHourlyLocked()
	}
	om.hourlyHist.RecordValue(e2eMicros)
	om.hourlyOps++
	if !s.Success {
		om.hourlyErrors++
	}
	om.hourlyBytes += int64(s.DocumentSizeBytes)
	om.mu.Unlock()

	om.totalCount.Add(1)
	if !s.Success {
		om.errorCount.Add(1)
		if s.Error != "" {
			c.pushError(RecentError{
				Timestamp: s.Timestamp,
				Operation: s.Operation,
				Message:   s.Error,
			})
		}
	}
	if s.DocumentSizeBytes > 0 {
		om.totalBytes.Add(int64(s.DocumentSizeBytes))
	}
}

func (c *Collector) Snapshot(operation string) Snapshot {
	om, ok := c.operations[operation]
	if !ok {
		return Snapshot{Operation: operation}
	}

	om.mu.Lock()
	e2eSnap := histSnapshot(om.e2eHist)
	windowOps := om.windowHist.TotalCount()
	windowStart := om.windowStart
	om.windowHist.Reset()
	om.windowStart = time.Now()
	om.mu.Unlock()

	return Snapshot{
		Operation:   operation,
		E2E:         e2eSnap,
		TotalCount:  om.totalCount.Load(),
		ErrorCount:  om.errorCount.Load(),
		TotalBytes:  om.totalBytes.Load(),
		WindowOps:   windowOps,
		WindowStart: windowStart,
		WindowEnd:   time.Now(),
	}
}

func (c *Collector) AllSnapshots() []Snapshot {
	ops := []string{
		OpCreateConversation,
		OpWriteHumanMessage,
		OpWriteAssistantMsg,
		OpWriteConvoMetadata,
		OpReadConvoHistory,
	}
	snapshots := make([]Snapshot, 0, len(ops))
	for _, op := range ops {
		snapshots = append(snapshots, c.Snapshot(op))
	}
	return snapshots
}

func (c *Collector) TotalOps() int64 {
	var total int64
	for _, om := range c.operations {
		total += om.totalCount.Load()
	}
	return total
}

func (c *Collector) TotalErrors() int64 {
	var total int64
	for _, om := range c.operations {
		total += om.errorCount.Load()
	}
	return total
}

func (c *Collector) TotalWriteOps() int64 {
	var total int64
	for _, op := range []string{OpWriteHumanMessage, OpWriteAssistantMsg, OpWriteConvoMetadata, OpCreateConversation} {
		if om, ok := c.operations[op]; ok {
			total += om.totalCount.Load()
		}
	}
	return total
}

func (c *Collector) TotalReadOps() int64 {
	if om, ok := c.operations[OpReadConvoHistory]; ok {
		return om.totalCount.Load()
	}
	return 0
}

func (c *Collector) TotalBytes() int64 {
	var total int64
	for _, om := range c.operations {
		total += om.totalBytes.Load()
	}
	return total
}

func (c *Collector) Elapsed() time.Duration {
	return time.Since(c.startTime)
}

func (c *Collector) pushError(e RecentError) {
	c.errorMu.Lock()
	defer c.errorMu.Unlock()
	c.recentErrors = append(c.recentErrors, e)
	if len(c.recentErrors) > maxRecentErrors {
		c.recentErrors = c.recentErrors[len(c.recentErrors)-maxRecentErrors:]
	}
}

func (c *Collector) RecentErrors() []RecentError {
	c.errorMu.Lock()
	defer c.errorMu.Unlock()
	out := make([]RecentError, len(c.recentErrors))
	copy(out, c.recentErrors)
	return out
}

func (om *OperationMetrics) rotateHourlyLocked() {
	now := time.Now()
	om.hourlyBuckets = append(om.hourlyBuckets, HourlyBucket{
		Hour:      om.hourIndex,
		HourStart: om.hourlyStart,
		HourEnd:   now,
		E2E:       histSnapshot(om.hourlyHist),
		Ops:       om.hourlyOps,
		Errors:    om.hourlyErrors,
		BytesMB:   float64(om.hourlyBytes) / (1024 * 1024),
	})
	om.hourlyHist.Reset()
	om.hourlyOps = 0
	om.hourlyErrors = 0
	om.hourlyBytes = 0
	om.hourlyStart = now
	om.hourIndex++
}

func (om *OperationMetrics) flushHourlyLocked() []HourlyBucket {
	if om.hourlyOps > 0 {
		om.rotateHourlyLocked()
	}
	out := make([]HourlyBucket, len(om.hourlyBuckets))
	copy(out, om.hourlyBuckets)
	return out
}

type HourlyOperationBreakdown struct {
	Operation string
	Buckets   []HourlyBucket
}

func (c *Collector) HourlyBreakdown() []HourlyOperationBreakdown {
	ops := []string{
		OpCreateConversation,
		OpWriteHumanMessage,
		OpWriteAssistantMsg,
		OpWriteConvoMetadata,
		OpReadConvoHistory,
	}
	var result []HourlyOperationBreakdown
	for _, op := range ops {
		om, ok := c.operations[op]
		if !ok {
			continue
		}
		om.mu.Lock()
		buckets := om.flushHourlyLocked()
		om.mu.Unlock()
		if len(buckets) > 0 {
			result = append(result, HourlyOperationBreakdown{
				Operation: op,
				Buckets:   buckets,
			})
		}
	}
	return result
}

func histSnapshot(h *hdrhistogram.Histogram) LatencySnapshot {
	if h.TotalCount() == 0 {
		return LatencySnapshot{}
	}
	return LatencySnapshot{
		P50:    float64(h.ValueAtPercentile(50)) / 1000.0,
		P75:    float64(h.ValueAtPercentile(75)) / 1000.0,
		P90:    float64(h.ValueAtPercentile(90)) / 1000.0,
		P95:    float64(h.ValueAtPercentile(95)) / 1000.0,
		P99:    float64(h.ValueAtPercentile(99)) / 1000.0,
		P999:   float64(h.ValueAtPercentile(99.9)) / 1000.0,
		Max:    float64(h.Max()) / 1000.0,
		Mean:   h.Mean() / 1000.0,
		StdDev: h.StdDev() / 1000.0,
		Count:  h.TotalCount(),
	}
}

func (ls LatencySnapshot) String() string {
	return fmt.Sprintf("p50=%.2fms p95=%.2fms p99=%.2fms p99.9=%.2fms max=%.2fms",
		ls.P50, ls.P95, ls.P99, ls.P999, ls.Max)
}
