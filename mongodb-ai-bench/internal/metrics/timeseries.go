package metrics

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type TimeSeriesWriter struct {
	mu        sync.Mutex
	file      *os.File
	writer    *csv.Writer
	collector *Collector
	interval  time.Duration
	stopCh    chan struct{}
	doneCh    chan struct{}
	phase     string
}

func NewTimeSeriesWriter(outputDir string, collector *Collector, interval time.Duration) (*TimeSeriesWriter, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("creating output dir: %w", err)
	}

	filename := filepath.Join(outputDir, fmt.Sprintf("timeseries_%s.csv", time.Now().Format("20060102_150405")))
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("creating CSV file: %w", err)
	}

	w := csv.NewWriter(file)
	header := []string{
		"timestamp", "elapsed_sec", "phase", "operation",
		"p50_ms", "p95_ms", "p99_ms", "p999_ms", "max_ms", "mean_ms",
		"window_ops", "window_duration_ms", "throughput_ops_sec",
		"total_count", "error_count", "total_bytes_mb",
	}
	if err := w.Write(header); err != nil {
		file.Close()
		return nil, fmt.Errorf("writing CSV header: %w", err)
	}
	w.Flush()

	return &TimeSeriesWriter{
		file:      file,
		writer:    w,
		collector: collector,
		interval:  interval,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}, nil
}

func (ts *TimeSeriesWriter) SetPhase(phase string) {
	ts.mu.Lock()
	ts.phase = phase
	ts.mu.Unlock()
}

func (ts *TimeSeriesWriter) Start() {
	go ts.loop()
}

func (ts *TimeSeriesWriter) loop() {
	defer close(ts.doneCh)
	ticker := time.NewTicker(ts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ts.writeSnapshot()
		case <-ts.stopCh:
			ts.writeSnapshot()
			return
		}
	}
}

func (ts *TimeSeriesWriter) writeSnapshot() {
	ts.mu.Lock()
	phase := ts.phase
	ts.mu.Unlock()

	snapshots := ts.collector.AllSnapshots()
	now := time.Now()
	elapsed := ts.collector.Elapsed().Seconds()

	for _, snap := range snapshots {
		windowDuration := snap.WindowEnd.Sub(snap.WindowStart)
		throughput := 0.0
		if windowDuration > 0 {
			throughput = float64(snap.WindowOps) / windowDuration.Seconds()
		}

		record := []string{
			now.Format(time.RFC3339Nano),
			fmt.Sprintf("%.1f", elapsed),
			phase,
			snap.Operation,
			fmt.Sprintf("%.3f", snap.E2E.P50),
			fmt.Sprintf("%.3f", snap.E2E.P95),
			fmt.Sprintf("%.3f", snap.E2E.P99),
			fmt.Sprintf("%.3f", snap.E2E.P999),
			fmt.Sprintf("%.3f", snap.E2E.Max),
			fmt.Sprintf("%.3f", snap.E2E.Mean),
			fmt.Sprintf("%d", snap.WindowOps),
			fmt.Sprintf("%.1f", float64(windowDuration.Milliseconds())),
			fmt.Sprintf("%.1f", throughput),
			fmt.Sprintf("%d", snap.TotalCount),
			fmt.Sprintf("%d", snap.ErrorCount),
			fmt.Sprintf("%.2f", float64(snap.TotalBytes)/(1024*1024)),
		}

		ts.mu.Lock()
		ts.writer.Write(record)
		ts.writer.Flush()
		ts.mu.Unlock()
	}
}

func (ts *TimeSeriesWriter) Stop() {
	close(ts.stopCh)
	<-ts.doneCh
	ts.file.Close()
}
