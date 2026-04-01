package metrics

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mongodb-ai-bench/internal/pool"
)

type CollStatsSnapshot struct {
	Timestamp  time.Time
	ElapsedSec float64
	Phase      string
	Stats      []pool.CollectionStatsResult
}

type CollStatsWriter struct {
	mu                   sync.Mutex
	file                 *os.File
	writer               *csv.Writer
	poolMgr              *pool.Manager
	interval             time.Duration
	conversationsEnabled bool
	startTime            time.Time
	phase                string
	stopCh               chan struct{}
	doneCh               chan struct{}
	logger               *slog.Logger
	latest               []pool.CollectionStatsResult
}

func NewCollStatsWriter(outputDir string, poolMgr *pool.Manager, interval time.Duration, conversationsEnabled bool) (*CollStatsWriter, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("creating output dir: %w", err)
	}

	filename := filepath.Join(outputDir, fmt.Sprintf("collstats_%s.csv", time.Now().Format("20060102_150405")))
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("creating collstats CSV: %w", err)
	}

	w := csv.NewWriter(file)
	header := []string{
		"timestamp", "elapsed_sec", "phase", "collection",
		"documents", "storage_mb", "index_mb", "avg_doc_bytes", "indexes",
	}
	if err := w.Write(header); err != nil {
		file.Close()
		return nil, fmt.Errorf("writing collstats CSV header: %w", err)
	}
	w.Flush()

	return &CollStatsWriter{
		file:                 file,
		writer:               w,
		poolMgr:              poolMgr,
		interval:             interval,
		conversationsEnabled: conversationsEnabled,
		startTime:            time.Now(),
		stopCh:               make(chan struct{}),
		doneCh:               make(chan struct{}),
		logger:               slog.Default().With("component", "collstats"),
	}, nil
}

func (cs *CollStatsWriter) SetPhase(phase string) {
	cs.mu.Lock()
	cs.phase = phase
	cs.mu.Unlock()
}

func (cs *CollStatsWriter) Start() {
	go cs.loop()
}

func (cs *CollStatsWriter) loop() {
	defer close(cs.doneCh)
	ticker := time.NewTicker(cs.interval)
	defer ticker.Stop()

	cs.collect()

	for {
		select {
		case <-ticker.C:
			cs.collect()
		case <-cs.stopCh:
			cs.collect()
			return
		}
	}
}

func (cs *CollStatsWriter) collect() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stats := cs.poolMgr.AllCollectionStats(ctx, cs.conversationsEnabled)
	if len(stats) == 0 {
		return
	}

	cs.mu.Lock()
	phase := cs.phase
	cs.latest = stats
	cs.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(cs.startTime).Seconds()

	for _, s := range stats {
		record := []string{
			now.Format(time.RFC3339Nano),
			fmt.Sprintf("%.1f", elapsed),
			phase,
			s.Collection,
			fmt.Sprintf("%d", s.Documents),
			fmt.Sprintf("%.2f", s.StorageMB),
			fmt.Sprintf("%.2f", s.IndexMB),
			fmt.Sprintf("%d", s.AvgDocSize),
			fmt.Sprintf("%d", s.Indexes),
		}

		cs.mu.Lock()
		if err := cs.writer.Write(record); err != nil {
			cs.logger.Error("failed to write collstats CSV record", "err", err)
		}
		cs.writer.Flush()
		cs.mu.Unlock()
	}
}

func (cs *CollStatsWriter) Latest() []pool.CollectionStatsResult {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make([]pool.CollectionStatsResult, len(cs.latest))
	copy(out, cs.latest)
	return out
}

func (cs *CollStatsWriter) Stop() {
	close(cs.stopCh)
	<-cs.doneCh
	cs.file.Close()
}
