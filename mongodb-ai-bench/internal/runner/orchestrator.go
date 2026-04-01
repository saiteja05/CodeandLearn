package runner

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mongodb-ai-bench/internal/config"
	"github.com/mongodb-ai-bench/internal/metrics"
	"github.com/mongodb-ai-bench/internal/pool"
)

type Orchestrator struct {
	cfg             *config.Config
	poolMgr         *pool.Manager
	collector       *metrics.Collector
	tsWriter        *metrics.TimeSeriesWriter
	collStatsWriter *metrics.CollStatsWriter
	dashboard       *Dashboard
	runner          *ProgressiveRunner
	logger          *slog.Logger
}

type Dashboard struct {
	collector *metrics.Collector
	poolMgr   *pool.Manager
	runner    *ProgressiveRunner
	interval  time.Duration
	stopCh    chan struct{}
	doneCh    chan struct{}
	mu        sync.RWMutex
	phase     string
}

func NewOrchestrator(cfg *config.Config) (*Orchestrator, error) {
	poolMgr, err := pool.NewManager(cfg.MongoDB)
	if err != nil {
		return nil, fmt.Errorf("creating pool manager: %w", err)
	}

	collector := metrics.NewCollector()

	csvInterval, err := cfg.Metrics.ParsedCSVInterval()
	if err != nil {
		poolMgr.Close(context.Background())
		return nil, fmt.Errorf("parsing csv_interval: %w", err)
	}
	tsWriter, err := metrics.NewTimeSeriesWriter(cfg.Metrics.OutputDir, collector, csvInterval)
	if err != nil {
		poolMgr.Close(context.Background())
		return nil, fmt.Errorf("creating timeseries writer: %w", err)
	}

	var collStatsWriter *metrics.CollStatsWriter
	if cfg.Metrics.CollectionStatsEnabled {
		csInterval, err := cfg.Metrics.ParsedCollectionStatsInterval()
		if err != nil {
			poolMgr.Close(context.Background())
			return nil, fmt.Errorf("parsing collection_stats_interval: %w", err)
		}
		csw, err := metrics.NewCollStatsWriter(cfg.Metrics.OutputDir, poolMgr, csInterval, cfg.Workload.ConversationsEnabled())
		if err != nil {
			poolMgr.Close(context.Background())
			return nil, fmt.Errorf("creating collstats writer: %w", err)
		}
		collStatsWriter = csw
	}

	statsInterval, err := cfg.Metrics.ParsedStatsInterval()
	if err != nil {
		poolMgr.Close(context.Background())
		return nil, fmt.Errorf("parsing stats_interval: %w", err)
	}
	dashboard := &Dashboard{
		collector: collector,
		poolMgr:   poolMgr,
		interval:  statsInterval,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}

	onPhaseChange := func(phase string) {
		tsWriter.SetPhase(phase)
		dashboard.SetPhase(phase)
		if collStatsWriter != nil {
			collStatsWriter.SetPhase(phase)
		}
	}

	progressiveRunner, err := NewProgressiveRunner(
		cfg.Phases,
		poolMgr,
		collector,
		cfg.Workload,
		onPhaseChange,
	)
	if err != nil {
		poolMgr.Close(context.Background())
		return nil, fmt.Errorf("creating progressive runner: %w", err)
	}

	dashboard.runner = progressiveRunner

	return &Orchestrator{
		cfg:             cfg,
		poolMgr:         poolMgr,
		collector:       collector,
		tsWriter:        tsWriter,
		collStatsWriter: collStatsWriter,
		dashboard:       dashboard,
		runner:          progressiveRunner,
		logger:          slog.Default().With("component", "orchestrator"),
	}, nil
}

func (o *Orchestrator) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		o.logger.Info("received signal, initiating graceful shutdown", "signal", sig)
		cancel()
	}()

	o.logger.Info("connecting to MongoDB", "uri", maskURI(o.cfg.MongoDB.URI))
	if err := o.poolMgr.Ping(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	o.logger.Info("connected to MongoDB successfully")

	convEnabled := o.cfg.Workload.ConversationsEnabled()
	o.logger.Info("ensuring indexes", "conversations_enabled", convEnabled)
	if err := o.poolMgr.EnsureIndexes(ctx, convEnabled); err != nil {
		return fmt.Errorf("ensuring indexes: %w", err)
	}

	o.tsWriter.Start()
	defer o.tsWriter.Stop()

	if o.collStatsWriter != nil {
		o.collStatsWriter.Start()
		defer o.collStatsWriter.Stop()
	}

	if o.cfg.Metrics.DashboardEnabled {
		o.dashboard.Start()
		defer o.dashboard.Stop()
	}

	startTime := time.Now()
	o.logger.Info("starting benchmark",
		"phases", len(o.cfg.Phases),
		"database", o.cfg.MongoDB.Database,
	)

	err := o.runner.Run(ctx)

	elapsed := time.Since(startTime)
	o.logger.Info("benchmark finished",
		"elapsed", elapsed,
		"total_ops", o.collector.TotalOps(),
		"total_errors", o.collector.TotalErrors(),
		"total_writes", o.collector.TotalWriteOps(),
		"total_reads", o.collector.TotalReadOps(),
		"total_data_mb", fmt.Sprintf("%.2f", float64(o.collector.TotalBytes())/(1024*1024)),
	)

	report := o.GenerateReport(elapsed)
	reportPath := fmt.Sprintf("%s/report_%s.md", o.cfg.Metrics.OutputDir, time.Now().Format("20060102_150405"))
	if writeErr := os.WriteFile(reportPath, []byte(report), 0600); writeErr != nil {
		o.logger.Error("failed to write report", "err", writeErr)
	} else {
		o.logger.Info("report written", "path", reportPath)
	}

	o.poolMgr.Close(context.Background())
	return err
}

func (o *Orchestrator) GenerateReport(elapsed time.Duration) string {
	snapshots := o.collector.AllSnapshots()

	report := "# MongoDB AI Chatbot Benchmark Report\n\n"
	report += fmt.Sprintf("**Date**: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	report += fmt.Sprintf("**Duration**: %s\n", elapsed.Round(time.Second))
	report += fmt.Sprintf("**Database**: %s\n\n", o.cfg.MongoDB.Database)

	report += "## Summary\n\n"
	report += fmt.Sprintf("| Metric | Value |\n")
	report += fmt.Sprintf("|--------|-------|\n")
	report += fmt.Sprintf("| Total Operations | %d |\n", o.collector.TotalOps())
	report += fmt.Sprintf("| Total Writes | %d |\n", o.collector.TotalWriteOps())
	report += fmt.Sprintf("| Total Reads | %d |\n", o.collector.TotalReadOps())
	report += fmt.Sprintf("| Total Errors | %d |\n", o.collector.TotalErrors())
	report += fmt.Sprintf("| Error Rate | %.4f%% |\n", errorRate(o.collector.TotalErrors(), o.collector.TotalOps()))
	report += fmt.Sprintf("| Total Data Written | %.2f MB |\n", float64(o.collector.TotalBytes())/(1024*1024))
	report += fmt.Sprintf("| Avg Throughput | %.0f ops/sec |\n", float64(o.collector.TotalOps())/elapsed.Seconds())
	report += fmt.Sprintf("| Avg Write Throughput | %.0f ops/sec |\n", float64(o.collector.TotalWriteOps())/elapsed.Seconds())
	report += fmt.Sprintf("| Avg Read Throughput | %.0f ops/sec |\n\n", float64(o.collector.TotalReadOps())/elapsed.Seconds())

	report += "## Latency by Operation\n\n"
	report += "| Operation | P50 (ms) | P95 (ms) | P99 (ms) | P99.9 (ms) | Max (ms) | Count |\n"
	report += "|-----------|----------|----------|----------|------------|----------|-------|\n"
	for _, s := range snapshots {
		report += fmt.Sprintf("| %s | %.2f | %.2f | %.2f | %.2f | %.2f | %d |\n",
			s.Operation, s.E2E.P50, s.E2E.P95, s.E2E.P99, s.E2E.P999, s.E2E.Max, s.TotalCount)
	}
	report += "\n"

	hourly := o.collector.HourlyBreakdown()
	if len(hourly) > 0 && len(hourly[0].Buckets) > 0 {
		report += "## Hourly Breakdown\n\n"
		for _, ob := range hourly {
			report += fmt.Sprintf("### %s\n\n", ob.Operation)
			report += "| Hour | Time Range | Ops | Ops/sec | Errors | P50 (ms) | P95 (ms) | P99 (ms) | Max (ms) | Data (MB) |\n"
			report += "|------|------------|-----|---------|--------|----------|----------|----------|----------|-----------|\n"
			for _, b := range ob.Buckets {
				dur := b.HourEnd.Sub(b.HourStart).Seconds()
				opsPerSec := 0.0
				if dur > 0 {
					opsPerSec = float64(b.Ops) / dur
				}
				report += fmt.Sprintf("| %d | %s - %s | %d | %.0f | %d | %.2f | %.2f | %.2f | %.2f | %.2f |\n",
					b.Hour,
					b.HourStart.Format("15:04:05"),
					b.HourEnd.Format("15:04:05"),
					b.Ops, opsPerSec, b.Errors,
					b.E2E.P50, b.E2E.P95, b.E2E.P99, b.E2E.Max,
					b.BytesMB)
			}
			report += "\n"
		}
	}

	poolStats := o.poolMgr.GetPoolStats()
	report += "## Connection Pool\n\n"
	report += fmt.Sprintf("| Metric | Value |\n")
	report += fmt.Sprintf("|--------|-------|\n")
	report += fmt.Sprintf("| Max Pool Size | %d |\n", o.cfg.MongoDB.MaxPoolSize)
	report += fmt.Sprintf("| Final Total Connections | %d |\n", poolStats.TotalConns)
	report += fmt.Sprintf("| Final Checked Out | %d |\n", poolStats.CheckedOut)
	report += fmt.Sprintf("| Final Available | %d |\n\n", poolStats.Available)

	if o.collStatsWriter != nil {
		collStats := o.collStatsWriter.Latest()
		if len(collStats) > 0 {
			report += "## Collection Stats\n\n"
			report += "| Collection | Documents | Storage (MB) | Index (MB) | Avg Doc (bytes) | Indexes |\n"
			report += "|------------|-----------|--------------|------------|-----------------|--------|\n"
			for _, cs := range collStats {
				report += fmt.Sprintf("| %s | %d | %.2f | %.2f | %d | %d |\n",
					cs.Collection, cs.Documents, cs.StorageMB, cs.IndexMB, cs.AvgDocSize, cs.Indexes)
			}
			report += "\n"
		}
	}

	recentErrs := o.collector.RecentErrors()
	if len(recentErrs) > 0 {
		report += "## Recent Errors (last 10)\n\n"
		report += "| Timestamp | Operation | Error |\n"
		report += "|-----------|-----------|-------|\n"
		for _, e := range recentErrs {
			msg := e.Message
			if len(msg) > 200 {
				msg = msg[:197] + "..."
			}
			report += fmt.Sprintf("| %s | %s | %s |\n",
				e.Timestamp.Format("15:04:05.000"), e.Operation, msg)
		}
		report += "\n"
	}

	report += "## Configuration\n\n"
	report += fmt.Sprintf("| Setting | Value |\n")
	report += fmt.Sprintf("|---------|-------|\n")
	report += fmt.Sprintf("| Write Concern | %s |\n", o.cfg.MongoDB.WriteConcern)
	report += fmt.Sprintf("| Read Preference | %s |\n", o.cfg.MongoDB.ReadPreference)
	report += fmt.Sprintf("| Max Pool Size | %d |\n", o.cfg.MongoDB.MaxPoolSize)
	report += fmt.Sprintf("| Track Conversations | %v |\n", o.cfg.Workload.ConversationsEnabled())
	report += fmt.Sprintf("| Phases | %d |\n", len(o.cfg.Phases))
	for _, p := range o.cfg.Phases {
		report += fmt.Sprintf("| Phase: %s | VUs=%d, Duration=%s, Ramp=%s |\n", p.Name, p.TargetVirtualUsers, p.Duration, p.Ramp)
	}

	return report
}

func (d *Dashboard) Start() {
	go d.loop()
}

func (d *Dashboard) loop() {
	defer close(d.doneCh)
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.printStats()
		case <-d.stopCh:
			return
		}
	}
}

func (d *Dashboard) printStats() {
	snapshots := d.collector.AllSnapshots()
	poolStats := d.poolMgr.GetPoolStats()
	elapsed := d.collector.Elapsed()
	activeVUs := d.runner.ActiveVUs()

	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Phase: %-20s | VUs: %-6d | Elapsed: %s\n", d.getPhase(), activeVUs, elapsed.Round(time.Second))
	fmt.Printf("  Total Ops: %-10d | Writes: %-10d | Reads: %-10d | Errors: %d\n",
		d.collector.TotalOps(), d.collector.TotalWriteOps(), d.collector.TotalReadOps(), d.collector.TotalErrors())
	fmt.Printf("  Pool: total=%d checked_out=%d available=%d\n",
		poolStats.TotalConns, poolStats.CheckedOut, poolStats.Available)
	fmt.Printf("  Data Written: %.2f MB\n", float64(d.collector.TotalBytes())/(1024*1024))
	fmt.Println("  ┌──────────────────────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐")
	fmt.Println("  │ Operation                    │  P50 ms  │  P95 ms  │  P99 ms  │ P99.9 ms │  Max ms  │ Ops/sec  │")
	fmt.Println("  ├──────────────────────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤")
	for _, s := range snapshots {
		windowDuration := s.WindowEnd.Sub(s.WindowStart)
		throughput := 0.0
		if windowDuration > 0 {
			throughput = float64(s.WindowOps) / windowDuration.Seconds()
		}
		fmt.Printf("  │ %-28s │ %8.2f │ %8.2f │ %8.2f │ %8.2f │ %8.2f │ %8.0f │\n",
			s.Operation, s.E2E.P50, s.E2E.P95, s.E2E.P99, s.E2E.P999, s.E2E.Max, throughput)
	}
	fmt.Println("  └──────────────────────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘")

	totalErrors := d.collector.TotalErrors()
	if totalErrors > 0 {
		errRate := float64(totalErrors) / float64(d.collector.TotalOps()) * 100
		fmt.Printf("\n  ⚠ ERRORS: %d total (%.3f%%)\n", totalErrors, errRate)
		recentErrs := d.collector.RecentErrors()
		if len(recentErrs) > 0 {
			shown := len(recentErrs)
			if shown > 5 {
				shown = 5
			}
			fmt.Printf("  Last %d errors:\n", shown)
			for i := len(recentErrs) - shown; i < len(recentErrs); i++ {
				e := recentErrs[i]
				msg := e.Message
				if len(msg) > 120 {
					msg = msg[:117] + "..."
				}
				fmt.Printf("    [%s] %s: %s\n",
					e.Timestamp.Format("15:04:05"), e.Operation, msg)
			}
		}
	}
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

func (d *Dashboard) SetPhase(phase string) {
	d.mu.Lock()
	d.phase = phase
	d.mu.Unlock()
}

func (d *Dashboard) getPhase() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.phase
}

func (d *Dashboard) Stop() {
	close(d.stopCh)
	<-d.doneCh
}

func errorRate(errors, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(errors) / float64(total) * 100
}

func maskURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return "***"
	}
	u.User = nil
	return u.String()
}
