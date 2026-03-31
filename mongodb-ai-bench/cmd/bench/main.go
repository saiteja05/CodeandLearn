package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/mongodb-ai-bench/internal/config"
	"github.com/mongodb-ai-bench/internal/runner"
)

func main() {
	configPath := flag.String("config", "configs/default.yaml", "path to benchmark config file")
	flag.Parse()

	printBanner()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("loading config", "path", *configPath)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	slog.Info("config loaded",
		"database", cfg.MongoDB.Database,
		"phases", len(cfg.Phases),
		"max_pool_size", cfg.MongoDB.MaxPoolSize,
		"write_concern", cfg.MongoDB.WriteConcern,
	)

	orchestrator, err := runner.NewOrchestrator(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create orchestrator: %v\n", err)
		os.Exit(1)
	}

	if err := orchestrator.Run(); err != nil {
		slog.Error("benchmark failed", "err", err)
		os.Exit(1)
	}

	slog.Info("benchmark completed successfully")
}
