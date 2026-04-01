package config

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MongoDB  MongoDBConfig  `yaml:"mongodb"`
	Workload WorkloadConfig `yaml:"workload"`
	Phases   []Phase        `yaml:"phases"`
	Metrics  MetricsConfig  `yaml:"metrics"`
}

type MongoDBConfig struct {
	URI             string `yaml:"uri"`
	Database        string `yaml:"database"`
	WriteConcern    string `yaml:"write_concern"`
	ReadPreference  string `yaml:"read_preference"`
	MaxPoolSize     uint64 `yaml:"max_pool_size"`
	MinPoolSize     uint64 `yaml:"min_pool_size"`
	MaxConnIdleTime string `yaml:"max_conn_idle_time"`
	ConnectTimeout  string `yaml:"connect_timeout"`
	SocketTimeout   string `yaml:"socket_timeout"`
}

type WorkloadConfig struct {
	ContinueConversationPct  int                      `yaml:"continue_conversation_pct"`
	WebSearchPct             int                      `yaml:"web_search_pct"`
	MaxHistoryMessages       int                      `yaml:"max_history_messages"`
	TrackConversations       *bool                    `yaml:"track_conversations"`
	Models                   []string                 `yaml:"models"`
	ResponseSizeDistribution ResponseSizeDistribution `yaml:"response_size_distribution"`
}

func (w WorkloadConfig) ConversationsEnabled() bool {
	if w.TrackConversations == nil {
		return true
	}
	return *w.TrackConversations
}

type ResponseSizeDistribution struct {
	ShortPct    int `yaml:"short_pct"`
	MediumPct   int `yaml:"medium_pct"`
	LongPct     int `yaml:"long_pct"`
	VeryLongPct int `yaml:"very_long_pct"`
}

type Phase struct {
	Name               string `yaml:"name"`
	Duration           string `yaml:"duration"`
	TargetVirtualUsers int    `yaml:"target_virtual_users"`
	Ramp               string `yaml:"ramp"`
}

type MetricsConfig struct {
	OutputDir               string `yaml:"output_dir"`
	CSVInterval             string `yaml:"csv_interval"`
	StatsInterval           string `yaml:"stats_interval"`
	DashboardEnabled        bool   `yaml:"dashboard_enabled"`
	CollectionStatsEnabled  bool   `yaml:"collection_stats_enabled"`
	CollectionStatsInterval string `yaml:"collection_stats_interval"`
}

func (p Phase) ParsedDuration() (time.Duration, error) {
	return time.ParseDuration(p.Duration)
}

func (m MongoDBConfig) ParsedMaxConnIdleTime() (time.Duration, error) {
	return time.ParseDuration(m.MaxConnIdleTime)
}

func (m MongoDBConfig) ParsedConnectTimeout() (time.Duration, error) {
	return time.ParseDuration(m.ConnectTimeout)
}

func (m MongoDBConfig) ParsedSocketTimeout() (time.Duration, error) {
	return time.ParseDuration(m.SocketTimeout)
}

func (m MetricsConfig) ParsedCSVInterval() (time.Duration, error) {
	return time.ParseDuration(m.CSVInterval)
}

func (m MetricsConfig) ParsedStatsInterval() (time.Duration, error) {
	return time.ParseDuration(m.StatsInterval)
}

func (m MetricsConfig) ParsedCollectionStatsInterval() (time.Duration, error) {
	return time.ParseDuration(m.CollectionStatsInterval)
}

func Load(path string) (*Config, error) {
	ext := strings.ToLower(filepath.Ext(path))
	if ext != ".yaml" && ext != ".yml" {
		return nil, fmt.Errorf("config file must have .yaml or .yml extension, got %q", ext)
	}

	loadEnvFile(".env")

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config YAML: %w", err)
	}

	// Environment variable overrides YAML — keeps secrets out of config files
	if envURI := os.Getenv("MONGODB_URI"); envURI != "" {
		cfg.MongoDB.URI = envURI
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return &cfg, nil
}

// allowedEnvKeys defines the only environment variables loadEnvFile is permitted to set.
// This prevents a malicious .env file from overriding security-sensitive vars like PATH.
var allowedEnvKeys = map[string]bool{
	"MONGODB_URI": true,
}

// loadEnvFile reads a .env file and sets only whitelisted environment variables.
// Must only be called from the main goroutine before any goroutines are spawned.
func loadEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if !allowedEnvKeys[key] {
			continue
		}
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}

func (c *Config) Validate() error {
	if c.MongoDB.URI == "" {
		return fmt.Errorf("mongodb.uri is required")
	}
	if c.MongoDB.Database == "" {
		return fmt.Errorf("mongodb.database is required")
	}
	if c.MongoDB.MaxPoolSize == 0 {
		c.MongoDB.MaxPoolSize = 100
	}
	if c.MongoDB.MinPoolSize == 0 {
		c.MongoDB.MinPoolSize = 10
	}
	if c.MongoDB.MaxConnIdleTime == "" {
		c.MongoDB.MaxConnIdleTime = "30s"
	}
	if c.MongoDB.ConnectTimeout == "" {
		c.MongoDB.ConnectTimeout = "10s"
	}
	if c.MongoDB.SocketTimeout == "" {
		c.MongoDB.SocketTimeout = "30s"
	}
	if c.MongoDB.WriteConcern == "" {
		c.MongoDB.WriteConcern = "1"
		slog.Warn("write_concern defaulted to 1: writes acknowledged before replication; set to \"majority\" for durability")
	}
	if c.MongoDB.ReadPreference == "" {
		c.MongoDB.ReadPreference = "primaryPreferred"
	}

	if _, err := c.MongoDB.ParsedMaxConnIdleTime(); err != nil {
		return fmt.Errorf("invalid mongodb.max_conn_idle_time: %w", err)
	}
	if _, err := c.MongoDB.ParsedConnectTimeout(); err != nil {
		return fmt.Errorf("invalid mongodb.connect_timeout: %w", err)
	}
	if _, err := c.MongoDB.ParsedSocketTimeout(); err != nil {
		return fmt.Errorf("invalid mongodb.socket_timeout: %w", err)
	}

	if len(c.Phases) == 0 {
		return fmt.Errorf("at least one phase is required")
	}
	for i, p := range c.Phases {
		if p.Name == "" {
			return fmt.Errorf("phase[%d].name is required", i)
		}
		if _, err := p.ParsedDuration(); err != nil {
			return fmt.Errorf("phase[%d].duration is invalid: %w", i, err)
		}
		if p.TargetVirtualUsers <= 0 {
			return fmt.Errorf("phase[%d].target_virtual_users must be > 0", i)
		}
		switch p.Ramp {
		case "linear", "none", "step":
		default:
			return fmt.Errorf("phase[%d].ramp must be linear, step, or none; got %q", i, p.Ramp)
		}
	}

	dist := c.Workload.ResponseSizeDistribution
	total := dist.ShortPct + dist.MediumPct + dist.LongPct + dist.VeryLongPct
	if total != 0 && total != 100 {
		return fmt.Errorf("response_size_distribution percentages must sum to 100, got %d", total)
	}
	if total == 0 {
		c.Workload.ResponseSizeDistribution = ResponseSizeDistribution{
			ShortPct: 30, MediumPct: 40, LongPct: 20, VeryLongPct: 10,
		}
	}

	if c.Workload.ContinueConversationPct == 0 {
		c.Workload.ContinueConversationPct = 70
	}
	if c.Workload.ContinueConversationPct < 0 || c.Workload.ContinueConversationPct > 100 {
		return fmt.Errorf("continue_conversation_pct must be 0-100, got %d", c.Workload.ContinueConversationPct)
	}
	if c.Workload.WebSearchPct == 0 {
		c.Workload.WebSearchPct = 10
	}
	if c.Workload.WebSearchPct < 0 || c.Workload.WebSearchPct > 100 {
		return fmt.Errorf("web_search_pct must be 0-100, got %d", c.Workload.WebSearchPct)
	}
	if c.Workload.MaxHistoryMessages == 0 {
		c.Workload.MaxHistoryMessages = 500
	}
	if c.Workload.MaxHistoryMessages < 0 {
		return fmt.Errorf("max_history_messages must be positive, got %d", c.Workload.MaxHistoryMessages)
	}
	if len(c.Workload.Models) == 0 {
		c.Workload.Models = []string{"mongo-v1"}
	}

	if c.Metrics.OutputDir == "" {
		c.Metrics.OutputDir = "results"
	}
	if filepath.IsAbs(c.Metrics.OutputDir) {
		return fmt.Errorf("metrics.output_dir must be a relative path, got %q", c.Metrics.OutputDir)
	}
	if c.Metrics.CSVInterval == "" {
		c.Metrics.CSVInterval = "1s"
	}
	if c.Metrics.StatsInterval == "" {
		c.Metrics.StatsInterval = "10s"
	}
	if c.Metrics.CollectionStatsInterval == "" {
		c.Metrics.CollectionStatsInterval = "60s"
	}

	if _, err := c.Metrics.ParsedCSVInterval(); err != nil {
		return fmt.Errorf("invalid metrics.csv_interval: %w", err)
	}
	if _, err := c.Metrics.ParsedStatsInterval(); err != nil {
		return fmt.Errorf("invalid metrics.stats_interval: %w", err)
	}
	if _, err := c.Metrics.ParsedCollectionStatsInterval(); err != nil {
		return fmt.Errorf("invalid metrics.collection_stats_interval: %w", err)
	}

	return nil
}
