package pool

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/mongodb-ai-bench/internal/config"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type PoolStats struct {
	CheckedOut int64
	TotalConns int64
	Available  int64
}

type Manager struct {
	client   *mongo.Client
	database *mongo.Database
	cfg      config.MongoDBConfig

	checkedOut atomic.Int64
	totalConns atomic.Int64
}

func NewManager(cfg config.MongoDBConfig) (*Manager, error) {
	m := &Manager{
		cfg: cfg,
	}

	clientOpts, err := m.buildClientOptions()
	if err != nil {
		return nil, fmt.Errorf("building client options: %w", err)
	}

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB: %w", err)
	}

	m.client = client
	m.database = client.Database(cfg.Database)

	return m, nil
}

func (m *Manager) buildClientOptions() (*options.ClientOptions, error) {
	connIdleTime, _ := m.cfg.ParsedMaxConnIdleTime()
	connectTimeout, _ := m.cfg.ParsedConnectTimeout()
	socketTimeout, _ := m.cfg.ParsedSocketTimeout()

	poolMonitor := &event.PoolMonitor{
		Event: func(e *event.PoolEvent) {
			switch e.Type {
			case event.ConnectionCheckedOut:
				m.checkedOut.Add(1)
			case event.ConnectionCheckedIn:
				m.checkedOut.Add(-1)
			case event.ConnectionCreated:
				m.totalConns.Add(1)
			case event.ConnectionClosed:
				m.totalConns.Add(-1)
			}
		},
	}

	opts := options.Client().
		ApplyURI(m.cfg.URI).
		SetMaxPoolSize(m.cfg.MaxPoolSize).
		SetMinPoolSize(m.cfg.MinPoolSize).
		SetMaxConnIdleTime(connIdleTime).
		SetConnectTimeout(connectTimeout).
		SetTimeout(socketTimeout).
		SetPoolMonitor(poolMonitor).
		SetCompressors([]string{"zstd", "snappy", "zlib"})

	wc, err := parseWriteConcern(m.cfg.WriteConcern)
	if err != nil {
		return nil, err
	}
	opts.SetWriteConcern(wc)

	rp, err := parseReadPreference(m.cfg.ReadPreference)
	if err != nil {
		return nil, err
	}
	opts.SetReadPreference(rp)

	return opts, nil
}

func parseWriteConcern(wc string) (*writeconcern.WriteConcern, error) {
	switch wc {
	case "majority":
		return writeconcern.Majority(), nil
	default:
		w, err := strconv.Atoi(wc)
		if err != nil {
			return nil, fmt.Errorf("invalid write concern %q: must be 'majority' or an integer", wc)
		}
		return &writeconcern.WriteConcern{W: w}, nil
	}
}

func parseReadPreference(rp string) (*readpref.ReadPref, error) {
	switch rp {
	case "primary":
		return readpref.Primary(), nil
	case "primaryPreferred":
		return readpref.PrimaryPreferred(), nil
	case "secondary":
		return readpref.Secondary(), nil
	case "secondaryPreferred":
		return readpref.SecondaryPreferred(), nil
	case "nearest":
		return readpref.Nearest(), nil
	default:
		return nil, fmt.Errorf("invalid read preference %q", rp)
	}
}

func (m *Manager) Ping(ctx context.Context) error {
	return m.client.Ping(ctx, nil)
}

func (m *Manager) Database() *mongo.Database {
	return m.database
}

func (m *Manager) Client() *mongo.Client {
	return m.client
}

func (m *Manager) ConversationsCollection() *mongo.Collection {
	return m.database.Collection("conversations")
}

func (m *Manager) MessagesCollection() *mongo.Collection {
	return m.database.Collection("messages")
}

func (m *Manager) GetPoolStats() PoolStats {
	total := m.totalConns.Load()
	checked := m.checkedOut.Load()
	return PoolStats{
		CheckedOut: checked,
		TotalConns: total,
		Available:  total - checked,
	}
}

type CollectionStatsResult struct {
	Collection string
	Documents  int64
	StorageMB  float64
	IndexMB    float64
	AvgDocSize int64
	Indexes    int
}

func (m *Manager) CollectionStats(ctx context.Context, collName string) (CollectionStatsResult, error) {
	result := m.database.RunCommand(ctx, bson.D{{Key: "collStats", Value: collName}})

	var raw bson.M
	if err := result.Decode(&raw); err != nil {
		return CollectionStatsResult{}, fmt.Errorf("collStats %s: %w", collName, err)
	}

	return CollectionStatsResult{
		Collection: collName,
		Documents:  toInt64(raw["count"]),
		StorageMB:  float64(toInt64(raw["storageSize"])) / (1024 * 1024),
		IndexMB:    float64(toInt64(raw["totalIndexSize"])) / (1024 * 1024),
		AvgDocSize: toInt64(raw["avgObjSize"]),
		Indexes:    int(toInt64(raw["nindexes"])),
	}, nil
}

func (m *Manager) AllCollectionStats(ctx context.Context, conversationsEnabled bool) []CollectionStatsResult {
	var results []CollectionStatsResult
	if cs, err := m.CollectionStats(ctx, "messages"); err == nil {
		results = append(results, cs)
	}
	if conversationsEnabled {
		if cs, err := m.CollectionStats(ctx, "conversations"); err == nil {
			results = append(results, cs)
		}
	}
	return results
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func (m *Manager) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

func (m *Manager) EnsureIndexes(ctx context.Context, conversationsEnabled bool) error {
	msgColl := m.MessagesCollection()
	_, err := msgColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "conversation_id", Value: 1},
				{Key: "create_time", Value: 1},
			},
		},
		{
			Keys: bson.D{{Key: "create_time", Value: -1}},
		},
	})
	if err != nil {
		return fmt.Errorf("creating message indexes: %w", err)
	}

	if conversationsEnabled {
		convColl := m.ConversationsCollection()
		_, err = convColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
			{
				Keys: bson.D{
					{Key: "user_id", Value: 1},
					{Key: "created_at", Value: -1},
				},
			},
		})
		if err != nil {
			return fmt.Errorf("creating conversation indexes: %w", err)
		}
	}

	return nil
}
