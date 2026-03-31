package data

var ShortResponses = []string{
	"Sure! Here's a quick answer: ",
	"The answer is straightforward - ",
	"Here you go: ",
	"That's a great question. ",
	"Absolutely! ",
	"Let me help with that. ",
}

var MediumResponseTemplates = []string{
	`Great question! Here's a detailed explanation:

%s

The key takeaway is that you need to consider %s when making this decision. The recommended approach is to %s, which gives you the best balance of performance and maintainability.

Let me know if you'd like me to elaborate on any of these points!`,

	`Here's how I'd approach this:

**Step 1: %s**
First, you'll want to %s. This ensures that %s.

**Step 2: %s**
Next, %s. This is important because %s.

**Step 3: %s**
Finally, %s. This gives you %s.

Would you like me to provide code examples for any of these steps?`,

	`That's a common challenge. Here's what I recommend:

The main issue is %s. This happens because %s.

**Solution:**
` + "```" + `
// %s
func solve(ctx context.Context) error {
    // Implementation approach: %s
    result, err := process(ctx)
    if err != nil {
        return fmt.Errorf("%s: %%w", err)
    }
    return nil
}
` + "```" + `

This approach works because %s. The time complexity is O(%s) and space complexity is O(%s).`,
}

var LongResponseTemplates = []string{
	`# Comprehensive Guide: %s

## Overview
%s

## Architecture

The recommended architecture follows these principles:

1. **Separation of Concerns**: %s
2. **Scalability**: %s
3. **Resilience**: %s

## Implementation Details

### Database Layer
` + "```go" + `
type Repository struct {
    db     *mongo.Database
    coll   *mongo.Collection
    pool   *pool.Manager
}

func NewRepository(db *mongo.Database) *Repository {
    return &Repository{
        db:   db,
        coll: db.Collection("%s"),
    }
}

func (r *Repository) Create(ctx context.Context, doc interface{}) error {
    _, err := r.coll.InsertOne(ctx, doc)
    return err
}

func (r *Repository) FindByID(ctx context.Context, id primitive.ObjectID) (*Document, error) {
    var doc Document
    err := r.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
    if err != nil {
        return nil, err
    }
    return &doc, nil
}
` + "```" + `

### Service Layer
` + "```go" + `
type Service struct {
    repo   *Repository
    cache  *redis.Client
    logger *slog.Logger
}

func (s *Service) Process(ctx context.Context, req *Request) (*Response, error) {
    // Check cache first
    if cached, err := s.cache.Get(ctx, req.CacheKey()).Result(); err == nil {
        return unmarshal(cached)
    }

    // Fetch from database
    result, err := s.repo.Find(ctx, req.Filter())
    if err != nil {
        return nil, fmt.Errorf("fetching data: %%w", err)
    }

    // Cache the result
    s.cache.Set(ctx, req.CacheKey(), marshal(result), 5*time.Minute)

    return &Response{Data: result}, nil
}
` + "```" + `

### API Layer
%s

## Performance Considerations

| Metric | Target | Actual |
|--------|--------|--------|
| Latency P50 | < 10ms | %s |
| Latency P99 | < 100ms | %s |
| Throughput | %s ops/sec | %s ops/sec |
| Error Rate | < 0.01%% | %s |

## Deployment

%s

## Conclusion

%s`,
}

var WebSearchResults = []map[string]string{
	{"url": "https://www.mongodb.com/docs/manual/core/aggregation-pipeline/", "title": "Aggregation Pipeline — MongoDB Manual", "preview": "The aggregation pipeline is a framework for data aggregation..."},
	{"url": "https://docs.docker.com/get-started/", "title": "Docker Getting Started Guide", "preview": "Learn how to build and share containerized applications..."},
	{"url": "https://kubernetes.io/docs/concepts/overview/", "title": "Kubernetes Overview", "preview": "Kubernetes is a portable, extensible, open source platform for managing containerized workloads..."},
	{"url": "https://go.dev/doc/effective_go", "title": "Effective Go - The Go Programming Language", "preview": "This document gives tips for writing clear, idiomatic Go code..."},
	{"url": "https://react.dev/learn", "title": "Quick Start – React", "preview": "Welcome to the React documentation! This page will give you an introduction to the 80%% of React concepts..."},
	{"url": "https://www.postgresql.org/docs/current/mvcc.html", "title": "PostgreSQL: Documentation: Concurrency Control", "preview": "PostgreSQL provides a rich set of tools for developers to manage concurrent access to data..."},
	{"url": "https://redis.io/docs/latest/develop/", "title": "Develop with Redis", "preview": "Connect your application to a Redis database and try an example..."},
	{"url": "https://aws.amazon.com/blogs/architecture/", "title": "AWS Architecture Blog", "preview": "Learn about AWS architecture best practices, reference architectures..."},
	{"url": "https://openai.com/research", "title": "OpenAI Research", "preview": "We research generative models and how to align them with human values..."},
	{"url": "https://www.terraform.io/docs", "title": "Terraform Documentation", "preview": "Terraform is an infrastructure as code tool that lets you define resources in human-readable configuration files..."},
}

var TechnicalParagraphs = []string{
	"When designing distributed systems, it's crucial to understand the trade-offs between consistency, availability, and partition tolerance. In practice, most modern systems opt for eventual consistency with conflict resolution strategies.",
	"Connection pooling is essential for high-throughput database operations. Without proper pool management, each request creates a new TCP connection, which involves a three-way handshake, TLS negotiation, and authentication — adding 10-50ms of overhead per request.",
	"The B-tree index structure used by MongoDB provides O(log n) lookup performance, making it suitable for range queries and equality matches. However, index maintenance during writes adds overhead proportional to the number of indexes.",
	"Horizontal scaling through sharding distributes data across multiple nodes. The choice of shard key is critical — a poor shard key leads to hot spots where one shard handles disproportionate traffic.",
	"Write-ahead logging (WAL) ensures durability by persisting changes to a log before applying them to the main data files. This allows recovery after crashes without data loss.",
	"Memory-mapped files allow the operating system to manage the buffer pool, trading application-level control for simplicity. MongoDB's WiredTiger storage engine uses its own cache management for more predictable performance.",
	"Lock-free data structures using CAS (Compare-And-Swap) operations provide better concurrent throughput than mutex-based approaches, but they're significantly more complex to implement correctly.",
	"The actor model provides a powerful abstraction for concurrent programming, where each actor processes messages sequentially, eliminating shared-state concurrency issues.",
}

var StepDescriptors = []string{
	"Set up the infrastructure",
	"Configure the database",
	"Implement the core logic",
	"Add error handling",
	"Write comprehensive tests",
	"Set up monitoring",
	"Deploy to production",
	"Configure auto-scaling",
}

var TopicNames = []string{
	"Distributed Database Design",
	"Microservices Architecture",
	"Real-time Data Processing",
	"High-Availability Systems",
	"Performance Optimization",
	"Security Best Practices",
	"Event-Driven Architecture",
	"Cloud-Native Development",
}

var PerformanceValues = []string{
	"5ms", "8ms", "12ms", "15ms", "25ms", "50ms", "75ms", "100ms",
}

var ThroughputValues = []string{
	"1K", "5K", "10K", "25K", "50K", "100K",
}
