package data

var HumanPrompts = []string{
	// Greetings
	"Good morning!",
	"Hey there, how are you?",
	"Hi! I need some help.",
	"Hello!",

	// Quick questions
	"What's the weather like today?",
	"Can you summarize the top news?",
	"What time is it in Tokyo?",
	"How do I reset my password?",
	"What's 2+2?",
	"Tell me a joke.",

	// Coding requests
	"Write a Python function to sort a list of dictionaries by a specific key.",
	"How do I implement a binary search tree in Go?",
	"Can you help me debug this React component? It's not rendering properly.",
	"Write a SQL query to find the top 10 customers by revenue.",
	"How do I set up a CI/CD pipeline with GitHub Actions?",
	"Explain the difference between a mutex and a semaphore.",
	"Write a Dockerfile for a Node.js application with multi-stage builds.",
	"How do I implement connection pooling in PostgreSQL?",
	"Write a unit test for this function using pytest.",
	"How do I optimize this MongoDB aggregation pipeline?",
	"Can you review this code for security vulnerabilities?",
	"Write a bash script to back up a MySQL database.",
	"How do I implement rate limiting in an Express.js API?",
	"Explain how garbage collection works in Java.",
	"Write a Terraform module for an AWS VPC.",

	// Analysis requests
	"Analyze the performance implications of using MongoDB vs PostgreSQL for write-heavy workloads.",
	"Compare REST vs GraphQL for a mobile application backend.",
	"What are the best practices for designing a microservices architecture?",
	"Explain the CAP theorem and how it applies to MongoDB.",
	"What are the trade-offs between eventual consistency and strong consistency?",

	// Summarization
	"summarize top news for today",
	"Give me a summary of the latest AI research papers.",
	"Summarize the key points from this meeting transcript.",
	"What are the main takeaways from the quarterly earnings report?",

	// Creative
	"Write a short story about a robot learning to cook.",
	"Help me draft an email to my team about the project deadline.",
	"Write a product description for a new smartwatch.",
	"Create a marketing tagline for an eco-friendly water bottle.",

	// Long-form requests
	"I'm building a real-time chat application. Can you help me design the database schema? We need to support group chats, direct messages, read receipts, and message reactions. The system should handle about 10 million active users.",
	"Explain the internals of the Linux kernel's process scheduler. How does CFS work? What are the key data structures involved?",
	"Write a comprehensive guide on implementing OAuth 2.0 with PKCE flow in a single-page application. Include code examples for both the frontend and backend.",
	"I need to migrate our monolithic application to microservices. We currently have a Django app with about 200 models and 50 API endpoints. Where do I start?",
	"Design a distributed task queue system similar to Celery but in Go. It should support priority queues, retry logic, dead letter queues, and horizontal scaling.",

	// Follow-ups
	"Can you explain that in more detail?",
	"What about error handling?",
	"How would this work at scale?",
	"Can you give me a concrete example?",
	"What are the alternatives?",
	"Is there a simpler way to do this?",
	"How do I test this?",
	"What about security considerations?",
	"Can you refactor this to be more efficient?",
	"What would the production deployment look like?",
}

var LongPromptTemplates = []string{
	`I'm working on a %s application that needs to handle %s. The current architecture uses %s but we're seeing performance issues at %s requests per second. The main bottleneck seems to be %s. Can you suggest a better approach? Here's the relevant code:

` + "```" + `
func processRequest(ctx context.Context, req *Request) (*Response, error) {
    // Current implementation
    data, err := db.Query(ctx, req.Query)
    if err != nil {
        return nil, fmt.Errorf("query failed: %%w", err)
    }
    
    result := transform(data)
    
    if err := cache.Set(ctx, req.Key, result, 5*time.Minute); err != nil {
        log.Printf("cache set failed: %%v", err)
    }
    
    return &Response{Data: result}, nil
}
` + "```" + `

We're running on %s with %s of RAM. The database is %s with %s of data.`,

	`Our team is debating between %s and %s for our new %s service. Here are our requirements:

1. We need to handle %s concurrent connections
2. Average response time should be under %s
3. We need %s data consistency
4. The system must support %s
5. We're planning to scale to %s users in the next year

What would you recommend and why? Please include concrete benchmarks or references if possible.`,

	`I've been debugging this issue for hours. Our %s service is experiencing %s under load. Here's what I've observed:

- CPU usage spikes to %s when we hit %s requests/sec
- Memory grows linearly and never gets freed
- The %s logs show timeouts after %s seconds
- We've tried %s but it didn't help
- The problem started after we deployed %s

Environment:
- OS: %s
- Runtime: %s
- Database: %s
- Load balancer: %s

Can you help me identify the root cause?`,
}

var ApplicationTypes = []string{
	"e-commerce", "fintech", "healthcare", "social media", "gaming",
	"IoT", "real-time analytics", "content management", "logistics", "SaaS",
}

var ScaleDescriptors = []string{
	"10K", "50K", "100K", "500K", "1M", "5M", "10M",
}

var TechStacks = []string{
	"Go with MongoDB", "Python with PostgreSQL", "Node.js with Redis",
	"Java with Cassandra", "Rust with ScyllaDB", "Kotlin with DynamoDB",
}

var BottleneckTypes = []string{
	"database connection pooling", "memory allocation", "GC pauses",
	"network I/O", "disk writes", "CPU-bound serialization", "lock contention",
}

var InfraDescriptors = []string{
	"AWS c6i.xlarge instances", "GCP n2-standard-8", "bare metal servers",
	"Kubernetes pods with 4 CPU cores", "Azure D4s v3 VMs",
}

var MemoryDescriptors = []string{"8GB", "16GB", "32GB", "64GB", "128GB"}

var DatabaseTypes = []string{
	"MongoDB Atlas M50", "PostgreSQL RDS r6g.xlarge", "MySQL Aurora",
	"Redis Cluster (6 nodes)", "DynamoDB (on-demand)",
}

var DataSizes = []string{"50GB", "200GB", "500GB", "1TB", "5TB"}
