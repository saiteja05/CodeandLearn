# MongoDB AI Chatbot Benchmark

Production-scale progressive overload benchmarking tool for MongoDB, designed to prove MongoDB's suitability for write-heavy AI chatbot workloads (Claude Code, Cursor, ChatGPT, etc.).

Targets: **100K writes/sec**, **25K reads/sec**, running for **days** as data grows from KBs to TBs.

## Quick Start

### Local Run

```bash
# 1. Set your MongoDB URI (Atlas or local)
echo 'MONGODB_URI=mongodb+srv://<username>:<password>@cluster.mongodb.net/' > .env

# 2. Build and run the 5-minute smoke test
make quick-test
```

This builds the binary and runs it with `configs/quick-test.yaml` (30s warmup → 2m load → 2m peak, up to 100 VUs). Results are written to `results/`.

### Other Make Targets

```bash
make build          # build the binary only (bin/mongodb-ai-bench)
make run            # build and run with configs/default.yaml
make quick-test     # build and run with configs/quick-test.yaml (~5 min)
make full-run       # build and run with configs/full-run.yaml (multi-day)
make cross-linux    # cross-compile for Linux/AMD64 (for EC2 deployment)
make test           # run Go tests
make lint           # run golangci-lint
make clean          # remove bin/ and results/
make deps           # go mod tidy
```

### Deploy to EC2

```bash
# 1. Cross-compile for Linux
make cross-linux

# 2. Copy binary and config to EC2
scp bin/mongodb-ai-bench-linux-amd64 ec2-user@<host>:~/
scp configs/full-run.yaml ec2-user@<host>:~/

# 3. SSH in and run
ssh ec2-user@<host>
export MONGODB_URI="mongodb+srv://<username>:<password>@cluster.mongodb.net/"
./mongodb-ai-bench-linux-amd64 -config full-run.yaml
```

No Go installation needed on the EC2 instance — the binary is self-contained. The URI is read at runtime from the `MONGODB_URI` env var (or a `.env` file in the working directory).

## How It Works

The benchmark simulates **thousands of concurrent AI chatbot users**, each running an independent conversation loop against MongoDB. There's no batching or bulk writes — every operation is a single `InsertOne`, `Find`, or `UpdateOne`, exactly like a real chatbot backend.

### What Each Virtual User Does

Each virtual user (VU) is a Go goroutine that repeats this cycle:

```
1. Create Conversation     →  InsertOne to conversations collection (30% of turns)
2. Send Human Message      →  InsertOne to messages collection
3. Read Conversation History  →  Find + sort on messages collection
4. Write Assistant Response →  InsertOne to messages collection (200 bytes to 100KB)
5. Update Conversation     →  UpdateOne on conversations collection
6. Think Time              →  50-250ms pause, then loop
```

This produces a natural **4:1 write-to-read ratio**. Set `track_conversations: false` to skip steps 1 and 5 for a messages-only benchmark (2:1 ratio).

### The 5 Operation Types

| Operation | MongoDB Op | Collection | What It Simulates |
|-----------|-----------|------------|-------------------|
| `create_conversation` | `InsertOne` | `conversations` | User starts a new chat session |
| `write_human_message` | `InsertOne` | `messages` | User sends a prompt |
| `read_conversation_history` | `Find` (sorted) | `messages` | LLM backend loads context for next response |
| `write_assistant_message` | `InsertOne` | `messages` | AI response stored (variable size: 200B to 100KB) |
| `write_conversation_metadata` | `UpdateOne` | `conversations` | Update last_message_at and message_count |

### Key Settings To Get Started

| Setting | Where | What To Set |
|---------|-------|-------------|
| **MongoDB URI** | `.env` file or env var | `MONGODB_URI=mongodb+srv://<username>:<password>@cluster.mongodb.net/` |
| **Database name** | YAML `mongodb.database` | Any name — collections are created automatically |
| **Write concern** | YAML `mongodb.write_concern` | `"1"` for speed, `"majority"` for durability testing |
| **Track conversations** | YAML `workload.track_conversations` | `true` = full model (5 ops/turn), `false` = messages only (2 ops/turn) |
| **Load profile** | YAML `phases` | Each phase sets VU count + duration. More VUs = more load |
| **Pool size** | YAML `mongodb.max_pool_size` | `100` for quick tests, `500` for 25K VUs |

### What You Get After a Run

```
results/
├── timeseries_<timestamp>.csv   ← per-second latency, throughput, errors per operation
├── collstats_<timestamp>.csv    ← periodic doc count, storage size, index size (if enabled)
└── report_<timestamp>.md        ← full summary with:
                                      • Overall stats (total ops, throughput, error rate)
                                      • Latency percentiles per operation (P50/P95/P99/Max)
                                      • Hourly breakdown per operation (for correlating with cluster events)
                                      • Collection stats, connection pool, errors, config
```

### Progressive Overload

Load ramps through configurable phases. Example from `quick-test.yaml`:

```
Phase 1: warmup  →  0 to 10 VUs over 30s
Phase 2: load    →  10 to 50 VUs over 2m
Phase 3: peak    →  50 to 100 VUs over 2m
```

VU count carries across phases (no reset). For production runs, `full-run.yaml` ramps from 100 to 25,000 VUs over days.

---

## Architecture (Deep Dive)

### System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLI Entry Point                                │
│                          cmd/bench/main.go                                  │
│         parse flags → print banner → load config → run orchestrator         │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                             Orchestrator                                    │
│                     internal/runner/orchestrator.go                          │
│                                                                             │
│  Lifecycle: Ping → EnsureIndexes → Start CSV → Start Dashboard → Run Phases │
│             → Generate Report → Close Pool                                  │
│                                                                             │
│  Signal handler (SIGINT/SIGTERM) → graceful shutdown at any point           │
├─────────────┬──────────────┬──────────────────┬─────────────────────────────┤
│  Pool Mgr   │  Collector   │  TimeSeries CSV  │  Dashboard                  │
│  (pool/)    │  (metrics/)  │  (metrics/)      │  (runner/)                  │
└──────┬──────┴──────┬───────┴────────┬─────────┴──────────┬──────────────────┘
       │             │                │                    │
       │             ▼                │                    │
       │  ┌───────────────────┐      │                    │
       │  │ ProgressiveRunner │      │                    │
       │  │  (runner/)        │      │                    │
       │  │                   │      │                    │
       │  │ Phase 1: warmup   │      │                    │
       │  │ Phase 2: load     │──────┼──── onPhaseChange ─┘
       │  │ Phase 3: peak     │      │    (updates CSV + dashboard labels)
       │  │ ...               │      │
       │  └────────┬──────────┘      │
       │           │                 │
       │           ▼                 │
       │  ┌────────────────┐         │
       │  │   VU Manager   │         │
       │  │   (runner/)    │         │
       │  │                │         │
       │  │ ScaleTo(N) ────┤         │
       │  │ addVUs()       │         │
       │  │ removeVUs()    │         │
       │  │ StopAll()      │         │
       │  └───┬───┬───┬────┘         │
       │      │   │   │              │
       │      ▼   ▼   ▼              │
       │  ┌──────────────────────┐   │
       │  │   Virtual Users (N)  │   │
       │  │   (workload/)        │   │
       │  │                      │   │
       │  │  Each VU goroutine:  │   │
       │  │  ┌────────────────┐  │   │
       │  │  │ New Convo?     │  │   │
       │  │  │   ↓            │  │   │
       │  │  │ Send Human Msg │  │   │
       │  │  │   ↓            │  │   │
  │  │  │ Read History   │──┼───┼──── Record(sample) ──→ Collector
  │  │  │   ↓            │  │   │     (E2E latency, bytes, ──→ CSV
  │  │  │ Write Asst Msg │  │   │      errors)              ──→ Dashboard
       │  │  │   ↓            │  │   │
       │  │  │ Update Convo   │  │   │
       │  │  │   ↓            │  │   │
       │  │  │ Think Time     │  │   │
       │  │  │   ↓ (loop)     │  │   │
       │  │  └────────────────┘  │   │
       │  └──────────┬───────────┘   │
       │             │               │
       ▼             ▼               │
┌─────────────────────────────┐      │
│      MongoDB Driver         │      │
│   (connection pool)         │      │
│                             │      │
│  Pool Monitor    ───────────┼──────→ pool stats for dashboard
│  Compressors: zstd/snappy   │      │
│  Write Concern / Read Pref  │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│       MongoDB Atlas         │
│   (or local mongod)        │
│                             │
│  messages collection        │
│  conversations collection   │
│                             │
│  Indexes:                   │
│  • {conversation_id, time}  │
│  • {create_time: -1}        │
│  • {user_id, created_at}    │
└─────────────────────────────┘
```

### How It Works: End-to-End Flow

#### 1. Startup & Configuration

```
./bin/mongodb-ai-bench -config configs/quick-test.yaml
```

The binary prints the ASCII art banner, then loads the YAML config file. The config loader also reads a `.env` file from the working directory (if present) and overrides `mongodb.uri` with the `MONGODB_URI` environment variable. This keeps credentials out of config files and version control. The config is validated for required fields, sane defaults, and phase definitions.

#### 2. Orchestrator Initialization

The orchestrator is the top-level coordinator. It creates four subsystems:

| Component | File | Purpose |
|-----------|------|---------|
| **Pool Manager** | `internal/pool/manager.go` | Manages the MongoDB client with connection pooling and pool event tracking (connections created/checked out/closed). Compressors (zstd, snappy, zlib) are enabled for wire protocol efficiency. Optionally runs `collStats` to track collection-level metrics. |
| **Metrics Collector** | `internal/metrics/collector.go` | Maintains per-operation HDR histograms (write_human_message, write_assistant_message, read_conversation_history, etc.) for E2E latency percentiles. Three histogram tiers: full-run (overall percentiles), sliding window (per-interval throughput), and hourly buckets (rotated every hour for the hourly breakdown report). Tracks total ops, errors, bytes via atomic counters. Keeps a ring buffer of the 10 most recent error messages. |
| **Time Series Writer** | `internal/metrics/timeseries.go` | Background goroutine that snapshots all metrics every `csv_interval` (default 1s) into a timestamped CSV file under `results/`. Each row captures: timestamp, phase, operation, P50/P95/P99/P99.9/max latency, throughput, error count, bytes written. |
| **Collection Stats Writer** | `internal/metrics/collstats.go` | Background goroutine (when `collection_stats_enabled: true`) that runs `collStats` every `collection_stats_interval` and writes document count, storage size, index size, and avg doc size to a separate CSV. Latest snapshot is included in the final report. |
| **Dashboard** | `internal/runner/orchestrator.go` | Background goroutine that prints a formatted stats table to the terminal every `stats_interval` (default 5s). Shows phase name, active VUs, total ops/writes/reads/errors, connection pool state, per-operation latency table with ops/sec, and recent errors if any. |

After construction, the orchestrator:
1. **Pings MongoDB** — fails fast if the cluster is unreachable
2. **Creates indexes** — compound index on `{conversation_id, create_time}` for the primary read pattern, plus secondary indexes; conversation indexes only if `track_conversations: true`
3. **Starts the CSV writer and dashboard** (both run until shutdown)
4. **Hands off to the Progressive Runner**

#### 3. Progressive Overload Phases

The progressive runner executes phases sequentially. Each phase has a **duration**, a **target virtual user count**, and a **ramp strategy**:

```yaml
phases:
  - name: "warmup"
    duration: "30s"
    target_virtual_users: 10
    ramp: "linear"

  - name: "load"
    duration: "2m"
    target_virtual_users: 50
    ramp: "linear"

  - name: "peak"
    duration: "2m"
    target_virtual_users: 100
    ramp: "linear"
```

**Ramp strategies:**

- **`linear`** — Interpolates from current VU count to target over ~20 equal steps. If warmup ends with 10 VUs and the load phase targets 50, it ramps 10 → 12 → 14 → ... → 50 over the phase duration. After reaching target, holds steady until the phase timer expires.
- **`step`** — 10 equal jumps (e.g., 10 → 14 → 18 → ... → 50), with equal wait between each step.
- **`none`** — Immediately jumps to the target VU count.

**VU count carries across phases.** The load phase starts from wherever warmup ended. This produces smooth, organic growth rather than artificial drops between phases.

#### 4. Virtual User Manager

The VU Manager tracks all active virtual user goroutines. When scaling up, it:
1. Creates a cancellable context for each new VU (child of the phase context)
2. Seeds a deterministic PRNG per VU for reproducible workloads
3. Constructs a `MessageGenerator` with the workload config (response size distribution, conversation continuation probability, model names)
4. Launches the VU as a goroutine

When scaling down, it cancels VU contexts (arbitrary selection). On shutdown, it cancels all VUs and waits for all goroutines to drain.

#### 5. Virtual User Conversation Lifecycle

Each virtual user is an independent goroutine that simulates a real AI chatbot user. It runs an infinite loop:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Virtual User Loop                           │
│                                                                 │
│  ┌─── Check: should I stop? (phase cancelled / Ctrl-C) ──────┐ │
│  │    YES → exit goroutine                                    │ │
│  │    NO  → continue                                          │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                      │
│                          ▼                                      │
│  ┌─── Need new conversation? ─────────────────────────────────┐ │
│  │    First run, or 30% chance of starting fresh              │ │
│  │    (continue_conversation_pct controls this)               │ │
│  │                                                            │ │
│  │    YES + track_conversations:                              │ │
│  │      → InsertOne to conversations collection               │ │
│  │    YES + messages-only:                                    │ │
│  │      → Generate in-memory state (no DB call)               │ │
│  │    NO → reuse existing conversation_id                     │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                      │
│                          ▼                                      │
│  ┌─── Send Human Message ─────────────────────────────────────┐ │
│  │    InsertOne → messages collection                         │ │
│  │    Content from prompt corpus (100+ templates)             │ │
│  │    Records: E2E latency, doc size, success/error           │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                      │
│                          ▼                                      │
│  ┌─── Read Conversation History ──────────────────────────────┐ │
│  │    Find → messages collection (sorted by create_time)      │ │
│  │    Returns 2, 10, 50+ docs depending on convo length       │ │
│  │    Uses compound index {conversation_id, create_time}      │ │
│  │    Records: E2E latency, total bytes read, success/error   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                      │
│                          ▼                                      │
│  ┌─── Write Assistant Response ───────────────────────────────┐ │
│  │    InsertOne → messages collection                         │ │
│  │    Size varies by distribution:                            │ │
│  │      short (30%):     ~200 bytes                           │ │
│  │      medium (40%):    ~2-5 KB                              │ │
│  │      long (20%):      ~10-30 KB                            │ │
│  │      very_long (10%): ~50-100 KB (with web_search_results) │ │
│  │    Records: E2E latency, doc size, success/error           │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                      │
│                          ▼                                      │
│  ┌─── Update Conversation Metadata (if tracking) ────────────┐ │
│  │    UpdateOne → conversations collection                    │ │
│  │    $set last_message_at, $inc message_count by 2           │ │
│  │    Records: E2E latency, success/error                     │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                      │
│                          ▼                                      │
│  ┌─── Simulate Think Time ────────────────────────────────────┐ │
│  │    Random 50-250ms delay (LLM processing + user reading)   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                      │
│                          └─── loop back to top ──────────────── │
└─────────────────────────────────────────────────────────────────┘
```

**Key design details:**
- Each MongoDB operation gets its own **30-second timeout**, independent of the phase deadline. This prevents CSOT (Client-Side Operation Timeout) errors at phase boundaries where the remaining phase time might be less than the network round-trip time.
- On error: the VU logs the error, records it in the metrics collector, resets conversation state, backs off 500ms, then retries with a fresh conversation.
- 70% of the time (configurable), the VU continues the same conversation. This produces a geometric distribution of conversation lengths with a median of ~6 messages, matching real-world chatbot usage patterns.

#### 6. Metrics Pipeline

Every MongoDB operation records a `Sample` into the metrics collector:

```
MongoDB op completes
    │
    ▼
Record(Sample{
    Operation:         "write_human_message",
    E2ELatency:        time.Since(start),    ← wall-clock round-trip
    Success:           err == nil,
    Error:             err.Error(),           ← empty string on success
    DocumentSizeBytes: len(bsonDoc),
})
    │
    ├──→ HDR Histogram (full run)        ← percentiles: P50/P95/P99/P99.9/Max
    ├──→ HDR Histogram (sliding window)  ← reset each snapshot for throughput calc
    ├──→ HDR Histogram (hourly bucket)   ← rotated every hour for hourly breakdown
    ├──→ Atomic counters                 ← total ops, errors, bytes
    └──→ Error ring buffer (last 10)     ← shown on dashboard + final report
```

**Three consumers run in background goroutines:**

| Consumer | Interval | Output |
|----------|----------|--------|
| CSV Writer | `csv_interval` (1s) | `results/timeseries_<timestamp>.csv` — one row per operation per tick |
| Dashboard | `stats_interval` (5s) | Terminal table with latency, throughput, pool stats, recent errors |
| Collection Stats | `collection_stats_interval` (30-60s) | `results/collstats_<timestamp>.csv` — document count, storage/index size per collection (optional) |

The CSV Writer and Dashboard both call `AllSnapshots()` which reads percentiles from the full-run histograms and resets the sliding window histogram for the next interval's throughput calculation. The Collection Stats Writer runs independently, polling `collStats` against MongoDB.

#### 7. Connection Pool Management

The pool manager configures the MongoDB Go driver with:

- **Connection pool**: `max_pool_size` (ceiling), `min_pool_size` (warm connections), `max_conn_idle_time` (recycle stale connections)
- **Pool monitor**: Tracks connections created, checked out, checked in, and closed via atomic counters — surfaced in the dashboard
- **Wire compression**: zstd, snappy, zlib negotiated automatically
- **Write concern and read preference**: Configurable per run (e.g., `w:1` for speed, `w:majority` for durability testing; `secondaryPreferred` for read scaling)
- **Collection stats**: Optionally runs `collStats` at a configurable interval to track document count, storage size, index size, and average document size

#### 8. Graceful Shutdown

Two shutdown paths converge on the same cleanup:

```
Normal completion:                    Ctrl-C / SIGTERM:
  All phases finish                     Signal handler fires
       │                                     │
       ▼                                     ▼
  StopAll VUs                          cancel(root context)
       │                                     │
       ▼                                     ▼
  Deferred cleanup:                    ProgressiveRunner sees ctx.Done()
  1. tsWriter.Stop()                         │
     → flush final CSV snapshot              ▼
  2. collStatsWriter.Stop()            StopAll VUs (cancel + wait)
     → flush final collection stats          │
  3. dashboard.Stop()                        ▼
     → close loop, wait for exit       Same deferred cleanup path
  4. GenerateReport()
     → markdown with tables
  5. poolMgr.Close()
     → disconnect client
```

VUs with in-flight operations finish their current MongoDB call (up to 30s per-operation timeout) before exiting. No operations are interrupted mid-flight.

#### 9. Output Artifacts

After a run completes, you get:

```
results/
├── timeseries_20260327_180530.csv     ← per-second metrics for every operation
├── collstats_20260327_180530.csv      ← periodic collection stats (if enabled)
└── report_20260327_180530.md          ← summary with latency tables, hourly breakdown, collection stats, error log, config
```

The timeseries CSV is designed for import into plotting tools. The `analysis/plot.py` script generates throughput and latency graphs from it. The collstats CSV tracks how storage, document count, and index size grow over the course of the benchmark.

**The Markdown report includes:**
- Summary table (total ops, throughput, error rate, data written)
- Latency by operation (P50/P95/P99/P99.9/Max)
- Hourly breakdown per operation (each hour gets its own HDR histogram — not cumulative — so you can see exactly when latency shifted, correlating with cluster scale-ups, working set growth, or compaction events)
- Connection pool final state
- Collection stats (if enabled)
- Recent errors
- Full configuration used

---

### Actual MongoDB Operations

The benchmark simulates realistic AI chatbot conversations using virtual users, each running an independent conversation lifecycle. Here are the exact MongoDB operations each virtual user executes per turn:

#### 1. Create Conversation (insert, `conversations` collection)

Only when starting a new conversation. Skipped when `track_conversations: false`.

```javascript
db.conversations.insertOne({
  _id: "a1b2c3d4-...",
  user_id: "f7e8d9c0-...",
  model: "mongo-v1",
  created_at: ISODate("2026-03-27T..."),
  last_message_at: ISODate("2026-03-27T..."),
  message_count: 0
})
```

#### 2. Send Human Message (insert, `messages` collection)

```javascript
db.messages.insertOne({
  _id: "b2c3d4e5-...",
  conversation_id: "a1b2c3d4-...",
  user_id: "f7e8d9c0-...",
  message: "How do I implement connection pooling in PostgreSQL?",
  sender: "human",
  create_time: ISODate("2026-03-27T..."),
  parent_response_id: "prev-msg-uuid-...",    // null on first message
  metadata: {
    requestModelDetails: { modelId: "mongo-v1" }
  },
  model: "mongo-v1",
  tool_responses: []
})
```

#### 3. Read Conversation History (find, `messages` collection)

Fetches messages for the current conversation, sorted by time, capped at `max_history_messages` (default 500). This is the query that LLM backends run to build context for the next response. Uses the compound index `{ conversation_id: 1, create_time: 1 }` — a covered query + sort.

```javascript
db.messages.find(
  { conversation_id: "a1b2c3d4-..." }
).sort({ create_time: 1 }).limit(500)
```

As conversations grow longer, this query returns more documents (2, 10, 50+ messages), so read latency naturally increases over time — exactly what happens in production. The limit bounds memory usage per query; set `max_history_messages` higher in the config if you need to benchmark unbounded reads.

#### 4. Write Assistant Response (insert, `messages` collection)

Document size varies: 200 bytes for short replies, up to 100KB+ for responses with `web_search_results`. The size distribution is configurable.

```javascript
db.messages.insertOne({
  _id: "c3d4e5f6-...",
  conversation_id: "a1b2c3d4-...",
  user_id: "f7e8d9c0-...",
  message: "Here's a detailed explanation of connection pooling...",
  sender: "assistant",
  create_time: ISODate("2026-03-27T..."),
  parent_response_id: "b2c3d4e5-...",
  metadata: {
    deepsearchPreset: "",
    ui_layout: { reasoningUiLayout: "FUNCTION_CALL", willThinkLong: false, effort: "LOW" },
    llm_info: { modelHash: "Pb4zFT4brLm..." },
    request_metadata: { model: "mongo-v1", mode: "auto", effort: "low" },
    request_trace_id: "2226c72b4f12e9cd..."
  },
  model: "mongo-v1",
  tool_responses: [],
  web_search_results: [                        // present on ~10% of responses
    { url: "https://...", title: "...", preview: "..." },
    ...
  ]
})
```

#### 5. Update Conversation Metadata (update, `conversations` collection)

Atomic update by `_id` (the fastest possible write path). Skipped when `track_conversations: false`.

```javascript
db.conversations.updateOne(
  { _id: "a1b2c3d4-..." },
  {
    $set: { last_message_at: ISODate("2026-03-27T...") },
    $inc: { message_count: 2 }
  }
)
```

#### 6. Continue or Start New Conversation

70% of the time (configurable via `continue_conversation_pct`), the virtual user loops back to step 2 with the same `conversation_id`. Otherwise, it starts a fresh conversation from step 1. This produces a geometric conversation length distribution with a median of ~6 messages.

This produces a natural **4:1 write-to-read ratio** with organic data growth (or **2:1** with `track_conversations: false`).

### Schema Design

We use **one document per message** across two collections. This was a deliberate choice over the alternative (embedding all messages in a single conversation document).

#### Why One Document Per Message

The embedded-document approach (all messages inside one conversation document) fails for write-heavy AI chatbot workloads:

- **16MB BSON limit** — a single LLM response with web search results can be 100KB+. A 50-message conversation with web results blows past the document size limit.
- **Write amplification** — every `$push` to an embedded array rewrites the entire document. At 100K writes/sec this is catastrophic for throughput.
- **Document-level contention** — concurrent writes to the same conversation (e.g. streaming chunks, parallel tool calls) serialize against each other.

The one-document-per-message approach gives us:

- **Append-only inserts** — no document rewrites, no contention, no size limit concerns.
- **Linear scaling** — write throughput scales with data volume, not conversation length.
- **Shardable** — hash `conversation_id` to distribute writes evenly while keeping conversation reads on a single shard.
- **Independently indexable** — query by time range, sender, model, or any field without scanning nested arrays.

#### Collections

**`conversations`** — lightweight metadata (~500 bytes per document)

```json
{
  "_id": "uuid",
  "user_id": "uuid",
  "model": "mongo-v1",
  "created_at": "ISODate",
  "last_message_at": "ISODate",
  "message_count": 12
}
```

**`messages`** — individual messages (200 bytes to 100KB+ with web search results)

```json
{
  "_id": "uuid",
  "conversation_id": "uuid",
  "user_id": "uuid",
  "message": "string",
  "sender": "human | assistant",
  "create_time": "ISODate",
  "parent_response_id": "uuid | null",
  "metadata": {
    "ui_layout": { "reasoningUiLayout": "FUNCTION_CALL", "willThinkLong": false, "effort": "LOW" },
    "llm_info": { "modelHash": "..." },
    "request_metadata": { "model": "mongo-v1", "mode": "auto", "effort": "low" },
    "request_trace_id": "hex"
  },
  "model": "string",
  "tool_responses": [],
  "web_search_results": []
}
```

#### Indexes

| Collection | Index | Purpose |
|---|---|---|
| `messages` | `{ conversation_id: 1, create_time: 1 }` | Primary read pattern: fetch a conversation's messages in order. Compound index covers the query + sort. |
| `messages` | `{ create_time: -1 }` | Time-range analytics, potential TTL expiration. |
| `conversations` | `{ user_id: 1, created_at: -1 }` | List a user's recent conversations. |

#### Shard Key

For sharded clusters: **hashed `conversation_id`** on the `messages` collection. This distributes writes evenly across shards while ensuring all messages for a single conversation live on the same shard (targeted reads, no scatter-gather).

#### Messages-Only vs Full Model

The `conversations` collection is **optional**. You control this with a single config flag:

```yaml
workload:
  track_conversations: true   # true = both collections, false = messages only
```

| Mode | `track_conversations` | Collections Used | Writes per Turn | Read:Write Ratio |
|---|---|---|---|---|
| **Full model** | `true` (default) | `messages` + `conversations` | 4 (human msg + assistant msg + create convo + update metadata) | ~4:1 |
| **Messages only** | `false` | `messages` only | 2 (human msg + assistant msg) | ~2:1 |

**When to use messages-only mode:**
- You want to isolate and benchmark pure message insert + read performance
- You don't care about the "list conversations" query pattern
- You want to cut write overhead in half and see how MongoDB handles the raw message throughput

**When to use the full model:**
- You want a realistic production simulation (every chatbot has a conversation list sidebar)
- You want to benchmark the mix of inserts (`messages`) and updates-by-id (`conversations`)
- You want to prove MongoDB handles both append-heavy and update-heavy patterns simultaneously

When `track_conversations: false`, the benchmark:
- Skips the `conversations` collection entirely (no inserts, no updates, no indexes created)
- Still groups messages by `conversation_id` for realistic read queries
- Still respects `continue_conversation_pct` for conversation length distribution

#### Customizing the Schema

- **Document structure**: `internal/workload/conversation.go` — `messageToDoc()` builds the BSON document for each message, `CreateConversation()` builds conversation docs. Modify these functions to add/remove fields.
- **Index definitions**: `internal/pool/manager.go` — `EnsureIndexes()` creates all indexes at startup. Add new `mongo.IndexModel` entries here.
- **Message content & metadata**: `internal/workload/message_gen.go` — `GenerateHumanMessage()` and `GenerateAssistantMessage()` control the metadata shape, model names, and field distributions.
- **Response sizes**: `configs/*.yaml` — `workload.response_size_distribution` controls the percentage of short/medium/long/very-long responses.
- **Conversation behavior**: `configs/*.yaml` — `workload.continue_conversation_pct` (default 70) controls how often a virtual user continues an existing conversation vs starting a new one. Lower values = more short conversations. Higher values = longer conversations with deeper read queries.

### Progressive Overload

Configurable phases ramp virtual users from 0 to 25K+, producing up to 100K writes/sec:

| Phase | Duration | Virtual Users | Writes/sec | Reads/sec |
|-------|----------|--------------|------------|-----------|
| Warmup | 5m | 100 | ~400 | ~100 |
| Low | 30m | 500 | ~2K | ~500 |
| Medium | 2h | 2,500 | ~10K | ~2.5K |
| High | 6h | 10,000 | ~40K | ~10K |
| Peak | 24h | 25,000 | ~100K | ~25K |
| Sustained | 7d | 25,000 | ~100K | ~25K |

### How 100K Writes/sec Is Generated

The benchmark generates load the same way a real production chatbot does: **thousands of concurrent user sessions**, each performing sequential operations. There is no batching, bulk writes, or pipelining -- every operation is a single `InsertOne`, `Find`, or `UpdateOne`, exactly like a real chatbot backend.

#### Per-VU Throughput Model

Each virtual user runs operations **sequentially** (you can't write the assistant response before reading conversation history). Throughput per VU is bounded by network round-trip time:

```
One Turn = 4-5 sequential DB operations + think time

  CreateConversation   →  InsertOne      (~RTT)    ← only 30% of turns
  SendHumanMessage     →  InsertOne      (~RTT)
  ReadConvoHistory     →  Find + cursor  (~RTT)
  WriteAssistantResp   →  InsertOne      (~RTT)
  UpdateConvoMetadata  →  UpdateOne      (~RTT)    ← if tracking conversations
  ThinkTime            →  50-250ms random delay

  Total time per turn ≈ (4-5 × RTT) + ~150ms think time
```

| Deployment | RTT per op | Time per turn | Writes/sec/VU | VUs for 100K writes/sec |
|------------|-----------|---------------|---------------|------------------------|
| **EC2 → Atlas (same region)** | ~2ms | ~160ms | ~21 | ~4,800 |
| **EC2 → Atlas (VPC peered)** | ~0.5ms | ~153ms | ~22 | ~4,500 |
| **Local laptop → Atlas** | ~80ms | ~550ms | ~6 | ~16,700 |

The full-run config targets **25,000 VUs**. From EC2 in the same region, that's 25K × ~4 writes/sec/VU ≈ 100K writes/sec with headroom.

#### Why 25,000 Virtual Users Don't Throttle Each Other

Each virtual user is a **Go goroutine**, not an OS thread. This distinction is critical:

| | OS Thread | Go Goroutine |
|--|-----------|-------------|
| Stack size | ~1-8 MB | ~4 KB (grows as needed) |
| 25K of them | 25-200 GB RAM for stacks | **100 MB** |
| Context switch | Kernel syscall (~1-10µs) | Userspace (~100ns) |
| Scheduling | OS kernel | Go runtime M:N scheduler |

At any given instant, the 25,000 VUs break down like this:

```
25,000 Virtual Users
    │
    ├── ~400 doing CPU work (message gen, BSON encode)    ← active, needs CPU
    ├── ~500 in network I/O (MongoDB round-trip)          ← parked, zero CPU
    ├── ~200 waiting for a pool connection                ← parked, zero CPU
    └── ~23,900 in think time (sleeping 50-250ms)         ← parked, zero CPU
```

**~96% of goroutines are parked at any instant.** They consume zero CPU and zero network. The Go scheduler doesn't touch them until their I/O completes or timer fires.

#### The Connection Pool Is The Deliberate Bottleneck

You do NOT want 25,000 simultaneous TCP connections to MongoDB. The connection pool is the rate limiter:

```
25,000 VUs ──→ connection pool (max_pool_size: 500) ──→ 500 TCP connections ──→ MongoDB
                     │
                     │  VU calls InsertOne()
                     │    ├── connection available → checkout, ~2ms RTT, checkin
                     │    └── pool full → goroutine parks (zero CPU) until a conn frees up
                     │
                     │  Throughput = pool_size × (1000ms / avg_RTT)
                     │            = 500 × 500 ops/sec
                     │            = 250,000 ops/sec theoretical max
```

With 500 connections and 2ms RTT, the pool can sustain ~250K ops/sec. We need ~125K total (100K writes + 25K reads), so 500 connections leaves headroom.

The dashboard shows pool health in real time:

```
Pool: total=47 checked_out=12 available=35    ← healthy: plenty of headroom
Pool: total=500 checked_out=500 available=0   ← saturated: VUs are queuing
```

When the pool is saturated, VUs queue up waiting for connections. You'll see E2E latency climbing even though the server isn't under proportionally more load — the extra time is pool wait. This is a signal to increase `max_pool_size` or distribute VUs across more EC2 instances.

#### EC2 Instance Sizing

The benchmark is **I/O-bound**, not CPU or memory bound:

| Resource | Demand at 25K VUs | c5.4xlarge (16 vCPU, 32GB, 10Gbps) |
|----------|-------------------|-------------------------------------|
| **CPU** | ~400 active goroutines, BSON encode/decode | ~5% utilization |
| **Memory** | 25K stacks (100MB) + buffers + message gen | ~2 GB total |
| **Network** | ~125K ops/sec × ~2KB avg = ~250 MB/sec | 10 Gbps = 1.25 GB/sec |
| **Connections** | 500 TCP connections to Atlas | Trivial (ulimit default 65K) |

A single **c5.4xlarge** ($0.68/hr) handles 25K VUs comfortably. Memory beyond 32GB provides no benefit -- the bottleneck is network I/O, not RAM. A larger instance type only helps if you need more network bandwidth (c5.9xlarge gives 12 Gbps) or want to run 50K+ VUs on a single machine.

**Recommended instance types by scale:**

| Target | Instance | vCPU | RAM | Network | VUs | Cost/hr |
|--------|----------|------|-----|---------|-----|---------|
| Quick test | c5.xlarge | 4 | 8 GB | Up to 10 Gbps | 5,000 | $0.17 |
| Full run | c5.4xlarge | 16 | 32 GB | 10 Gbps | 25,000 | $0.68 |
| Extreme | c5.9xlarge | 36 | 72 GB | 12 Gbps | 50,000 | $1.53 |
| Distributed | 4× c5.4xlarge | 64 | 128 GB | 40 Gbps | 100,000 | $2.72 |

Choose **c5** (compute-optimized) over **r5** (memory-optimized) -- extra RAM doesn't help. Choose over **m5** (general purpose) because the slightly higher clock speed on c5 gives better per-core BSON throughput.

#### When to Distribute Across Multiple EC2s

| Trigger | Symptom | Solution |
|---------|---------|----------|
| Network bandwidth saturation | >1 GB/sec sustained, increasing retransmits | Add more EC2 instances |
| Atlas connection limit hit | Driver errors about max connections | Split VUs across clients (each gets its own pool) |
| Simulating multi-region clients | N/A -- architectural requirement | Deploy in multiple AWS regions |

The `deploy/terraform` setup supports multi-instance deployment. Each instance runs its own copy of the benchmark with a subset of VUs. CSV results can be merged for analysis.

### Latency Measurement

Every MongoDB operation is measured with **E2E (end-to-end) latency** — a wall-clock `time.Since(start)` wrapped around the entire driver call:

```
┌─────────────────── E2E Latency ───────────────────┐
│                                                     │
│  1. Wait for connection from pool                   │
│  2. BSON serialize                                  │
│  3. TCP send ──────────────→ MongoDB                │
│  4. MongoDB processes the command                   │
│  5. TCP receive ←─────────── MongoDB                │
│  6. BSON deserialize                                │
│                                                     │
└─────────────────────────────────────────────────────┘
```

This captures the full client experience: pool checkout wait, serialization, network round-trip, server processing, and deserialization. Recorded per operation into **HDR histograms** (P50, P95, P99, P99.9, Max) without storing individual samples.

**Why not separate server-side latency?** The Go driver's command monitor reports wire-level duration (send + server + receive), but that's not truly "server-side" — it includes network time. Isolating actual server processing time requires `db.setProfilingLevel(2)` or Atlas Performance Advisor. Since the command monitor duration is neither pure server time nor meaningfully different from E2E in same-region deployments, we measure E2E only to avoid misleading labels.

### Metrics

- Per-operation E2E latency: P50, P95, P99, P99.9, max (HDR histograms)
- Hourly breakdown per operation with independent histograms (not cumulative) for correlating with cluster events
- Throughput, error rates, connection pool utilization
- Collection stats: document count, storage size, index size, avg doc size (optional, via `collStats`)
- Real-time terminal dashboard with recent errors
- CSV time-series export for post-run analysis
- Separate collection stats CSV when enabled

## Configuration Reference

See `configs/` for example configurations (`quick-test.yaml`, `default.yaml`, `full-run.yaml`).

### `mongodb` — Database Connection

| Setting | Default | Description |
|---------|---------|-------------|
| `uri` | `mongodb://localhost:27017` | Connection string. **Overridden by `MONGODB_URI`** from `.env` file or environment variable — keeps credentials out of config files and version control. |
| `database` | *(required)* | Database name for `messages` and `conversations` collections. |
| `write_concern` | `"1"` | Write acknowledgement level. `"1"` = primary only (fastest). `"2"` = acknowledged by 2 members. `"majority"` = majority of voting members (most durable, highest latency). |
| `read_preference` | `primaryPreferred` | Where reads are routed. `primary` = primary only. `primaryPreferred` = primary, fallback to secondary. `secondaryPreferred` = secondaries, fallback to primary (spreads read load). `secondary` = secondaries only. `nearest` = lowest latency member. |
| `max_pool_size` | `100` | Maximum concurrent TCP connections the driver maintains. This is the ceiling — if all are checked out, new operations wait for one to free up. 500 recommended for 25K VUs. |
| `min_pool_size` | `10` | Driver pre-warms and maintains at least this many connections even when idle. Avoids cold-start latency spikes. |
| `max_conn_idle_time` | `30s` | Connections idle longer than this are closed. Prevents holding stale connections. |
| `connect_timeout` | `10s` | Maximum wait for a new TCP connection to establish. |
| `socket_timeout` | `30s` | Maximum wait for any single operation to complete on the wire. Operations exceeding this are killed client-side. |

### `workload` — Traffic Shape

| Setting | Default | Description |
|---------|---------|-------------|
| `track_conversations` | `true` | Enables the full chatbot flow: create conversation → insert messages → update metadata. When `false`, fires standalone message inserts only (no `conversations` collection). |
| `continue_conversation_pct` | `70` | Percentage of turns where a VU continues its existing conversation. The remaining percentage starts a new conversation. Controls conversation depth vs breadth. At 70%, median conversation length is ~6 messages. |
| `web_search_pct` | `10` | Percentage of very-long assistant responses that include simulated web search result documents, making those documents larger and more complex. |
| `max_history_messages` | `500` | Maximum number of messages returned when reading conversation history. Bounds memory usage per read query. Set higher for stress-testing very long conversations. |
| `models` | `["mongo-v1"]` | List of model name strings randomly assigned to conversations. Metadata only — doesn't change behavior, but mimics real apps tracking which AI model served each response. |
| `response_size_distribution` | see below | Controls the size mix of generated assistant response documents. Must sum to 100. |
| `  short_pct` | `30` | Percentage of responses that are ~100-500 bytes. |
| `  medium_pct` | `40` | Percentage of responses that are ~500 bytes - 3 KB. |
| `  long_pct` | `20` | Percentage of responses that are ~3-10 KB. |
| `  very_long_pct` | `10` | Percentage of responses that are ~10-100 KB (long reasoning traces, code blocks, web search results). |

### `phases` — Progressive Overload Stages

Phases run **sequentially**. VU count carries across phases (no reset between them).

| Setting | Description |
|---------|-------------|
| `name` | Label shown in dashboard, CSV, and report. |
| `duration` | How long the phase lasts (e.g. `30s`, `2m`, `6h`, `7d`). |
| `target_virtual_users` | Number of concurrent goroutines running the workload loop by end of phase. Each VU simulates one user: create conversation → send message → read history → get response → update metadata → think → repeat. |
| `ramp` | How VUs scale during the phase. `linear` = gradual ramp from current count to target over ~20 steps. `step` = 10 equal jumps. `none` = jump immediately to target. |

### `metrics` — Output & Monitoring

| Setting | Default | Description |
|---------|---------|-------------|
| `output_dir` | `results` | Directory for CSV files and the final Markdown report. Created automatically if it doesn't exist. |
| `csv_interval` | `1s` | How often a row is written to `timeseries_*.csv` per operation type. At `1s` with 5 operation types, that's 5 rows/sec. This is the high-resolution data for plotting. |
| `stats_interval` | `10s` | How often the terminal dashboard table refreshes. Lower = more frequent but noisier. |
| `dashboard_enabled` | `true` | Whether to print the live stats table to the terminal. Set `false` for headless/scripted runs. |
| `collection_stats_enabled` | `false` | Whether to periodically run MongoDB's `collStats` command on the `messages` and `conversations` collections during the benchmark. Captures document count, storage size, index size, and average document size. |
| `collection_stats_interval` | `60s` | How often to poll `collStats`. Only applies when `collection_stats_enabled: true`. This is a lightweight metadata command, but no need to run it every second. |

## AWS Deployment

See `deploy/` for Terraform configs and deployment scripts.

```bash
cd deploy/terraform
terraform init

# Required variables: allowed_ssh_cidr, results_bucket, key_name, bench_binary_s3, bench_config_s3
terraform apply \
  -var="allowed_ssh_cidr=203.0.113.0/24" \
  -var="results_bucket=my-bench-results" \
  -var="key_name=my-keypair" \
  -var="bench_binary_s3=s3://my-bucket/mongodb-ai-bench-linux-amd64" \
  -var="bench_config_s3=s3://my-bucket/full-run.yaml"

# Deploy and run across EC2 instances
cd ../scripts
./run-distributed.sh <key-file> <ip1> <ip2> ...
```

### Terraform Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `allowed_ssh_cidr` | Yes | — | CIDR block allowed to SSH into instances (e.g. `203.0.113.50/32` for a single IP). |
| `results_bucket` | Yes | — | S3 bucket name for storing benchmark results. IAM policy is scoped to this bucket only. |
| `key_name` | Yes | — | EC2 SSH key pair name. |
| `bench_binary_s3` | Yes | — | S3 URI of the pre-built benchmark binary. |
| `bench_config_s3` | Yes | — | S3 URI of the benchmark config YAML. |
| `aws_region` | No | `us-east-1` | AWS region for benchmark clients. |
| `instance_type` | No | `c6i.4xlarge` | EC2 instance type. |
| `client_count` | No | `4` | Number of EC2 benchmark client instances. |
| `assign_public_ip` | No | `true` | Assign public IPs to instances. Set `false` if using VPN/SSM. |

## Security

The following security measures are built into the project:

### Credentials & Secrets

- **MongoDB URI**: loaded from the `MONGODB_URI` environment variable or a local `.env` file, never from YAML configs. The `.env` file is in `.gitignore`.
- **`.env` whitelist**: the `.env` loader only sets `MONGODB_URI`. Other keys are silently ignored to prevent overriding security-sensitive variables like `PATH`.
- **Error sanitization**: MongoDB connection URIs are stripped from error messages before they reach metrics, dashboards, or reports.
- **URI masking**: the orchestrator logs the connection URI with credentials removed.

### Network & TLS

- **TLS 1.2 minimum**: remote `mongodb://` connections enforce TLS 1.2+. `mongodb+srv://` connections (Atlas) use the driver's native TLS handling to preserve URI-derived settings (custom CAs, client certs). Localhost connections are unaffected.
- **SSH restricted by CIDR**: Terraform requires an explicit `allowed_ssh_cidr` variable — no default, no `0.0.0.0/0`.
- **SSH host key verification**: deploy scripts use `StrictHostKeyChecking=accept-new` (trust on first connect, reject on change) instead of disabling verification.

### Infrastructure (Terraform)

- **Scoped IAM policy**: S3 access is limited to the specific `results_bucket` — no wildcard resource grants.
- **Encrypted EBS volumes**: root block devices use `encrypted = true`.
- **Public IP opt-in**: `assign_public_ip` defaults to `true` for backward compatibility but can be set to `false` for private-subnet deployments with VPN/SSM.

### Application

- **Config path validation**: only `.yaml`/`.yml` files are accepted as config paths.
- **Output directory validation**: `metrics.output_dir` must be a relative path to prevent writing to arbitrary system directories.
- **Report file permissions**: benchmark reports are written with `0600` (owner-only read/write).
- **Bounded reads**: conversation history queries are capped at `max_history_messages` (default 500) to prevent unbounded memory growth.
- **Input validation**: percentage fields (`continue_conversation_pct`, `web_search_pct`) are validated to 0-100. Duration parse errors are checked explicitly.
- **Dashboard race condition**: the dashboard's phase field is protected by `sync.RWMutex` for safe concurrent access.

## Project Structure

```
cmd/bench/           - Entry point and startup banner
internal/config/     - YAML configuration + .env loading
internal/pool/       - MongoDB connection pool, pool monitoring, collection stats
internal/workload/   - Virtual user lifecycle, conversation runner, message generation
internal/metrics/    - HDR histograms, CSV time-series, collection stats CSV, recent error tracking
internal/runner/     - Orchestrator, progressive phases, dashboard
internal/data/       - Prompt & response corpus
deploy/              - Terraform + deployment scripts
configs/             - Benchmark configurations (quick-test, default, full-run)
analysis/            - Post-run analysis and plotting scripts
```
