# 📈 MongoDB Atlas Query Shape Analyzer

A  performance diagnostic tool for MongoDB Atlas clusters (8.0+). Analyzes query shapes, identifies performance issues, and generates comprehensive reports.

## 🛠 Features

### Core Analysis
- **Query Shape Insights**: Fetches and analyzes all query shapes from Atlas Query Insights API (v2)
- **Slow Query Logs**: Independently fetches slow query logs from all replica set nodes
- **Multi-Node Analysis**: Automatically detects and analyzes all nodes in a replica set
- **System Query Filtering**: Filters out internal MongoDB system queries for clean reports
- **Full Query Shape Display**: Shows complete query shapes with proper JSON formatting

### Performance Metrics
- **Latency Percentiles**: P50, P90, P99 execution times for each query shape
- **Efficiency Scoring**: 0-100 score based on scan efficiency
- **Scan Ratio**: Documents examined vs. documents returned
- **Keys Ratio**: Index keys examined vs. documents returned
- **Keys Examined**: Number of index keys scanned (0 = no index used)

### Issue Detection
- � **No Index (Collection Scan)**: Queries with keysExamined=0 (CRITICAL)
- �🔥 **High Frequency Queries**: Queries exceeding 10,000 executions
- 📊 **Inefficient Scans**: Queries with scan ratio > 10x (CRITICAL if > 100x)
- 💡 **Low Efficiency**: Queries scoring below 50/100 (CRITICAL if < 20)
- ⚡ **Inconsistent Performance**: High P99/P50 variance (> 5x)
- 🐢 **Slow Queries**: P99 latency > 100ms (CRITICAL if > 500ms)
- ✨ **Covered Queries**: Queries answered entirely from index (positive highlight)

### CI/CD Integration
- **Exit Codes**: Returns exit code 1 if critical issues found, 0 otherwise
- **Critical Issues**: No index, very slow (>500ms), very inefficient (>100x), very low efficiency (<20)

### Reports & Exports
- **Console Output**: Real-time terminal dashboard with color-coded metrics
- **Markdown Report**: `mongo_performance_report.md` for documentation/review
- **CSV Export**: `mongo_performance_report.csv` for spreadsheet analysis

---

## 🚀 Quick Start

### Prerequisites
1. Python 3.8+
2. MongoDB Atlas M10+ cluster (Query Insights requires dedicated clusters)
3. Atlas API keys with Project Read Only access or higher

### Installation

```bash
pip install requests
```

### Configuration

Edit the script and set your credentials:

```python
PUBLIC_KEY = "your_public_key"
PRIVATE_KEY = "your_private_key"
PROJECT_ID = "your_project_id"
CLUSTER_NAME = "your_cluster_name"
```

> **Production Tip**: Use environment variables instead of hardcoding credentials.

### Run

```bash
python atlasMongoDiagnostic.py
```

---

## 📊 Understanding the Output

### Executive Summary
```
📋 EXECUTIVE SUMMARY

   Query Shapes Analyzed: 81
   Slow Query Logs Found: 45
   Total Executions: 2,200,656
   Collections Analyzed: 29
   Slow Queries (P99 > 100ms): 0
   Inefficient Queries (scan ratio > 10x): 14
   High Frequency Queries (> 10,000 execs): 18
   Critical Issues: 19
   ✨ Covered Queries (index-only): 1
```

### Query Detail Output
```
Command: find
Query Shape Hash: ABC123...
Query Shape:
   {
      "filter": {"status": "?string"},
      "sort": {"createdAt": 1}
   }
Executions: 10,000
P99: 25.00ms | P90: 15.00ms | P50: 5.00ms
Docs Examined: 1000 | Docs Returned: 100 | Keys Examined: 500
Efficiency Score: 90/100 | Scan Ratio: 10.0x | Keys Ratio: 5.0x
```

### How to Check if an Index is Used

| Metric | Value | Meaning |
|--------|-------|---------|
| Keys Examined | > 0 | ✅ Index IS being used |
| Keys Examined | = 0 | ❌ NO index (collection scan) |
| Keys Ratio | Low (1-2x) | Good index coverage |
| Keys Ratio | High (10x+) | Index exists but inefficient |
| Scan Ratio | = 1x | Perfect efficiency |
| Scan Ratio | > 10x | Needs optimization |

### Performance Risk Levels

| Risk Level | P99 Latency | Likely Root Cause |
|:-----------|:------------|:------------------|
| **Low** | 1-10ms | Optimized, in-memory queries |
| **Medium** | 10-100ms | Normal disk I/O |
| **High** | 100-500ms | Missing or inefficient indexes |
| **Critical** | 500ms+ | Collection scans or large unindexed sorts |

---

## 🔧 Configuration Options

### Performance Thresholds

Customize these values in the script:

```python
SLOW_QUERY_THRESHOLD_MS = 100      # P99 threshold for slow query alerts
HIGH_EXEC_COUNT_THRESHOLD = 10000  # Execution count threshold
INEFFICIENT_RATIO_THRESHOLD = 10   # docsExamined/docsReturned threshold
P99_P50_VARIANCE_THRESHOLD = 5     # P99/P50 ratio for inconsistent perf
```

### Latency Buckets

Queries are categorized into these buckets for the breakdown table:

| Bucket | Latency Range |
|--------|---------------|
| 1-10ms | Excellent |
| 10-30ms | Good |
| 30-100ms | Acceptable |
| 100-300ms | Needs attention |
| 300-500ms | Poor |
| 500ms-1s | Critical |
| 1-10s | Severe |
| >10s | Emergency |

---

## 🔒 Security Best Practices

### Use Environment Variables (Recommended)

```python
import os

PUBLIC_KEY = os.environ.get('ATLAS_PUBLIC_KEY')
PRIVATE_KEY = os.environ.get('ATLAS_PRIVATE_KEY')
PROJECT_ID = os.environ.get('ATLAS_PROJECT_ID')
CLUSTER_NAME = os.environ.get('ATLAS_CLUSTER_NAME')
```

```bash
export ATLAS_PUBLIC_KEY="your_public_key"
export ATLAS_PRIVATE_KEY="your_private_key"
export ATLAS_PROJECT_ID="your_project_id"
export ATLAS_CLUSTER_NAME="your_cluster_name"
python queryshapes.py
```

### API Access
- Create a dedicated API key with **Project Read Only** role
- Whitelist only required IP addresses
- Rotate keys periodically

### Data Safety
- This script is **read-only** - it will not modify your database
- No data is sent to external services

---

## � CI/CD Integration

The script returns exit codes for CI/CD pipeline integration:

| Exit Code | Meaning |
|-----------|---------|
| 0 | No critical issues found |
| 1 | Critical issues found (requires attention) |
| 2 | Script error (API failure, etc.) |

### GitHub Actions Example

```yaml
- name: Run MongoDB Performance Check
  run: python atlasMongoDiagnostic.py
  env:
    ATLAS_PUBLIC_KEY: ${{ secrets.ATLAS_PUBLIC_KEY }}
    ATLAS_PRIVATE_KEY: ${{ secrets.ATLAS_PRIVATE_KEY }}
    ATLAS_PROJECT_ID: ${{ secrets.ATLAS_PROJECT_ID }}
    ATLAS_CLUSTER_NAME: ${{ secrets.ATLAS_CLUSTER_NAME }}

- name: Upload Performance Report
  if: always()
  uses: actions/upload-artifact@v3
  with:
    name: mongo-performance-report
    path: |
      mongo_performance_report.md
      mongo_performance_report.csv
```

### Critical Issues That Trigger Exit Code 1

- 🔴 **No Index**: Collection scan detected (keysExamined=0)
- 🐢 **Very Slow**: P99 latency > 500ms
- 📊 **Very Inefficient**: Scan ratio > 100x
- 💡 **Very Low Efficiency**: Score < 20/100

---

## �🗓️ Automation

### Linux/macOS (cron)

Run every Monday at 9:00 AM:

```bash
crontab -e
```

```bash
0 9 * * 1 cd /path/to/script && /usr/bin/python3 queryshapes.py >> /var/log/atlas_perf.log 2>&1
```

### Windows (Task Scheduler)

1. Open **Task Scheduler** → **Create Basic Task**
2. **Trigger**: Weekly, Monday
3. **Action**: Start a Program
4. **Program**: `python`
5. **Arguments**: `C:\path\to\queryshapes.py`

---

## 📧 Email Reports (Optional)

Add this to the end of the script:

```python
import smtplib
from email.message import EmailMessage

def send_email_report(filename):
    msg = EmailMessage()
    msg['Subject'] = f"MongoDB Performance Report: {CLUSTER_NAME}"
    msg['From'] = "devops@yourcompany.com"
    msg['To'] = "team@yourcompany.com"

    with open(filename, 'r') as f:
        msg.set_content(f.read())

    with smtplib.SMTP('smtp.yourserver.com', 587) as s:
        s.starttls()
        s.login("user", "password")
        s.send_message(msg)

# Call after report generation
send_email_report('mongo_performance_report.md')
```

---

## 📝 Output Files

| File | Description |
|------|-------------|
| `mongo_performance_report.md` | Full Markdown report with all metrics |
| `mongo_performance_report.csv` | CSV export for spreadsheet analysis |

---

## 🔍 Troubleshooting

### 500 Internal Server Error
This is a **temporary server-side issue** from MongoDB Atlas, not a problem with your script or credentials.

**What happens:**
```
✗ Query Shapes API failed: 500 Server Error: Internal Server Error
```

**The script will automatically:**
1. Retry up to **10 times** with random jitter (3-5 seconds between attempts)
2. Continue to fetch Slow Query Logs independently (not affected by Query Shapes failure)
3. Generate a report with whatever data is available

**Note:** The script fetches **both** Query Shape Insights and Slow Query Logs independently. If one fails, the other will still be collected.

**If it persists:**
- ⏳ **Wait a few minutes and retry** - this is usually a transient Atlas issue
- 🔧 Check [Atlas Status Page](https://status.cloud.mongodb.com/) for ongoing incidents
- 🔄 Verify your cluster isn't undergoing maintenance or scaling operations
- 📊 Check Atlas UI for any alerts on your cluster

**Manual retry:**
```bash
python atlasMongoDiagnostic.py
```

### 404 Errors
- Ensure your cluster is M10+ (Query Insights not available on shared clusters)
- Verify PROJECT_ID and CLUSTER_NAME are correct

### Empty Results
- Wait for queries to run on your cluster (Query Insights needs data)
- Check if all queries are system queries (filtered by default)

### Authentication Errors
- Verify API keys are correct
- Ensure IP is whitelisted in Atlas
- Check API key has appropriate permissions

---

## 📚 API Reference

This tool uses the following MongoDB Atlas APIs:

- **Query Shape Insights API** (v2): `/api/atlas/v2/groups/{groupId}/clusters/{clusterName}/queryShapeInsights/summaries`
- **Slow Query Logs API** (v1.0): `/api/atlas/v1.0/groups/{groupId}/processes/{processId}/performanceAdvisor/slowQueryLogs`
- **Processes API** (v1.0): `/api/atlas/v1.0/groups/{groupId}/processes`
- **Performance Advisor API** (v1.0): `/api/atlas/v1.0/groups/{groupId}/processes/{processId}/performanceAdvisor/suggestedIndexes`
- **Clusters API** (v2): `/api/atlas/v2/groups/{groupId}/clusters/{clusterName}`

---

## 📄 License

MIT License - Free for commercial and personal use.