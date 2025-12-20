import requests
from requests.auth import HTTPDigestAuth
from collections import defaultdict
import csv
import sys

# --- CONFIGURATION ---
PUBLIC_KEY = "your_public_key"
PRIVATE_KEY = "your_private_key"

PROJECT_ID = "your_project_id"
CLUSTER_NAME = "your_cluster_name"

# Process ID format: "hostname:port" - Get this from Atlas UI or API
# Example: "cluster0-shard-00-00.xxxxx.mongodb.net:27017"
PROCESS_ID = ""  # Leave empty to auto-detect or fill in use one of the cluster nodes

PUBLIC_KEY="yxvorpoz"
PRIVATE_KEY="ce051203-2584-4870-be7d-d495dd7ddc23"

PROJECT_ID = "654dc3bcd782d2777b255b9f"
CLUSTER_NAME = "Cluster0"
# Leave empty to auto-detect, or set manually: "hostname:port"
# To find it manually: Atlas UI -> Cluster -> Metrics -> any node hostname:port

# Correct API base URLs - Query Insights uses v2 API without cluster name in path
BASE_URL_V2 = f"https://cloud.mongodb.com/api/atlas/v2/groups/{PROJECT_ID}"
BASE_URL_V1 = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{PROJECT_ID}"
BUCKETS = ["1-10", "<30", "<100", "<300", "<500", "<1000", "<10000", ">10000"]

# Performance thresholds for alerts
SLOW_QUERY_THRESHOLD_MS = 100  # P99 threshold for slow query alerts
HIGH_EXEC_COUNT_THRESHOLD = 10000  # Execution count threshold
INEFFICIENT_RATIO_THRESHOLD = 10  # docsExamined/docsReturned ratio threshold
P99_P50_VARIANCE_THRESHOLD = 5  # P99/P50 ratio for inconsistent performance

def get_bucket(ms):
    if ms <= 10: return "1-10"
    if ms <= 30: return "<30"
    if ms <= 100: return "<100"
    if ms <= 300: return "<300"
    if ms <= 500: return "<500"
    if ms <= 1000: return "<1000"
    if ms <= 10000: return "<10000"
    return ">10000"

def calculate_efficiency_score(stats):
    """Calculate efficiency metrics for a query shape"""
    docs_examined = stats.get('avg_docs_examined', 0)
    docs_returned = stats.get('avg_docs_returned', 0)
    keys_examined = stats.get('avg_keys_examined', 0)

    # Index efficiency: lower is better (ideally close to 1)
    if docs_returned > 0:
        scan_ratio = docs_examined / docs_returned
    else:
        scan_ratio = docs_examined if docs_examined > 0 else 0

    # Key efficiency
    if docs_returned > 0:
        key_ratio = keys_examined / docs_returned
    else:
        key_ratio = keys_examined if keys_examined > 0 else 0

    # Overall efficiency score (0-100, higher is better)
    if docs_examined == 0 and docs_returned == 0:
        efficiency_score = 100  # No data movement
    elif scan_ratio <= 1:
        efficiency_score = 100  # Perfect efficiency
    elif scan_ratio <= 10:
        efficiency_score = 90 - (scan_ratio - 1) * 5
    elif scan_ratio <= 100:
        efficiency_score = 50 - (scan_ratio - 10) * 0.5
    else:
        efficiency_score = max(0, 10 - (scan_ratio - 100) * 0.1)

    return {
        'scan_ratio': scan_ratio,
        'key_ratio': key_ratio,
        'efficiency_score': efficiency_score
    }

def calculate_resource_impact(stats):
    """Calculate total resource impact of a query shape"""
    exec_count = stats.get('total_executions', 0)
    avg_time = stats.get('avg_p99', 0)
    bytes_read = stats.get('avg_bytes_read', 0)

    # Total time spent (ms)
    total_time_ms = exec_count * avg_time

    # Total bytes read
    total_bytes = exec_count * bytes_read

    return {
        'total_time_ms': total_time_ms,
        'total_bytes': total_bytes,
        'impact_score': total_time_ms * (1 + bytes_read / 1000000)  # Weight by data read
    }

def detect_performance_issues(stats):
    """Detect various performance issues in a query shape
    
    Returns:
        dict with keys:
        - issues: list of warning/problem strings
        - highlights: list of positive highlights (like covered queries)
        - has_critical: True if any critical issues found
    """
    issues = []
    highlights = []
    has_critical = False

    p99 = stats.get('avg_p99', 0)
    p50 = stats.get('avg_p50', 0)
    exec_count = stats.get('total_executions', 0)
    docs_examined = stats.get('avg_docs_examined', 0)
    keys_examined = stats.get('avg_keys_examined', 0)

    efficiency = calculate_efficiency_score(stats)

    # Covered query detection (positive highlight)
    if docs_examined == 0 and keys_examined > 0:
        highlights.append("✨ COVERED QUERY: Answered entirely from index")

    # No index used - collection scan
    if keys_examined == 0 and docs_examined > 0:
        issues.append("🔴 NO INDEX: Collection scan (keysExamined=0)")
        has_critical = True

    # Slow query (critical if very slow)
    if p99 > SLOW_QUERY_THRESHOLD_MS:
        issues.append(f"⚠️ SLOW: P99 {p99:.1f}ms exceeds {SLOW_QUERY_THRESHOLD_MS}ms threshold")
        if p99 > 500:  # Very slow
            has_critical = True

    # High execution count
    if exec_count > HIGH_EXEC_COUNT_THRESHOLD:
        issues.append(f"🔥 HIGH FREQUENCY: {exec_count:,} executions")

    # Inefficient scan (critical if very inefficient)
    if efficiency['scan_ratio'] > INEFFICIENT_RATIO_THRESHOLD:
        issues.append(f"📊 INEFFICIENT SCAN: Examining {efficiency['scan_ratio']:.1f}x more docs than returned")
        if efficiency['scan_ratio'] > 100:  # Very inefficient
            has_critical = True

    # Inconsistent performance (high variance)
    if p50 > 0 and p99 / p50 > P99_P50_VARIANCE_THRESHOLD:
        issues.append(f"⚡ INCONSISTENT: P99/P50 ratio is {p99/p50:.1f}x (high variance)")

    # Low efficiency score (critical if very low)
    if efficiency['efficiency_score'] < 50:
        issues.append(f"💡 LOW EFFICIENCY: Score {efficiency['efficiency_score']:.0f}/100")
        if efficiency['efficiency_score'] < 20:
            has_critical = True

    return {'issues': issues, 'highlights': highlights, 'has_critical': has_critical}

def fetch_data(endpoint, params=None, use_v2=False):
    """Fetch data from MongoDB Atlas API

    Args:
        endpoint: API endpoint path
        params: Query parameters
        use_v2: Use v2 API (for Query Insights), otherwise use v1.0
    """
    base_url = BASE_URL_V2 if use_v2 else BASE_URL_V1
    url = f"{base_url}/{endpoint}"

    headers = {
        "Accept": "application/vnd.atlas.2025-03-12+json" if use_v2 else "application/json",
        "Content-Type": "application/json"
    }

    response = requests.get(url, auth=HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY), params=params, headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_full_query_shape(query_shape_hash):
    """Fetch the full query shape using the queryShapeHash"""
    try:
        endpoint = f"clusters/{CLUSTER_NAME}/queryShapeInsights/{query_shape_hash}"
        data = fetch_data(endpoint, use_v2=True)
        # The full query shape might be in different fields
        import json

        # Try different possible fields
        shape = data.get('shape', data.get('queryShape', data.get('query', '')))

        if shape:
            if isinstance(shape, dict):
                return json.dumps(shape, indent=2)
            return str(shape)
        return ''
    except Exception as e:
        # If we can't fetch it, return empty string (silently fail)
        return ''

def get_cluster_processes():
    """Get all process IDs for the cluster"""
    try:
        # Get cluster details to find processes
        print("   Fetching all processes from Atlas API...")
        processes_data = fetch_data(f"processes", use_v2=True)
        all_processes = processes_data.get('results', [])

        # Filter processes that match the cluster name
        cluster_processes = [
            p['id'] for p in all_processes
            if CLUSTER_NAME.lower() in p.get('userAlias', '').lower() or
               CLUSTER_NAME.lower() in p.get('hostname', '').lower()
        ]

        if cluster_processes:
            print(f"   Found {len(cluster_processes)} process(es) for {CLUSTER_NAME}:")
            for proc in cluster_processes:
                print(f"      • {proc}")

        return cluster_processes
    except Exception as e:
        print(f"⚠️  Could not auto-detect processes: {e}")
        return []

def check_cluster_info():
    """Get basic cluster information for debugging"""
    try:
        print("\n🔍 Checking cluster configuration...")

        # Get cluster details
        cluster_data = fetch_data(f"clusters/{CLUSTER_NAME}", use_v2=False)
        print(f"   Cluster Tier: {cluster_data.get('providerSettings', {}).get('instanceSizeName', 'Unknown')}")
        print(f"   MongoDB Version: {cluster_data.get('mongoDBVersion', 'Unknown')}")
        print(f"   Cluster Type: {cluster_data.get('clusterType', 'Unknown')}")

        return cluster_data
    except Exception as e:
        print(f"   ⚠️  Could not fetch cluster info: {e}")
        return None

def run_master_diagnostic():
    print(f"🚀 Analyzing {CLUSTER_NAME}...")

    try:
        # Get cluster info first
        cluster_info = check_cluster_info()

        # Auto-detect all process IDs if not provided
        process_ids = []
        if not PROCESS_ID:
            print("\n📡 Auto-detecting cluster processes...")
            processes = get_cluster_processes()
            if not processes:
                print("❌ Error: Could not find processes. Please set PROCESS_ID manually.")
                print("   Find it in Atlas UI -> Cluster -> Metrics -> any node hostname:port")
                return
            process_ids = processes  # Use ALL processes
            print(f"✓ Will analyze all {len(process_ids)} processes")
        else:
            process_ids = [PROCESS_ID]
            print(f"✓ Using configured process: {PROCESS_ID}")

        # 1. Fetch Telemetry & Advisor Data
        # Try Query Insights / Slow Query Logs (may not be available on all clusters)
        shapes = []
        slow_queries = []

        print(f"\n📊 Fetching query performance data...")

        # Step 1: Try Query Shape Insights Summaries (most comprehensive)
        try:
            print(f"   Step 1: Fetching query shape insights summaries...")
            summaries_data = fetch_data(f"clusters/{CLUSTER_NAME}/queryShapeInsights/summaries", params={'nSummaries': 500}, use_v2=True)
            summaries_list = summaries_data.get('summaries', [])
            print(f"✓ Found {len(summaries_list)} query shape summaries")

            # Extract data from summaries
            if summaries_list:
                for summary in summaries_list:
                    shapes.append({
                        'namespace': summary.get('namespace', 'unknown'),
                        'command': summary.get('command', 'unknown'),
                        'queryShape': summary.get('queryShape', ''),
                        'queryShapeHash': summary.get('queryShapeHash', ''),
                        'p99ExecutionTimeMicros': summary.get('p99ExecMicros', 0),
                        'p90ExecutionTimeMicros': summary.get('p90ExecMicros', 0),
                        'p50ExecutionTimeMicros': summary.get('p50ExecMicros', 0),
                        'avgWorkingMillis': summary.get('avgWorkingMillis', 0),
                        'execCount': summary.get('execCount', 0),
                        'docsExamined': summary.get('docsExamined', 0),
                        'docsReturned': summary.get('docsReturned', 0),
                        'keysExamined': summary.get('keysExamined', 0),
                        'docsExaminedRatio': summary.get('docsExaminedRatio', 0),
                        'keysExaminedRatio': summary.get('keysExaminedRatio', 0),
                        'bytesRead': summary.get('bytesRead', 0),
                        'systemQuery': summary.get('systemQuery', False),
                    })

                print(f"✓ Successfully extracted {len(shapes)} query shapes from summaries")

        except Exception as e:
            print(f"   ✗ Query Shapes API failed: {str(e)[:80]}")
            print(f"   Falling back to Slow Query Logs from all processes...")

            # Fallback to Slow Query Logs from all processes
            for i, pid in enumerate(process_ids, 1):
                try:
                    print(f"      [{i}/{len(process_ids)}] Fetching slow queries from {pid[:40]}...")
                    slow_query_data = fetch_data(f"processes/{pid}/performanceAdvisor/slowQueryLogs", params={}, use_v2=False)
                    slow_queries = slow_query_data.get('slowQueries', [])
                    print(f"         Found {len(slow_queries)} slow queries")

                    for sq in slow_queries:
                        shapes.append({
                            'namespace': sq.get('namespace', 'unknown'),
                            'command': 'unknown',
                            'queryShape': '',
                            'p99ExecutionTimeMicros': sq.get('millis', 0) * 1000
                        })
                except Exception as e2:
                    print(f"         ✗ Failed: {str(e2)[:60]}")

            if shapes:
                print(f"✓ Total slow queries collected: {len(shapes)}")

        if not shapes:
            print(f"⚠️  No query performance data available")
            print(f"   Note: Query data requires recent query activity on the cluster")

        # Performance Advisor - collect from all processes
        print(f"\n💡 Fetching performance advisor suggestions from all processes...")
        suggestions = []

        for i, pid in enumerate(process_ids, 1):
            try:
                print(f"   [{i}/{len(process_ids)}] Fetching suggestions from {pid[:40]}...")
                advisor_data = fetch_data(f"processes/{pid}/performanceAdvisor/suggestedIndexes", use_v2=False)
                process_suggestions = advisor_data.get('suggestedIndexes', [])
                suggestions.extend(process_suggestions)
                print(f"      Found {len(process_suggestions)} suggestions")
            except Exception as e:
                print(f"      ✗ Failed: {str(e)[:60]}")

        print(f"✓ Total index suggestions collected: {len(suggestions)}")
        
        # Group shapes by namespace, command, and query shape
        grouped_shapes = defaultdict(list)
        by_namespace = defaultdict(list)

        for s in shapes:
            ns = s.get('namespace', 'unknown')
            cmd = s.get('command', 'unknown')
            qs_key = s.get('queryShape', '')[:100]  # Truncate for grouping key only
            key = (ns, cmd, qs_key)
            grouped_shapes[key].append(s)

        # Calculate statistics for each group
        group_stats = {}
        for key, shape_list in grouped_shapes.items():
            ns, cmd, qs_key = key
            # Get the FULL query shape from the first item in the group
            qs_full = shape_list[0].get('queryShape', '') if shape_list else ''
            p99_values = [s.get('p99ExecutionTimeMicros', 0) / 1000 for s in shape_list]
            p90_values = [s.get('p90ExecutionTimeMicros', 0) / 1000 for s in shape_list]
            p50_values = [s.get('p50ExecutionTimeMicros', 0) / 1000 for s in shape_list]
            exec_counts = [s.get('execCount', 0) for s in shape_list]
            docs_examined = [s.get('docsExamined', 0) for s in shape_list]
            docs_returned = [s.get('docsReturned', 0) for s in shape_list]
            keys_examined = [s.get('keysExamined', 0) for s in shape_list]
            bytes_read = [s.get('bytesRead', 0) for s in shape_list]

            # Get queryShapeHash (use the first one from the group, or collect all if multiple)
            query_shape_hashes = [s.get('queryShapeHash', 'N/A') for s in shape_list]
            # If all hashes are the same, use one; otherwise, use the first or join them
            query_shape_hash = query_shape_hashes[0] if query_shape_hashes else 'N/A'

            # Check if this is a system query (use first item's value)
            is_system_query = shape_list[0].get('systemQuery', False) if shape_list else False

            group_stats[key] = {
                'namespace': ns,
                'command': cmd,
                'queryShape': qs_full,  # Use the full query shape, not the truncated key
                'queryShapeHash': query_shape_hash,
                'systemQuery': is_system_query,
                'count': len(shape_list),
                'avg_p99': sum(p99_values) / len(p99_values) if p99_values else 0,
                'avg_p90': sum(p90_values) / len(p90_values) if p90_values else 0,
                'avg_p50': sum(p50_values) / len(p50_values) if p50_values else 0,
                'total_executions': sum(exec_counts),
                'avg_docs_examined': sum(docs_examined) / len(docs_examined) if docs_examined else 0,
                'avg_docs_returned': sum(docs_returned) / len(docs_returned) if docs_returned else 0,
                'avg_keys_examined': sum(keys_examined) / len(keys_examined) if keys_examined else 0,
                'avg_bytes_read': sum(bytes_read) / len(bytes_read) if bytes_read else 0,
            }

            # Add efficiency metrics
            efficiency = calculate_efficiency_score(group_stats[key])
            group_stats[key].update(efficiency)

            # Add resource impact
            impact = calculate_resource_impact(group_stats[key])
            group_stats[key].update(impact)

            # Detect issues and highlights
            issue_result = detect_performance_issues(group_stats[key])
            group_stats[key]['issues'] = issue_result['issues']
            group_stats[key]['highlights'] = issue_result['highlights']
            group_stats[key]['has_critical'] = issue_result['has_critical']

        # Group by namespace for display
        for key, stats in group_stats.items():
            by_namespace[stats['namespace']].append(stats)

        # Add index suggestions
        for advice in suggestions:
            ns = advice.get('namespace')
            for key in group_stats:
                if group_stats[key]['namespace'] == ns:
                    if 'fixes' not in group_stats[key]:
                        group_stats[key]['fixes'] = []
                    group_stats[key]['fixes'].append(advice)

        # 2. PRINT ANALYSIS TO TERMINAL
        print("\n" + "="*80)
        print(f"📊 MONGODB PERFORMANCE ANALYSIS: {CLUSTER_NAME}")
        print("="*80 + "\n")

        if group_stats:
            # ===== EXECUTIVE SUMMARY =====
            print("📋 EXECUTIVE SUMMARY\n")

            # Calculate overall metrics
            total_queries = len(group_stats)
            total_executions = sum(s['total_executions'] for s in group_stats.values())
            total_collections = len(set(s['namespace'] for s in group_stats.values()))

            # Count issues
            slow_queries = sum(1 for s in group_stats.values() if s['avg_p99'] > SLOW_QUERY_THRESHOLD_MS)
            inefficient_queries = sum(1 for s in group_stats.values() if s['scan_ratio'] > INEFFICIENT_RATIO_THRESHOLD)
            high_freq_queries = sum(1 for s in group_stats.values() if s['total_executions'] > HIGH_EXEC_COUNT_THRESHOLD)
            critical_issues = sum(1 for s in group_stats.values() if s.get('has_critical', False))
            covered_queries = sum(1 for s in group_stats.values() if s.get('highlights'))

            print(f"   Total Query Shapes: {total_queries}")
            print(f"   Total Executions: {total_executions:,}")
            print(f"   Collections Analyzed: {total_collections}")
            print(f"   Slow Queries (P99 > {SLOW_QUERY_THRESHOLD_MS}ms): {slow_queries}")
            print(f"   Inefficient Queries (scan ratio > {INEFFICIENT_RATIO_THRESHOLD}x): {inefficient_queries}")
            print(f"   High Frequency Queries (> {HIGH_EXEC_COUNT_THRESHOLD:,} execs): {high_freq_queries}")
            print(f"   Critical Issues: {critical_issues}")
            if covered_queries > 0:
                print(f"   ✨ Covered Queries (index-only): {covered_queries}")

            # Filter out system queries for all top 10 lists
            non_system_queries = [s for s in group_stats.values() if not s.get('systemQuery', False)]

            # Top 10 slowest queries
            print("\n🐌 TOP 10 SLOWEST QUERIES (by P99 latency)\n")
            slowest = sorted(non_system_queries, key=lambda x: x['avg_p99'], reverse=True)[:10]
            for i, s in enumerate(slowest, 1):
                qs_preview = s['queryShape'][:60] if s['queryShape'] else 'N/A'
                print(f"   {i:2}. {s['namespace']:<40} P99: {s['avg_p99']:>8.1f}ms  [{s['command']}]")
                print(f"       Hash: {s['queryShapeHash']}")
                if s['issues']:
                    for issue in s['issues'][:2]:  # Show first 2 issues
                        print(f"       {issue}")

            # Top 10 most executed queries
            print("\n🔥 TOP 10 MOST EXECUTED QUERIES\n")
            most_executed = sorted(non_system_queries, key=lambda x: x['total_executions'], reverse=True)[:10]
            for i, s in enumerate(most_executed, 1):
                print(f"   {i:2}. {s['namespace']:<40} {s['total_executions']:>10,} execs  P99: {s['avg_p99']:>6.1f}ms")
                print(f"       Hash: {s['queryShapeHash']}")

            # Top 10 resource consumers
            print("\n💾 TOP 10 RESOURCE CONSUMERS (by total time)\n")
            resource_hogs = sorted(non_system_queries, key=lambda x: x['total_time_ms'], reverse=True)[:10]
            for i, s in enumerate(resource_hogs, 1):
                total_time_sec = s['total_time_ms'] / 1000
                print(f"   {i:2}. {s['namespace']:<40} {total_time_sec:>10.1f}s total  ({s['total_executions']:,} × {s['avg_p99']:.1f}ms)")
                print(f"       Hash: {s['queryShapeHash']}")

            # Top 10 inefficient queries
            print("\n📊 TOP 10 INEFFICIENT QUERIES (by scan ratio)\n")
            inefficient = sorted(non_system_queries, key=lambda x: x['scan_ratio'], reverse=True)[:10]
            for i, s in enumerate(inefficient, 1):
                if s['scan_ratio'] > 1:
                    print(f"   {i:2}. {s['namespace']:<40} Scan ratio: {s['scan_ratio']:>6.1f}x  Efficiency: {s['efficiency_score']:>3.0f}/100")
                    print(f"       Hash: {s['queryShapeHash']}")

            # Collections needing attention
            print("\n⚠️  COLLECTIONS NEEDING ATTENTION\n")
            coll_issues = defaultdict(list)
            for s in group_stats.values():
                if s['issues']:
                    coll_issues[s['namespace']].extend(s['issues'])

            coll_priority = sorted(coll_issues.items(), key=lambda x: len(x[1]), reverse=True)[:10]
            if coll_priority:
                for coll, _ in coll_priority:
                    print(f"   • {coll}")
            else:
                print("   ✅ No collections with performance issues detected!")

            print("\n" + "="*80 + "\n")

            # Continue with existing slow queries breakdown
            # First, show slow queries breakdown by collection
            print("🐌 SLOW QUERIES BREAKDOWN BY COLLECTION\n")

            # Calculate latency buckets per collection
            coll_buckets = defaultdict(lambda: {b: 0 for b in BUCKETS})
            coll_p99_list = defaultdict(list)

            for stats in group_stats.values():
                ns = stats['namespace']
                p99 = stats['avg_p99']
                bucket = get_bucket(p99)
                coll_buckets[ns][bucket] += 1
                coll_p99_list[ns].append(p99)

            # Print header
            print(f"{'Collection':<45} {'1-10ms':<8} {'10-30ms':<8} {'30-100ms':<9} {'100-300ms':<10} {'300-500ms':<10} {'500ms-1s':<10} {'1-10s':<8} {'>10s':<8} {'Avg P99':<10}")
            print("-" * 140)

            for ns in sorted(coll_buckets.keys()):
                counts = coll_buckets[ns]
                avg_p99 = sum(coll_p99_list[ns]) / len(coll_p99_list[ns])
                print(f"{ns:<45} {counts['1-10']:<8} {counts['<30']:<8} {counts['<100']:<9} {counts['<300']:<10} "
                      f"{counts['<500']:<10} {counts['<1000']:<10} {counts['<10000']:<8} {counts['>10000']:<8} {avg_p99:>8.1f}ms")

            print("\n" + "="*80 + "\n")

            # Then show detailed query shapes
            print("📈 QUERY SHAPES GROUPED BY COLLECTION, COMMAND & PATTERN\n")

            for ns in sorted(by_namespace.keys()):
                print(f"\n📁 {ns}")
                print("-" * 80)

                for stats in sorted(by_namespace[ns], key=lambda x: x['avg_p99'], reverse=True):
                    # Skip system queries
                    if stats.get('systemQuery', False):
                        continue

                    cmd = stats['command']
                    qs_raw = stats['queryShape'] if stats['queryShape'] else 'N/A'

                    # Try to parse and pretty-print the query shape
                    import json
                    qs = 'N/A'
                    if qs_raw and qs_raw != 'N/A':
                        # If it's a JSON string, parse and format it
                        if isinstance(qs_raw, str):
                            try:
                                qs_obj = json.loads(qs_raw)
                                qs = json.dumps(qs_obj, indent=3)
                            except (json.JSONDecodeError, ValueError) as e:
                                # If JSON parsing fails, use raw string
                                qs = qs_raw
                        else:
                            qs = str(qs_raw)

                    print(f"   Command: {cmd}")
                    print(f"   Query Shape Hash: {stats.get('queryShapeHash', 'N/A')}")
                    print(f"   Query Shape:")
                    # Indent each line of the query shape
                    for line in qs.split('\n'):
                        print(f"      {line}")
                    print(f"   Executions: {stats['total_executions']:,}")
                    print(f"   P99: {stats['avg_p99']:.2f}ms | P90: {stats['avg_p90']:.2f}ms | P50: {stats['avg_p50']:.2f}ms")
                    print(f"   Docs Examined: {stats['avg_docs_examined']:.0f} | Docs Returned: {stats['avg_docs_returned']:.0f} | Keys Examined: {stats['avg_keys_examined']:.0f}")
                    print(f"   Efficiency Score: {stats['efficiency_score']:.0f}/100 | Scan Ratio: {stats['scan_ratio']:.1f}x | Keys Ratio: {stats['key_ratio']:.1f}x")

                    # Show resource impact for high-impact queries
                    if stats['total_time_ms'] > 1000:  # More than 1 second total
                        print(f"   Total Time: {stats['total_time_ms']/1000:.1f}s | Bytes Read: {stats['avg_bytes_read']/1024:.1f}KB avg")

                    # Show highlights (positive indicators like covered queries)
                    if stats.get('highlights'):
                        for highlight in stats['highlights']:
                            print(f"   {highlight}")

                    # Show performance issues
                    if stats['issues']:
                        print(f"   ⚠️  Issues:")
                        for issue in stats['issues']:
                            print(f"      {issue}")

                    if 'fixes' in stats and stats['fixes']:
                        print(f"   💡 Index Suggestions:")
                        for fix in stats['fixes']:
                            print(f"      • Impact: {fix.get('impact', 'N/A')} - {fix.get('id', 'N/A')}")
                    print()
        else:
            print("⚠️  No query shape data available.")
            print("   Query Insights may not be enabled or no queries have been executed recently.\n")
            print("   Requirements:")
            print("   - MongoDB 7.0 or later")
            print("   - M10+ cluster tier")
            print("   - Recent query activity")

        print("\n" + "-"*80)
        print("💡 SUMMARY\n")

        if suggestions:
            print(f"   Total Index Suggestions: {len(suggestions)}")
            for suggestion in suggestions:
                ns = suggestion.get('namespace', 'unknown')
                print(f"   • {ns}: Impact {suggestion.get('impact', 'N/A')}")
        else:
            print("   No index suggestions from Performance Advisor.")

        print(f"   Total Query Shapes Analyzed: {len(shapes)}")
        print(f"   Total Collections: {len(by_namespace)}")

        print("\n" + "="*80 + "\n")

        # 3. EXPORT TO MARKDOWN FILE
        md_filename = "mongo_performance_report.md"
        with open(md_filename, "w") as f:
            f.write(f"# MongoDB Performance Report: {CLUSTER_NAME}\n\n")
            f.write(f"**Generated:** {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Executive Summary
            if group_stats:
                f.write("## 📋 Executive Summary\n\n")
                f.write(f"- **Total Query Shapes:** {total_queries}\n")
                f.write(f"- **Total Executions:** {total_executions:,}\n")
                f.write(f"- **Collections Analyzed:** {total_collections}\n")
                f.write(f"- **Slow Queries (P99 > {SLOW_QUERY_THRESHOLD_MS}ms):** {slow_queries}\n")
                f.write(f"- **Inefficient Queries (scan ratio > {INEFFICIENT_RATIO_THRESHOLD}x):** {inefficient_queries}\n")
                f.write(f"- **High Frequency Queries (> {HIGH_EXEC_COUNT_THRESHOLD:,} execs):** {high_freq_queries}\n\n")

                # Top 10 Slowest
                f.write("### 🐌 Top 10 Slowest Queries (by P99 latency)\n\n")
                f.write("| Rank | Collection | P99 (ms) | Command | Query Shape Hash | Issues |\n")
                f.write("| :---: | :--- | ---: | :--- | :--- | :--- |\n")
                for i, s in enumerate(slowest, 1):
                    issues_str = "; ".join(s['issues'][:2]) if s['issues'] else "None"
                    f.write(f"| {i} | {s['namespace']} | {s['avg_p99']:.1f} | {s['command']} | `{s['queryShapeHash']}` | {issues_str} |\n")
                f.write("\n")

                # Top 10 Most Executed
                f.write("### 🔥 Top 10 Most Executed Queries\n\n")
                f.write("| Rank | Collection | Executions | P99 (ms) | Command | Query Shape Hash |\n")
                f.write("| :---: | :--- | ---: | ---: | :--- | :--- |\n")
                for i, s in enumerate(most_executed, 1):
                    f.write(f"| {i} | {s['namespace']} | {s['total_executions']:,} | {s['avg_p99']:.1f} | {s['command']} | `{s['queryShapeHash']}` |\n")
                f.write("\n")

                # Top 10 Resource Consumers
                f.write("### 💾 Top 10 Resource Consumers (by total time)\n\n")
                f.write("| Rank | Collection | Total Time (s) | Executions | Avg P99 (ms) | Query Shape Hash |\n")
                f.write("| :---: | :--- | ---: | ---: | ---: | :--- |\n")
                for i, s in enumerate(resource_hogs, 1):
                    total_time_sec = s['total_time_ms'] / 1000
                    f.write(f"| {i} | {s['namespace']} | {total_time_sec:.1f} | {s['total_executions']:,} | {s['avg_p99']:.1f} | `{s['queryShapeHash']}` |\n")
                f.write("\n")

                # Top 10 Inefficient
                f.write("### 📊 Top 10 Inefficient Queries (by scan ratio)\n\n")
                f.write("| Rank | Collection | Scan Ratio | Efficiency Score | Command | Query Shape Hash |\n")
                f.write("| :---: | :--- | ---: | ---: | :--- | :--- |\n")
                for i, s in enumerate(inefficient, 1):
                    if s['scan_ratio'] > 1:
                        f.write(f"| {i} | {s['namespace']} | {s['scan_ratio']:.1f}x | {s['efficiency_score']:.0f}/100 | {s['command']} | `{s['queryShapeHash']}` |\n")
                f.write("\n")

            # Add slow queries breakdown table
            f.write("## 🐌 Slow Queries Breakdown by Collection\n\n")

            if group_stats:
                f.write("| Collection | 1-10ms | 10-30ms | 30-100ms | 100-300ms | 300-500ms | 500ms-1s | 1-10s | >10s | Avg P99 |\n")
                f.write("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |\n")

                for ns in sorted(coll_buckets.keys()):
                    counts = coll_buckets[ns]
                    avg_p99 = sum(coll_p99_list[ns]) / len(coll_p99_list[ns])
                    f.write(f"| {ns} | {counts['1-10']} | {counts['<30']} | {counts['<100']} | {counts['<300']} | "
                            f"{counts['<500']} | {counts['<1000']} | {counts['<10000']} | {counts['>10000']} | {avg_p99:.1f}ms |\n")

                f.write("\n")
            else:
                f.write("*No query shape data available.*\n\n")

            f.write("## 📊 Query Shapes Grouped by Collection, Command & Pattern\n\n")

            if group_stats:
                for ns in sorted(by_namespace.keys()):
                    f.write(f"\n### 📁 {ns}\n\n")

                    for stats in sorted(by_namespace[ns], key=lambda x: x['avg_p99'], reverse=True):
                        cmd = stats['command']
                        qs = stats['queryShape'][:200] if stats['queryShape'] else 'N/A'

                        f.write(f"**Command:** `{cmd}`\n\n")
                        f.write(f"**Query Shape:**\n```\n{qs}\n```\n\n")
                        f.write(f"- **Executions:** {stats['total_executions']:,}\n")
                        f.write(f"- **P99:** {stats['avg_p99']:.2f}ms | **P90:** {stats['avg_p90']:.2f}ms | **P50:** {stats['avg_p50']:.2f}ms\n")
                        f.write(f"- **Docs Examined:** {stats['avg_docs_examined']:.0f} | **Docs Returned:** {stats['avg_docs_returned']:.0f}\n")
                        f.write(f"- **Efficiency Score:** {stats['efficiency_score']:.0f}/100 | **Scan Ratio:** {stats['scan_ratio']:.1f}x\n")

                        if stats['total_time_ms'] > 1000:
                            f.write(f"- **Total Time:** {stats['total_time_ms']/1000:.1f}s | **Bytes Read:** {stats['avg_bytes_read']/1024:.1f}KB avg\n")

                        if stats['issues']:
                            f.write(f"\n**⚠️ Performance Issues:**\n")
                            for issue in stats['issues']:
                                f.write(f"- {issue}\n")

                        if 'fixes' in stats and stats['fixes']:
                            f.write(f"\n**💡 Index Suggestions:**\n")
                            for fix in stats['fixes']:
                                f.write(f"- Impact: {fix.get('impact', 'N/A')} - `{fix.get('id', 'N/A')}`\n")

                        f.write("\n---\n\n")
            else:
                f.write("*No query shape data available. Query Insights may not be enabled or no queries have been executed recently.*\n\n")

            f.write("\n## 💡 Summary\n\n")
            f.write(f"- **Total Query Shapes Analyzed:** {len(shapes)}\n")
            f.write(f"- **Total Collections:** {len(by_namespace) if group_stats else 0}\n")
            f.write(f"- **Total Index Suggestions:** {len(suggestions)}\n")

        print(f"✅ Report exported to {md_filename}")

        # 4. EXPORT TO CSV FILE
        csv_filename = "mongo_performance_report.csv"
        if group_stats:
            with open(csv_filename, "w", newline='') as csvfile:
                fieldnames = [
                    'namespace', 'command', 'query_shape_hash', 'query_shape_preview',
                    'executions', 'p99_ms', 'p90_ms', 'p50_ms',
                    'docs_examined', 'docs_returned', 'keys_examined',
                    'scan_ratio', 'efficiency_score',
                    'total_time_ms', 'bytes_read_avg',
                    'issues_count', 'issues_summary'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for stats in sorted(group_stats.values(), key=lambda x: x['avg_p99'], reverse=True):
                    # Skip system queries in CSV export
                    if stats.get('systemQuery', False):
                        continue
                    qs_preview = stats['queryShape'][:100] if stats['queryShape'] else 'N/A'
                    issues_summary = "; ".join(stats['issues']) if stats['issues'] else "None"

                    writer.writerow({
                        'namespace': stats['namespace'],
                        'command': stats['command'],
                        'query_shape_hash': stats['queryShapeHash'],
                        'query_shape_preview': qs_preview,
                        'executions': stats['total_executions'],
                        'p99_ms': f"{stats['avg_p99']:.2f}",
                        'p90_ms': f"{stats['avg_p90']:.2f}",
                        'p50_ms': f"{stats['avg_p50']:.2f}",
                        'docs_examined': f"{stats['avg_docs_examined']:.0f}",
                        'docs_returned': f"{stats['avg_docs_returned']:.0f}",
                        'keys_examined': f"{stats['avg_keys_examined']:.0f}",
                        'scan_ratio': f"{stats['scan_ratio']:.2f}",
                        'efficiency_score': f"{stats['efficiency_score']:.0f}",
                        'total_time_ms': f"{stats['total_time_ms']:.0f}",
                        'bytes_read_avg': f"{stats['avg_bytes_read']:.0f}",
                        'issues_count': len(stats['issues']),
                        'issues_summary': issues_summary
                    })

            print(f"✅ CSV data exported to {csv_filename}")

            # Return exit code based on critical issues
            if critical_issues > 0:
                print(f"\n🚨 EXIT CODE 1: {critical_issues} critical issue(s) found")
                return 1
            else:
                print(f"\n✅ EXIT CODE 0: No critical issues found")
                return 0
        else:
            print("⚠️  No query data to analyze")
            return 0

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 2  # Error exit code

if __name__ == "__main__":
    exit_code = run_master_diagnostic()
    sys.exit(exit_code if exit_code is not None else 0)



