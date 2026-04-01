#!/bin/bash
set -euo pipefail

# Collects benchmark results from all EC2 client instances.
#
# Two modes:
#   SSH:  ./collect-results.sh <key-file> <ip1> <ip2> ...
#   S3:   ./collect-results.sh --s3 <bucket> <client-count>
#
# S3 mode is preferred for long runs since results are synced automatically.

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOCAL_DIR="collected_results_${TIMESTAMP}"
BENCH_DIR="/opt/mongodb-bench"

collect_via_ssh() {
    local KEY_FILE="$1"
    shift
    local IPS=("$@")

    echo "==> Collecting results via SSH from ${#IPS[@]} clients..."

    for i in "${!IPS[@]}"; do
        IP="${IPS[$i]}"
        CLIENT_DIR="$LOCAL_DIR/client-$i"
        mkdir -p "$CLIENT_DIR"

        echo "  [client-$i] $IP - downloading results..."
        scp -i "$KEY_FILE" -o StrictHostKeyChecking=accept-new -r \
            "ec2-user@$IP:$BENCH_DIR/results/*" "$CLIENT_DIR/" 2>/dev/null || \
            echo "  [client-$i] Warning: no results found"

        scp -i "$KEY_FILE" -o StrictHostKeyChecking=accept-new \
            "ec2-user@$IP:$BENCH_DIR/bench.log" "$CLIENT_DIR/bench.log" 2>/dev/null || \
            echo "  [client-$i] Warning: no log found"
    done
}

collect_via_s3() {
    local BUCKET="$1"
    local CLIENT_COUNT="$2"

    echo "==> Collecting results via S3 from bucket '$BUCKET' ($CLIENT_COUNT clients)..."

    for i in $(seq 0 $((CLIENT_COUNT - 1))); do
        CLIENT_DIR="$LOCAL_DIR/client-$i"
        mkdir -p "$CLIENT_DIR"

        echo "  [client-$i] Downloading from s3://$BUCKET/client-$i/ ..."
        aws s3 sync "s3://$BUCKET/client-$i/results/" "$CLIENT_DIR/" --quiet 2>/dev/null || \
            echo "  [client-$i] Warning: no results found in S3"

        aws s3 cp "s3://$BUCKET/client-$i/bench.log" "$CLIENT_DIR/bench.log" --quiet 2>/dev/null || \
            echo "  [client-$i] Warning: no log found in S3"
    done
}

# ---------- Parse mode ----------

mkdir -p "$LOCAL_DIR"

if [ "${1:-}" = "--s3" ]; then
    BUCKET="${2:?Usage: collect-results.sh --s3 <bucket> <client-count>}"
    CLIENT_COUNT="${3:?Usage: collect-results.sh --s3 <bucket> <client-count>}"
    collect_via_s3 "$BUCKET" "$CLIENT_COUNT"
else
    KEY_FILE="${1:?Usage: collect-results.sh <key-file> <ip1> [ip2] ...  OR  collect-results.sh --s3 <bucket> <client-count>}"
    shift
    IPS=("$@")
    collect_via_ssh "$KEY_FILE" "${IPS[@]}"
fi

# ---------- Merge CSV files ----------

echo "==> Merging timeseries CSV files..."

HEADER_WRITTEN=false
MERGED_CSV="$LOCAL_DIR/merged_timeseries.csv"
for CSV in "$LOCAL_DIR"/client-*/timeseries_*.csv; do
    if [ ! -f "$CSV" ]; then continue; fi

    if [ "$HEADER_WRITTEN" = false ]; then
        head -1 "$CSV" > "$MERGED_CSV"
        HEADER_WRITTEN=true
    fi
    tail -n +2 "$CSV" >> "$MERGED_CSV"
done

if [ -f "$MERGED_CSV" ]; then
    LINES=$(wc -l < "$MERGED_CSV")
    echo "==> Merged CSV: $MERGED_CSV ($LINES lines)"
else
    echo "==> No CSV files to merge"
fi

echo ""
echo "==> Merging collstats CSV files..."

HEADER_WRITTEN=false
MERGED_COLLSTATS="$LOCAL_DIR/merged_collstats.csv"
for CSV in "$LOCAL_DIR"/client-*/collstats_*.csv; do
    if [ ! -f "$CSV" ]; then continue; fi

    if [ "$HEADER_WRITTEN" = false ]; then
        head -1 "$CSV" > "$MERGED_COLLSTATS"
        HEADER_WRITTEN=true
    fi
    tail -n +2 "$CSV" >> "$MERGED_COLLSTATS"
done

if [ -f "$MERGED_COLLSTATS" ]; then
    LINES=$(wc -l < "$MERGED_COLLSTATS")
    echo "==> Merged collstats CSV: $MERGED_COLLSTATS ($LINES lines)"
fi

echo ""
echo "==> Results collected to $LOCAL_DIR/"
echo "==> Generate charts with:"
echo "    python3 analysis/plot.py $MERGED_CSV"
