#!/bin/bash
set -euo pipefail

# Collects benchmark results from all EC2 client instances.
# Usage: ./collect-results.sh <key-file> <ip1> <ip2> ...

KEY_FILE="${1:?Usage: collect-results.sh <key-file> <ip1> [ip2] ...}"
shift
IPS=("$@")

BENCH_DIR="/opt/mongodb-bench"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOCAL_DIR="collected_results_${TIMESTAMP}"
mkdir -p "$LOCAL_DIR"

echo "==> Collecting results from ${#IPS[@]} clients..."

for i in "${!IPS[@]}"; do
    IP="${IPS[$i]}"
    CLIENT_DIR="$LOCAL_DIR/client-$i"
    mkdir -p "$CLIENT_DIR"
    
    echo "  [client-$i] $IP - downloading results..."
    scp -i "$KEY_FILE" -o StrictHostKeyChecking=no -r \
        "ec2-user@$IP:$BENCH_DIR/results/*" "$CLIENT_DIR/" 2>/dev/null || \
        echo "  [client-$i] Warning: no results found"
    
    scp -i "$KEY_FILE" -o StrictHostKeyChecking=no \
        "ec2-user@$IP:$BENCH_DIR/bench.log" "$CLIENT_DIR/bench.log" 2>/dev/null || \
        echo "  [client-$i] Warning: no log found"
done

echo "==> Results collected to $LOCAL_DIR/"
echo "==> Merging CSV files..."

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

echo "==> Done. Generate charts with:"
echo "    python3 analysis/plot.py $MERGED_CSV"
