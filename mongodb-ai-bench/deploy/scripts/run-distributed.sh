#!/bin/bash
set -euo pipefail

# Runs the benchmark across multiple EC2 instances in parallel.
# Usage: ./run-distributed.sh <key-file> <ip1> <ip2> ...
#
# Prerequisite: instances must be set up with setup-client.sh or user_data first.

KEY_FILE="${1:?Usage: run-distributed.sh <key-file> <ip1> [ip2] ...}"
shift
IPS=("$@")

if [ ${#IPS[@]} -eq 0 ]; then
    echo "Error: provide at least one IP address"
    exit 1
fi

BENCH_DIR="/opt/mongodb-bench"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOCAL_RESULTS="results_${TIMESTAMP}"
mkdir -p "$LOCAL_RESULTS"

echo "==> Starting benchmark on ${#IPS[@]} clients..."

PIDS=()
for i in "${!IPS[@]}"; do
    IP="${IPS[$i]}"
    echo "  [client-$i] $IP - starting..."
    
    ssh -i "$KEY_FILE" -o StrictHostKeyChecking=accept-new "ec2-user@$IP" \
        "nohup $BENCH_DIR/mongodb-ai-bench -config $BENCH_DIR/config.yaml > $BENCH_DIR/bench.log 2>&1 &" &
    PIDS+=($!)
done

for PID in "${PIDS[@]}"; do
    wait "$PID" 2>/dev/null || true
done

echo "==> All clients started. Monitor with:"
for i in "${!IPS[@]}"; do
    echo "  ssh -i $KEY_FILE ec2-user@${IPS[$i]} 'tail -f $BENCH_DIR/bench.log'"
done

echo ""
echo "==> When complete, collect results with:"
echo "  ./collect-results.sh $KEY_FILE ${IPS[*]}"
