#!/bin/bash
set -euo pipefail

BENCH_DIR="/opt/mongodb-bench"
RESULTS_DIR="$BENCH_DIR/results"

mkdir -p "$BENCH_DIR" "$RESULTS_DIR"

echo "[$(date)] Downloading benchmark binary..."
aws s3 cp "${bench_binary_s3}" "$BENCH_DIR/mongodb-ai-bench"
chmod +x "$BENCH_DIR/mongodb-ai-bench"

echo "[$(date)] Downloading config..."
aws s3 cp "${bench_config_s3}" "$BENCH_DIR/config.yaml"

ulimit -n 65536
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sysctl -w net.ipv4.ip_local_port_range="1024 65535"

echo "[$(date)] Client ${client_id} of ${total_clients} ready"
echo "[$(date)] To start: $BENCH_DIR/mongodb-ai-bench -config $BENCH_DIR/config.yaml"
