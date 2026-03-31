#!/bin/bash
set -euo pipefail

# Setup script for manually configuring an EC2 instance as a benchmark client.
# Usage: ./setup-client.sh <s3-binary-path> <s3-config-path>

BINARY_S3="${1:?Usage: setup-client.sh <s3-binary-path> <s3-config-path>}"
CONFIG_S3="${2:?Usage: setup-client.sh <s3-binary-path> <s3-config-path>}"

BENCH_DIR="/opt/mongodb-bench"
mkdir -p "$BENCH_DIR/results"

echo "==> Downloading benchmark binary..."
aws s3 cp "$BINARY_S3" "$BENCH_DIR/mongodb-ai-bench"
chmod +x "$BENCH_DIR/mongodb-ai-bench"

echo "==> Downloading config..."
aws s3 cp "$CONFIG_S3" "$BENCH_DIR/config.yaml"

echo "==> Tuning OS for high connection counts..."
cat >> /etc/security/limits.conf <<'EOF'
* soft nofile 65536
* hard nofile 65536
EOF

cat >> /etc/sysctl.conf <<'EOF'
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.core.netdev_max_backlog = 65535
EOF
sysctl -p

echo "==> Setup complete. Run:"
echo "    $BENCH_DIR/mongodb-ai-bench -config $BENCH_DIR/config.yaml"
