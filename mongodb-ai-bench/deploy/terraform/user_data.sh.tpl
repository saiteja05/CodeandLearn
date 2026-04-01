#!/bin/bash
set -euo pipefail

BENCH_DIR="/opt/mongodb-bench"
RESULTS_DIR="$BENCH_DIR/results"
LOG_FILE="$BENCH_DIR/bench.log"

mkdir -p "$BENCH_DIR" "$RESULTS_DIR"

# ---------- 1. Download binary and config from S3 ----------

echo "[$(date)] Downloading benchmark binary..."
aws s3 cp "${bench_binary_s3}" "$BENCH_DIR/mongodb-ai-bench"
chmod +x "$BENCH_DIR/mongodb-ai-bench"

echo "[$(date)] Downloading config..."
aws s3 cp "${bench_config_s3}" "$BENCH_DIR/config.yaml"

# ---------- 2. Persist kernel tuning ----------

cat > /etc/sysctl.d/99-bench.conf <<'SYSCTL'
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
SYSCTL
sysctl --system

cat > /etc/security/limits.d/99-bench.conf <<'LIMITS'
*  soft  nofile  65536
*  hard  nofile  65536
LIMITS

# ---------- 3. systemd service for the benchmark ----------

cat > /etc/systemd/system/mongodb-bench.service <<EOF
[Unit]
Description=MongoDB AI Chatbot Benchmark
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$BENCH_DIR/mongodb-ai-bench -config $BENCH_DIR/config.yaml
WorkingDirectory=$BENCH_DIR
Restart=on-failure
RestartSec=10
LimitNOFILE=65536
StandardOutput=append:$LOG_FILE
StandardError=append:$LOG_FILE
EnvironmentFile=-$BENCH_DIR/.env

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable mongodb-bench.service

# ---------- 4. Periodic S3 result sync (every 5 minutes) ----------

cat > /etc/cron.d/bench-s3-sync <<EOF
*/5 * * * * root aws s3 sync $RESULTS_DIR/ s3://${results_bucket}/client-${client_id}/results/ --quiet 2>>/var/log/bench-s3-sync.log
EOF
chmod 0644 /etc/cron.d/bench-s3-sync

# Also sync the log file every 15 minutes for remote debugging
cat > /etc/cron.d/bench-log-sync <<EOF
*/15 * * * * root aws s3 cp $LOG_FILE s3://${results_bucket}/client-${client_id}/bench.log --quiet 2>>/var/log/bench-s3-sync.log
EOF
chmod 0644 /etc/cron.d/bench-log-sync

# ---------- 5. Disk usage watchdog ----------
# Stops the benchmark if disk reaches 90% to prevent data loss from a full disk.

cat > $BENCH_DIR/disk-watchdog.sh <<'WATCHDOG'
#!/bin/bash
USAGE=$(df --output=pcent /opt/mongodb-bench | tail -1 | tr -d ' %')
if [ "$USAGE" -ge 90 ]; then
    echo "[$(date)] DISK WATCHDOG: usage at $${USAGE}%, stopping benchmark" >> /opt/mongodb-bench/bench.log
    systemctl stop mongodb-bench.service
    aws s3 sync /opt/mongodb-bench/results/ s3://${results_bucket}/client-${client_id}/results/ --quiet || true
fi
WATCHDOG
chmod +x $BENCH_DIR/disk-watchdog.sh

cat > /etc/cron.d/bench-disk-watchdog <<EOF
* * * * * root $BENCH_DIR/disk-watchdog.sh
EOF
chmod 0644 /etc/cron.d/bench-disk-watchdog

# ---------- 6. Final sync on shutdown ----------
# Ensures results are uploaded to S3 before the instance terminates.

cat > /etc/systemd/system/bench-final-sync.service <<EOF
[Unit]
Description=Final S3 sync of benchmark results on shutdown
DefaultDependencies=no
Before=shutdown.target reboot.target halt.target
After=mongodb-bench.service

[Service]
Type=oneshot
ExecStart=/bin/bash -c 'aws s3 sync $RESULTS_DIR/ s3://${results_bucket}/client-${client_id}/results/ && aws s3 cp $LOG_FILE s3://${results_bucket}/client-${client_id}/bench.log'
TimeoutStartSec=120

[Install]
WantedBy=halt.target reboot.target shutdown.target
EOF

systemctl daemon-reload
systemctl enable bench-final-sync.service

echo "[$(date)] Client ${client_id} of ${total_clients} ready"
echo "[$(date)] Start with: systemctl start mongodb-bench"
echo "[$(date)] Monitor with: journalctl -u mongodb-bench -f"
