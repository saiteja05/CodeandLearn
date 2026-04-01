#!/bin/bash
set -euo pipefail

# Runs the benchmark across multiple EC2 instances via systemd.
# Usage: ./run-distributed.sh <key-file> <mongodb-uri> <ip1> <ip2> ...
#
# Prerequisite: instances must be set up with user_data (Terraform) first.
# The MongoDB URI is written to a .env file on each instance, never passed
# as a command-line argument (which would be visible in `ps` output).

KEY_FILE="${1:?Usage: run-distributed.sh <key-file> <mongodb-uri> <ip1> [ip2] ...}"
MONGODB_URI="${2:?Usage: run-distributed.sh <key-file> <mongodb-uri> <ip1> [ip2] ...}"
shift 2
IPS=("$@")

if [ ${#IPS[@]} -eq 0 ]; then
    echo "Error: provide at least one IP address"
    exit 1
fi

BENCH_DIR="/opt/mongodb-bench"

echo "==> Deploying MongoDB URI and starting benchmark on ${#IPS[@]} clients..."

PIDS=()
for i in "${!IPS[@]}"; do
    IP="${IPS[$i]}"
    echo "  [client-$i] $IP - configuring and starting..."

    ssh -i "$KEY_FILE" -o StrictHostKeyChecking=accept-new "ec2-user@$IP" bash -s <<REMOTE
        sudo bash -c 'echo "MONGODB_URI=$MONGODB_URI" > $BENCH_DIR/.env'
        sudo chmod 0600 $BENCH_DIR/.env
        sudo systemctl restart mongodb-bench
REMOTE
    PIDS+=($!)
done

for PID in "${PIDS[@]}"; do
    wait "$PID" 2>/dev/null || true
done

echo ""
echo "==> All clients started. Monitor with:"
for i in "${!IPS[@]}"; do
    echo "  ssh -i $KEY_FILE ec2-user@${IPS[$i]} 'sudo journalctl -u mongodb-bench -f'"
    echo "  ssh -i $KEY_FILE ec2-user@${IPS[$i]} 'sudo tail -f $BENCH_DIR/bench.log'"
done

echo ""
echo "==> Check status:"
for i in "${!IPS[@]}"; do
    echo "  ssh -i $KEY_FILE ec2-user@${IPS[$i]} 'sudo systemctl status mongodb-bench'"
done

echo ""
echo "==> Results are synced to S3 every 5 minutes automatically."
echo "==> When complete, collect results with:"
echo "  ./collect-results.sh $KEY_FILE ${IPS[*]}"
