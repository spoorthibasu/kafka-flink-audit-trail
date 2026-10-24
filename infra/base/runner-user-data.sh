#!/bin/bash
# First boot of the runner instance: tools, the NVMe instance store at /data, and a marker
# file the orchestrator waits for. Logs go to /var/log/cloud-init-output.log.
set -euxo pipefail

dnf install -y java-17-amazon-corretto-devel python3.11 python3.11-pip tar gzip jq
dnf install -y maven-amazon-corretto17 || dnf install -y maven

# The m8id instance store: one NVMe device whose model is "Amazon EC2 NVMe Instance Storage".
dev=$(lsblk -dpno NAME,MODEL | awk '/Instance Storage/ {print $1; exit}')
mkfs.xfs -f "$dev"
mkdir -p /data
mount -o noatime "$dev" /data
mkdir -p /data/steps /data/work /data/logs /data/code
chown -R ec2-user:ec2-user /data

java -version
mvn -version
python3.11 --version
date -u +%Y-%m-%dT%H:%M:%SZ > /var/lib/audit-trail-bench-ready
