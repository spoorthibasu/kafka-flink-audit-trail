#!/usr/bin/env bash
# The steps after the Kafka and Flink measurements, in order: the current-state table, the
# equivalence check, Iceberg statistics and history checks around expiration and compaction,
# and the Athena timing. Each step is skipped when run.py has already recorded it.
#   bench/aws/after_flink.sh bench/aws/config.aws.json
set -euo pipefail
cd "$(dirname "$0")/../.."
PY=${PYTHON:-python3}
run() { echo "=== $*"; "$PY" bench/aws/run.py --config "$1" "${@:2}"; }
run "$1" load-c
run "$1" equivalence
run "$1" stats after_commits
run "$1" expire
run "$1" stats after_expiration
run "$1" compact
run "$1" stats after_compaction
run "$1" expire
run "$1" stats after_compaction_and_expiration
run "$1" athena-timing
