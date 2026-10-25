#!/usr/bin/env bash
# Runs com.example.audit.aws.RunnerTools (or MAIN=<class>) with the flink-app classes and
# their compile and provided dependencies, so local Flink jobs (C table load, compaction)
# run in this JVM. Builds the classes and the classpath file on first use.
#   bench/aws/tools.sh produce --bootstrap host:9092 --topic t ...
set -euo pipefail
APP="$(cd "$(dirname "$0")" && pwd)/flink-app"
if [ ! -f "$APP/target/cp.txt" ] || [ ! -d "$APP/target/classes" ]; then
  (cd "$APP" && mvn -q -B compile dependency:build-classpath -Dmdep.includeScope=compile -Dmdep.outputFile=target/cp.txt)
fi
OPENS=()
for p in java.lang java.net java.io java.nio sun.nio.ch java.lang.reflect java.util java.util.concurrent \
         java.util.concurrent.atomic java.time java.text; do
  OPENS+=("--add-opens=java.base/$p=ALL-UNNAMED")
done
# JAVA_OPTS is intentionally word-split (for example "-Xmx24g -XX:+UseG1GC").
# shellcheck disable=SC2086
exec java ${JAVA_OPTS:-} -XX:+IgnoreUnrecognizedVMOptions "${OPENS[@]}" \
  -cp "$APP/target/classes:$(cat "$APP/target/cp.txt")" "${MAIN:-com.example.audit.aws.RunnerTools}" "$@"
