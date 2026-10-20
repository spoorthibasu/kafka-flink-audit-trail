# Kafka, Flink, and Iceberg audit-trail example.
# Everything below needs only JDK 11+ and Maven, except the targets that say Docker,
# and the Python benchmark, which needs bench/requirements.txt.
.PHONY: help test demo bench bench-full bench-smoke sql-matrix trino-up trino-down crash-test loop build run produce up down

PYTHON ?= python3

help:
	@echo "make test        - unit tests (no infra)"
	@echo "make demo        - run sql/ over a seeded table; prints the reconstructed values"
	@echo "make bench       - schema size comparison on one synthetic event (schema lines, fields, Avro bytes)"
	@echo "make bench-full  - the full benchmark: schema, payload, storage, latency, scaling; writes results/"
	@echo "make bench-smoke - the same benchmark on a tiny config, to check the script runs"
	@echo "make sql-matrix  - run every sql/ file on DuckDB, Spark and Trino against the golden outputs (needs 'make trino-up')"
	@echo "make trino-up    - start a local Trino (Docker) for sql-matrix; make trino-down stops it"
	@echo "make crash-test  - Flink to Iceberg job: fail, restart from the last checkpoint, assert counts (needs 'make up')"
	@echo "make loop        - Flink writes a real Iceberg table, then query it back (no Kafka/Docker)"
	@echo "make build       - shaded fat jar for 'flink run'"
	@echo "make up          - start single-node Kafka (Docker)"
	@echo "make run         - run the Flink job; stays running, Ctrl+C to stop (needs 'make up')"
	@echo "make produce     - send the three sample events; run in a SECOND terminal while 'make run' is going"
	@echo "make down        - stop Kafka"

test:
	mvn test

demo:
	@mvn -q compile exec:java -Dexec.mainClass=com.example.audit.demo.DemoRunner

bench:
	@$(PYTHON) bench/generate_schemas.py >/dev/null
	@mvn -q compile exec:java -Dexec.mainClass=com.example.audit.bench.SchemaBenchmark

# Synthetic data at demonstration scale; every parameter is in bench/config.json.
# pip install -r bench/requirements.txt first.
bench-full:
	@$(PYTHON) bench/benchmark.py --config bench/config.json --out results

bench-smoke:
	@$(PYTHON) bench/benchmark.py --config bench/config.smoke.json --out results-smoke

# Cross-engine golden tests. pip install -r ci/requirements.txt first. ENGINES=duckdb,spark
# skips Trino when no container is running.
ENGINES ?= duckdb,spark,trino
sql-matrix:
	@$(PYTHON) ci/run_sql_matrix.py --engines $(ENGINES) --out ci/matrix.md

trino-up:
	docker run -d --name kfat-trino -p 8080:8080 trinodb/trino:483
	@for i in $$(seq 1 60); do \
	  if curl -sf localhost:8080/v1/info | grep -q '"starting":false'; then echo "trino ready"; exit 0; fi; sleep 2; \
	done; echo "trino did not start"; exit 1

trino-down:
	docker rm -f kfat-trino

# Crash and restart evidence against the Kafka that `make up` starts. Writes what it
# observed to results/crash_restart_case1.md and results/crash_restart_case2.md.
KAFKA_BOOTSTRAP ?= localhost:9092
crash-test:
	REQUIRE_KAFKA=1 KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) mvn -q test -Dtest=CrashRestartTest

loop:
	@mvn -q compile exec:exec@run-loop

build:
	mvn -q clean package -DskipTests

up:
	docker compose up -d
	@sleep 6
	@docker exec kfat-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
	  --topic customer-profile-changes --bootstrap-server localhost:9092

run:
	mvn -q compile exec:exec@run-job

produce:
	@printf '%s\n' \
	  '{"customerId":"98765","eventTimestamp":1000,"eventSequence":1,"email":"alice@gmail.com","phone":"555-1001","address":"123 Main St"}' \
	  '{"customerId":"98765","eventTimestamp":2000,"eventSequence":2,"email":"alice@yahoo.com"}' \
	  '{"customerId":"98765","eventTimestamp":3000,"eventSequence":3,"email":"alice@outlook.com"}' \
	  | docker exec -i kfat-kafka /opt/kafka/bin/kafka-console-producer.sh \
	    --topic customer-profile-changes --bootstrap-server localhost:9092
	@echo "produced 3 events"

down:
	docker compose down
