# Kafka, Flink, and Iceberg audit-trail example.
# Everything below needs only JDK 11+ and Maven, except the targets that say Docker,
# and the Python benchmark, which needs bench/requirements.txt.
.PHONY: help test demo bench bench-full bench-smoke bench-resources bench-sweep bench-pipeline engine-docs wikimedia-study wikimedia-smoke kafka3-up kafka3-down sql-matrix trino-up trino-down crash-test loop build run produce up down

PYTHON ?= python3

help:
	@echo "make test        - unit tests (no infra)"
	@echo "make demo        - run sql/ over a seeded table; prints the reconstructed values"
	@echo "make bench       - schema size comparison on one synthetic event (schema lines, fields, Avro bytes)"
	@echo "make bench-full  - the full benchmark: schema, payload, storage, latency, scaling, resources, cost; writes results/"
	@echo "make bench-smoke - the same benchmark on a tiny config, to check the scripts run"
	@echo "make bench-resources - only the resource-use and cost stage; adds its files to results/"
	@echo "make bench-sweep - only the attribute-count sweep (K = 10, 25, 67, 150); adds its files to results/"
	@echo "make bench-pipeline - Kafka, Flink and Iceberg measurements for both schemas (needs 'make kafka3-up'; about an hour)"
	@echo "make engine-docs - write results/engine_docs.csv and its SUMMARY section"
	@echo "make wikimedia-study - Wikimedia field study steps 3 to 7 on WIKIMEDIA_CAPTURES (see study/wikimedia/README.md)"
	@echo "make wikimedia-smoke - the same steps on a synthetic capture, with planted counts checked"
	@echo "make kafka3-up   - start a three-broker Kafka for bench-pipeline (Docker); make kafka3-down stops it"
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
	@$(PYTHON) bench/resources.py --config bench/config.json --prices bench/prices.json --out results
	@$(PYTHON) bench/sweep_attributes.py --config bench/config.json --out results
	@$(PYTHON) bench/engine_docs.py --out results

bench-smoke:
	@$(PYTHON) bench/benchmark.py --config bench/config.smoke.json --out results-smoke
	@$(PYTHON) bench/resources.py --config bench/config.smoke.json --prices bench/prices.json --out results-smoke
	@$(PYTHON) bench/sweep_attributes.py --config bench/config.smoke.json --out results-smoke
	@$(PYTHON) bench/engine_docs.py --out results-smoke

bench-sweep:
	@$(PYTHON) bench/sweep_attributes.py --config bench/config.json --out results

# Pipeline measurements need Kafka: three brokers (make kafka3-up) for bench/config.json.
bench-pipeline:
	@$(PYTHON) bench/pipeline.py --config bench/config.json --out results

engine-docs:
	@$(PYTHON) bench/engine_docs.py --out results

# Wikimedia field study on captured public data; capture first with study/wikimedia/capture.py.
# pip install -r study/wikimedia/requirements.txt first.
WIKIMEDIA_DATA ?= $(HOME)/audit-trail-bench-data/wikimedia
WIKIMEDIA_CAPTURES ?= $(WIKIMEDIA_DATA)/capture-earliest-to-2026-10-20T00Z $(WIKIMEDIA_DATA)/capture-2026-10-20T00Z-2026-10-23T02Z
wikimedia-study:
	@$(PYTHON) study/wikimedia/study.py $(foreach c,$(WIKIMEDIA_CAPTURES),--capture $(c)) \
		--work $(WIKIMEDIA_DATA)/work --out results/wikimedia --summary results/SUMMARY.md

wikimedia-smoke:
	@rm -rf results-smoke-wikimedia
	@$(PYTHON) study/wikimedia/synthetic.py --out results-smoke-wikimedia/capture
	@$(PYTHON) study/wikimedia/study.py --capture results-smoke-wikimedia/capture --work results-smoke-wikimedia/work \
		--out results-smoke-wikimedia --summary results-smoke-wikimedia/SUMMARY.md --runs 3 --warmup 1 --buckets 2 \
		--expect results-smoke-wikimedia/capture/expected.json

kafka3-up:
	docker compose -f docker-compose.kafka3.yml up -d

kafka3-down:
	docker compose -f docker-compose.kafka3.yml down -v

# Resource use (CPU, peak memory, spill, bytes scanned) and cost (S3 storage, Athena scans)
# for the datasets in results/, without rerunning the rest of the benchmark. Writes
# resources.csv, cost.csv, prices.json, resources_environment.json; appends to SUMMARY.md.
bench-resources:
	@$(PYTHON) bench/resources.py --config bench/config.json --prices bench/prices.json --out results

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
	  '{"customerId":"98765","eventTimestamp":1000,"eventSequence":1,"email":"alice@gmail.com","phone":"555-397"}' \
	  '{"customerId":"98765","eventTimestamp":2000,"eventSequence":2,"email":"alice@yahoo.com","phone":"555-397"}' \
	  '{"customerId":"98765","eventTimestamp":3000,"eventSequence":3,"email":"alice@newjob.com","phone":"555-493"}' \
	  | docker exec -i kfat-kafka /opt/kafka/bin/kafka-console-producer.sh \
	    --topic customer-profile-changes --bootstrap-server localhost:9092
	@echo "produced 3 events"

down:
	docker compose down
