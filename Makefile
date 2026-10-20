# Kafka, Flink, and Iceberg audit-trail example.
# Everything below needs only JDK 11+ and Maven, except the targets that say Docker,
# and the Python benchmark, which needs bench/requirements.txt.
.PHONY: help test demo bench bench-full bench-smoke loop build run produce up down

PYTHON ?= python3

help:
	@echo "make test        - unit tests (no infra)"
	@echo "make demo        - run sql/ over a seeded table; prints the reconstructed values"
	@echo "make bench       - schema size comparison on one synthetic event (schema lines, fields, Avro bytes)"
	@echo "make bench-full  - the full benchmark: schema, payload, storage, latency, scaling; writes results/"
	@echo "make bench-smoke - the same benchmark on a tiny config, to check the script runs"
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
