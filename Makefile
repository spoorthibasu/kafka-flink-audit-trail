# Kafka, Flink, and Iceberg audit-trail example.
# Everything below needs only JDK 11+ and Maven, except `run`/`produce` which add Docker.
.PHONY: help test demo bench loop build run produce up down

help:
	@echo "make test     - unit tests (no infra)"
	@echo "make demo     - run sql/ over a seeded table; prints the reconstructed values"
	@echo "make bench    - schema size comparison (schema lines, fields, Avro bytes)"
	@echo "make loop     - Flink writes a real Iceberg table, then query it back (no Kafka/Docker)"
	@echo "make build    - shaded fat jar for 'flink run'"
	@echo "make up       - start single-node Kafka (Docker)"
	@echo "make run      - run the Flink job; stays running, Ctrl+C to stop (needs 'make up')"
	@echo "make produce  - send the three sample events; run in a SECOND terminal while 'make run' is going"
	@echo "make down     - stop Kafka"

test:
	mvn test

demo:
	@mvn -q compile exec:java -Dexec.mainClass=com.example.audit.demo.DemoRunner

bench:
	@python3 bench/generate_schemas.py >/dev/null
	@mvn -q compile exec:java -Dexec.mainClass=com.example.audit.bench.SchemaBenchmark

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
