#!/usr/bin/env bash
set -euo pipefail

echo "==> (1) Start infra"
docker compose down >/dev/null 2>&1 || true
docker compose up -d

echo "==> (2) Wait for kafka"
sleep 2

echo "==> (3) Create topics"
docker exec -it kafka bash -lc '
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic clean-events --partitions 2 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dlq-events   --partitions 2 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic user-events-raw-v2 --partitions 2 --replication-factor 1
echo "---- topics ----"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
'

echo "==> (4) Produce test messages"
docker exec -it kafka bash -lc '
cat <<MSG | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic user-events-raw-v2
{"eventId":"evt-ok-200","userId":"u-200","eventType":"click","eventTime":"2026-01-30T20:00:00Z","sessionId":"s-200","page":"/home","device":"mac","properties":{"ref":"ok"}}
{"eventId":"evt-bad-201","userId":"u-201","eventType":"click","eventTime":"not-a-time","sessionId":"s-201","page":"/bad","device":"mac","properties":null}
{"eventId":"evt-ok-202","userId":"u-202","eventType":"page_view","eventTime":"2026-01-30T20:00:00Z","sessionId":"s-202","page":"/prod","device":"mac","properties":{"k":"v"}}
MSG
'

echo
echo "==> (5) Now run stream-processor in another terminal:"
echo "cd ~/Documents/user_data_platform/stream-processor"
echo "export KAFKA_BOOTSTRAP_SERVERS=localhost:19092"
echo "java -jar target/stream-processor-0.0.1-SNAPSHOT-all.jar"
echo
echo "Tip: watch topics:"
echo '  clean: docker exec -it kafka bash -lc "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic clean-events --from-beginning"'
echo '  dlq:   docker exec -it kafka bash -lc "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic dlq-events --from-beginning"'
