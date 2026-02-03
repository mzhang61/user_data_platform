#!/usr/bin/env bash
set -euo pipefail

API_PORT="${API_PORT:-18080}"
REDIS_PORT="${REDIS_PORT:-16379}"
TOPIC_IN="${TOPIC_IN:-user-events-raw-v2}"

echo "== flush redis =="
docker exec -it redis redis-cli -p "$REDIS_PORT" FLUSHDB >/dev/null

eid_ok="evt-ok-$(date +%s)"
uid_ok="u-ok-$(date +%s)"
eid_bad="evt-bad-$(date +%s)"
uid_bad="u-bad-$(date +%s)"

echo "== produce clean =="
docker exec -i kafka bash -lc "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic $TOPIC_IN" <<EOF
{"eventId":"$eid_ok","userId":"$uid_ok","eventType":"page_view","eventTime":"2026-01-30T20:00:00Z","sessionId":"s1","page":"/prod","device":"mac","properties":{"k":"v"}}
EOF

echo "== produce dlq =="
docker exec -i kafka bash -lc "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic $TOPIC_IN" <<EOF
{"eventId":"$eid_bad","userId":"$uid_bad","eventType":"click","eventTime":"not-a-time","sessionId":"s2","page":"/bad","device":"mac","properties":null}
EOF

sleep 2

echo "== query api =="
curl -s "localhost:${API_PORT}/health" | jq
curl -s "localhost:${API_PORT}/events/${eid_ok}" | jq
curl -s "localhost:${API_PORT}/users/${uid_ok}/events?limit=10&includeDetails=true" | jq
curl -s "localhost:${API_PORT}/dlq?limit=10" | jq

echo "== done =="