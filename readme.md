
# User Data Platform (Kafka + Stream Processor + Redis + Query API + React UI)

This project is a minimal end-to-end user behavior data pipeline:

1) Frontend (React UI) or any client sends user events to **event-collector** via HTTP.
2) event-collector publishes raw events to **Kafka** topic: `user-events-raw-v2`.
3) **stream-processor** consumes Kafka events, validates them, and writes:
    - clean event -> Redis key `event:{eventId}`
    - user timeline index -> Redis ZSET `user:{userId}:events` (score = eventTimeMillis)
    - invalid event -> Redis List `dlq:events` AND Kafka topic `dlq-events`
4) **event-query-service** reads Redis and provides query APIs for UI and curl testing.

---

## Prerequisites

- JDK 21 (recommended: Homebrew OpenJDK 21)
- Maven 3.9+
- Node 18+ (for React UI)
- Docker + docker compose

Check:
```bash
java -version
mvn -v
node -v
docker -v
docker compose version


⸻

Project Structure
	•	infra/                 Docker compose for Kafka/Redis/Kafka-UI
	•	event-collector/       Spring Boot HTTP ingest service -> Kafka
	•	stream-processor/      Kafka consumer + validation + Redis writers (fat jar)
	•	event-query-service/   Spring Boot query API -> Redis
	•	event-query-ui-simple/ React UI (Vite) for one-click E2E test

⸻

Redis Data Model

Clean event storage:
	•	Key: event:{eventId}
	•	Value: JSON string of the clean event

User timeline index:
	•	ZSET: user:{userId}:events
	•	Member: {eventId}
	•	Score: {eventTimeMillis}

DLQ:
	•	List: dlq:events
	•	Each item is an envelope with reason + errors + original payload

⸻

Ports
	•	Kafka (host): localhost:19092
	•	Kafka (container): kafka:9092
	•	Redis: localhost:16379
	•	Kafka UI: http://localhost:8080
	•	event-query-service: http://localhost:18080
	•	event-collector: http://localhost:18081
	•	React UI dev server: http://localhost:5173

⸻

0) Make sure zsh uses correct JDK 21

Run this in every new terminal tab (or put into ~/.zshrc):

export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$JAVA_HOME/bin:$PATH"
java -version


⸻

1) Start Infra (Kafka + Redis + Kafka UI)

cd /Users/mmm/Documents/user_data_platform/infra
docker compose up -d
docker ps --format "table {{.Names}}\t{{.Ports}}"

Verify topics exist:

docker exec -it kafka bash -lc '/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | egrep "^(user-events-raw-v2|clean-events|dlq-events)$" || true'


⸻

2) Start stream-processor (Kafka -> Redis)

cd /Users/mmm/Documents/user_data_platform/stream-processor
mvn -q -DskipTests package

pkill -f "stream-processor-0.0.1-SNAPSHOT-all.jar" >/dev/null 2>&1 || true

export KAFKA_BOOTSTRAP_SERVERS=localhost:19092
export REDIS_HOST=localhost
export REDIS_PORT=16379

nohup java -jar target/stream-processor-0.0.1-SNAPSHOT-all.jar > /tmp/stream-processor.log 2>&1 &
sleep 1

pgrep -f "stream-processor-0.0.1-SNAPSHOT-all.jar" | head -n 1
tail -n 5 /tmp/stream-processor.log

Expected log contains something like:
[stream-processor] bootstrap=localhost:19092 topicIn=user-events-raw-v2 ...

⸻

3) Start event-query-service (Redis -> HTTP API)

cd /Users/mmm/Documents/user_data_platform/event-query-service
pkill -f "EventQueryApplication" >/dev/null 2>&1 || true

export REDIS_HOST=localhost
export REDIS_PORT=16379

nohup mvn -q -DskipTests spring-boot:run -Dspring-boot.run.arguments="--server.port=18080" > /tmp/event-query-service.log 2>&1 &
sleep 2

curl -s localhost:18080/health | jq
tail -n 10 /tmp/event-query-service.log


⸻

4) Start event-collector (HTTP ingest -> Kafka)

cd /Users/mmm/Documents/user_data_platform/event-collector
pkill -f "EventCollectorApplication" >/dev/null 2>&1 || true

export KAFKA_BOOTSTRAP_SERVERS=localhost:19092

nohup mvn -q -DskipTests spring-boot:run -Dspring-boot.run.arguments="--server.port=18081" > /tmp/event-collector.log 2>&1 &
sleep 2

curl -s localhost:18081/health | jq
tail -n 10 /tmp/event-collector.log


⸻

5) End-to-End Test (NO UI, pure CLI)

Clear Redis:

docker exec -it redis redis-cli FLUSHDB >/dev/null

Send a valid event to Kafka directly:

docker exec -i kafka bash -lc '/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic user-events-raw-v2' <<'EOF'
{"eventId":"evt-readme-ok-1","userId":"u-readme-1","eventType":"page_view","eventTime":"2026-01-30T20:00:00Z","sessionId":"s-readme-1","page":"/prod","device":"mac","properties":{"k":"v"}}
EOF
sleep 2

Verify Redis write:

docker exec -it redis redis-cli GET event:evt-readme-ok-1
docker exec -it redis redis-cli ZREVRANGE user:u-readme-1:events 0 9 WITHSCORES
docker exec -it redis redis-cli LLEN dlq:events

Verify query API:

curl -s localhost:18080/events/evt-readme-ok-1 | jq
curl -s "localhost:18080/users/u-readme-1/events?limit=10&includeDetails=true" | jq
curl -s "localhost:18080/dlq?limit=10" | jq

Send a bad event time (should go DLQ):

docker exec -i kafka bash -lc '/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic user-events-raw-v2' <<'EOF'
{"eventId":"evt-readme-bad-1","userId":"u-bad-1","eventType":"click","eventTime":"not-a-time","sessionId":"s-bad-1","page":"/bad","device":"mac","properties":null}
EOF
sleep 2

docker exec -it redis redis-cli LLEN dlq:events
docker exec -it redis redis-cli LRANGE dlq:events 0 0
curl -s "localhost:18080/dlq?limit=10" | jq


⸻

6) Start React UI (one-click test)

Assume you extracted event-query-ui-simple/ into project root.

cd /Users/mmm/Documents/user_data_platform/event-query-ui-simple
npm install
npm run dev

Open:
	•	http://localhost:5173

The UI calls:
	•	/collector/ingest -> proxied to http://localhost:18081/ingest
	•	/query/events/... -> proxied to http://localhost:18080/events/...
	•	/query/users/...  -> proxied to http://localhost:18080/users/...
	•	/query/dlq        -> proxied to http://localhost:18080/dlq

So backend MUST keep running while you click UI buttons.

⸻

How user behavior data is collected?

Two supported ways:

A) Realistic way (Frontend -> event-collector)

Frontend (or any app) sends an HTTP POST to:
	•	POST http://localhost:18081/ingest
Body example:

{
  "eventId":"evt-123",
  "userId":"u-123",
  "eventType":"page_view",
  "eventTime":"2026-01-30T20:00:00Z",
  "sessionId":"s-123",
  "page":"/prod",
  "device":"mac",
  "properties":{"k":"v"}
}

event-collector writes it to Kafka topic user-events-raw-v2.

B) Dev/test shortcut (CLI -> Kafka)

You can write JSON directly into Kafka (as shown above) to simulate ingestion.

⸻

Key Code (important parts)

event-collector: HTTP -> Kafka (EventController)

@RestController
@RequiredArgsConstructor
public class EventController {

  private final EventProducer producer;

  @GetMapping("/health")
  public Map<String,Object> health() {
    return Map.of("status","UP","service","event-collector","now", Instant.now().toString());
  }

  @PostMapping("/ingest")
  public Mono<Map<String,Object>> ingest(@RequestBody Map<String,Object> payload) {
    String eventId = String.valueOf(payload.getOrDefault("eventId", UUID.randomUUID().toString()));
    String userId  = String.valueOf(payload.getOrDefault("userId", "unknown"));
    payload.put("eventId", eventId);
    payload.putIfAbsent("schemaVersion", 1);
    payload.putIfAbsent("ingestedAt", Instant.now().toString());
    payload.putIfAbsent("source", "event-collector");

    producer.send(payload);

    return Mono.just(Map.of(
      "ok", true,
      "topic", "user-events-raw-v2",
      "eventId", eventId,
      "userId", userId
    ));
  }
}

stream-processor: validate + route (RedisWriters)

Clean path writes:
	•	event:{eventId}
	•	user:{userId}:events (ZSET score = eventTimeMillis)

DLQ writes:
	•	dlq:events (List)
	•	Kafka topic dlq-events

public void writeClean(EventRecord e, long eventTimeMillis) {
  String eventKey = "event:" + e.eventId();
  redis.set(eventKey, e.rawJson());

  String zkey = "user:" + e.userId() + ":events";
  redis.zadd(zkey, eventTimeMillis, e.eventId());
}

public void writeDlq(String dlqJson) {
  redis.lpush("dlq:events", dlqJson);
}

event-query-service: Redis -> API

@GetMapping("/events/{eventId}")
public Map<String,Object> getEvent(@PathVariable String eventId) {
  String s = redis.get("event:" + eventId);
  if (s == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "event not found");
  return json.readValue(s, Map.class);
}

@GetMapping("/users/{userId}/events")
public Map<String,Object> getUserEvents(@PathVariable String userId,
                                        @RequestParam(defaultValue="10") int limit,
                                        @RequestParam(defaultValue="false") boolean includeDetails) {
  List<Tuple> tuples = redis.zrevrangeWithScores("user:" + userId + ":events", 0, limit-1);
  // ... build response items, optionally attach event JSON from event:{eventId}
  return Map.of("userId", userId, "limit", limit, "count", tuples.size(), "items", items);
}

@GetMapping("/dlq")
public Map<String,Object> getDlq(@RequestParam(defaultValue="10") int limit) {
  List<String> items = redis.lrange("dlq:events", 0, limit-1);
  return Map.of("limit", limit, "count", items.size(), "items", parsed);
}


⸻

Stop Everything

pkill -f "EventCollectorApplication" >/dev/null 2>&1 || true
pkill -f "EventQueryApplication" >/dev/null 2>&1 || true
pkill -f "stream-processor-0.0.1-SNAPSHOT-all.jar" >/dev/null 2>&1 || true

cd /Users/mmm/Documents/user_data_platform/infra
docker compose down

