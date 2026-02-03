package com.atguigu.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.time.Instant;
import java.util.*;

/**
 * RedisWriters routes incoming records into CLEAN or DLQ, and provides Flink sinks
 * that write results into Redis.
 *
 * Redis keys written by this class:
 * 1) event:{eventId} -> JSON string of a single event
 * 2) user:{userId}:events -> ZSET(score=eventTimeMillis, member=eventId)
 * 3) dlq:events -> LIST (LPUSH newest first)
 *
 * Important:
 * - This class reads REDIS_HOST and REDIS_PORT from environment variables.
 * - For your docker-compose setup (redis container exposes 6379 and maps to 16379 on host),
 *   when running stream-processor on your Mac:
 *     export REDIS_HOST=localhost
 *     export REDIS_PORT=16379
 */
public class RedisWriters {

    public enum Kind { CLEAN, DLQ }

    public record RouteResult(Kind kind, String payload) {}

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String KEY_DLQ_LIST = "dlq:events";
    private static final String KEY_EVENT_PREFIX = "event:";
    private static final String KEY_USER_EVENTS_PREFIX = "user:";
    private static final String KEY_USER_EVENTS_SUFFIX = ":events";

    // Retention controls (can be overridden by env vars)
    private static final int KEEP_USER_EVENTS = intEnv("REDIS_USER_EVENTS_KEEP", 200);
    private static final int KEEP_DLQ_EVENTS  = intEnv("REDIS_DLQ_KEEP", 200);

    /**
     * Validates an input line and routes it to either CLEAN or DLQ.
     *
     * Input formats supported:
     * - "ingestTime=... | {json}" (preferred)
     * - "{json}" (raw JSON)
     *
     * JSON formats supported:
     * - Envelope: {"schemaVersion":1,"ingestedAt":"...","source":"...","event":{...}}
     * - Raw event: {"eventId":"...","userId":"...","eventTime":"..."}
     */
    public static RouteResult validateAndRoute(String line) {
        String json = extractJsonPart(line);

        Map<String, Object> root;
        try {
            root = MAPPER.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            return new RouteResult(
                    Kind.DLQ,
                    dlq("invalid_json", json, List.of(err("_", "invalid JSON: " + e.getMessage())))
            );
        }

        // Support envelope OR raw event.
        Map<String, Object> eventObj = unwrapEvent(root);

        // If it is already a DLQ envelope from upstream, pass through as DLQ.
        if (isDlqEnvelope(eventObj)) {
            return new RouteResult(Kind.DLQ, json);
        }

        String eventId = asString(eventObj.get("eventId"));
        String userId  = asString(eventObj.get("userId"));
        String eventTimeStr = asString(eventObj.get("eventTime"));

        List<Map<String, String>> errors = new ArrayList<>();

        if (isBlank(eventId)) errors.add(err("eventId", "eventId is required"));
        if (isBlank(userId))  errors.add(err("userId", "userId is required"));

        if (isBlank(eventTimeStr)) {
            errors.add(err("eventTime", "eventTime is required"));
        } else {
            try {
                Instant.parse(eventTimeStr);
            } catch (Exception ex) {
                errors.add(err("eventTime", "eventTime must be an ISO-8601 instant like 2026-01-30T20:00:00Z"));
            }
        }

        if (!errors.isEmpty()) {
            Map<String, Object> payload = new LinkedHashMap<>(eventObj);
            return new RouteResult(Kind.DLQ, dlq("validation_failed", payload, errors));
        }

        // Valid => keep original line (e.g., "ingestTime=... | {json}").
        return new RouteResult(Kind.CLEAN, line);
    }

    // -------------------- Redis sinks --------------------

    /**
     * Writes clean events to Redis:
     * - event:{eventId} -> JSON event
     * - user:{userId}:events -> ZSET with eventTime as score
     *
     * Safety net:
     * Even if routing is wrong upstream, this sink re-validates "eventTime".
     * If eventTime is invalid, it will push into dlq:events and skip writing CLEAN keys.
     */
    public static class CleanRedisSink extends RichSinkFunction<String> {
        private transient JedisPool pool;

        @Override
        public void open(Configuration parameters) {
            String host = redisHost();
            int port = redisPort();
            System.out.println("[redis] CleanRedisSink connecting to " + host + ":" + port);
            pool = new JedisPool(host, port);

            // Fail-fast connectivity check (prints useful error if Redis is unreachable).
            try (Jedis jedis = pool.getResource()) {
                String pong = jedis.ping();
                System.out.println("[redis] CleanRedisSink ping: " + pong);
            } catch (Exception e) {
                System.err.println("[redis] CleanRedisSink cannot connect: " + e.getMessage());
                throw e;
            }
        }

        @Override
        public void invoke(String cleanLine, Context context) {
            // Keep while debugging; remove later if too noisy.
            System.out.println("[redis] CLEAN sink got record");

            String json = extractJsonPart(cleanLine);

            Map<String, Object> root;
            try {
                root = MAPPER.readValue(json, new TypeReference<>() {});
            } catch (Exception e) {
                // If we cannot parse it, push it into Redis DLQ list.
                safePushToRedisDlq("invalid_json", json, List.of(err("_", e.getMessage())));
                return;
            }

            Map<String, Object> eventObj = unwrapEvent(root);

            // If upstream accidentally sent a DLQ envelope into clean stream, store it as DLQ.
            if (isDlqEnvelope(eventObj)) {
                safePushToRedisDlq("already_dlq_envelope", root, List.of(err("_", "CLEAN sink received a DLQ envelope")));
                return;
            }

            String eventId = asString(eventObj.get("eventId"));
            String userId  = asString(eventObj.get("userId"));
            String eventTimeStr = asString(eventObj.get("eventTime"));

            // Required fields guard (should already be valid, but keep for safety).
            if (isBlank(eventId) || isBlank(userId)) {
                safePushToRedisDlq(
                        "missing_ids",
                        eventObj,
                        List.of(err("_", "CLEAN record missing eventId or userId"))
                );
                return;
            }

            // IMPORTANT: re-validate eventTime here so Redis DLQ always works even if routing is broken.
            long scoreMillis;
            try {
                scoreMillis = Instant.parse(eventTimeStr).toEpochMilli();
            } catch (Exception ex) {
                safePushToRedisDlq(
                        "invalid_event_time",
                        eventObj,
                        List.of(err("eventTime", "eventTime must be an ISO-8601 instant like 2026-01-30T20:00:00Z"))
                );
                return;
            }

            String eventJson;
            try {
                eventJson = MAPPER.writeValueAsString(eventObj);
            } catch (Exception e) {
                eventJson = json; // fallback
            }

            try (Jedis jedis = pool.getResource()) {
                // 1) event:{eventId} -> JSON
                jedis.set(KEY_EVENT_PREFIX + eventId, eventJson);

                // 2) user:{userId}:events -> ZSET(score=eventTimeMillis, member=eventId)
                String zkey = KEY_USER_EVENTS_PREFIX + userId + KEY_USER_EVENTS_SUFFIX;
                jedis.zadd(zkey, scoreMillis, eventId);

                // Trim old events to keep only newest KEEP_USER_EVENTS
                trimUserEventsZset(jedis, zkey, KEEP_USER_EVENTS);
            } catch (JedisConnectionException jce) {
                System.err.println("[redis] CLEAN write failed (connection): " + jce.getMessage());
            } catch (Exception ex) {
                System.err.println("[redis] CLEAN write failed: " + ex.getMessage());
            }
        }

        private void safePushToRedisDlq(String reason, Object payload, List<Map<String, String>> errors) {
            String dlqJson = dlq(reason, payload, errors);
            try (Jedis jedis = pool.getResource()) {
                jedis.lpush(KEY_DLQ_LIST, dlqJson);
                jedis.ltrim(KEY_DLQ_LIST, 0, KEEP_DLQ_EVENTS - 1);
            } catch (Exception ex) {
                System.err.println("[redis] Failed to write into Redis DLQ list: " + ex.getMessage());
            }
        }

        private void trimUserEventsZset(Jedis jedis, String zkey, int keep) {
            long size = jedis.zcard(zkey);
            if (size <= keep) return;

            long removeCount = size - keep;
            // Remove the oldest (lowest scores) by rank.
            jedis.zremrangeByRank(zkey, 0, removeCount - 1);
        }
    }

    /**
     * Writes DLQ envelopes to Redis list:
     * - dlq:events -> LIST (LPUSH newest first)
     */
    public static class DlqRedisSink extends RichSinkFunction<String> {
        private transient JedisPool pool;

        @Override
        public void open(Configuration parameters) {
            String host = redisHost();
            int port = redisPort();
            System.out.println("[redis] DlqRedisSink connecting to " + host + ":" + port);
            pool = new JedisPool(host, port);

            // Fail-fast connectivity check.
            try (Jedis jedis = pool.getResource()) {
                String pong = jedis.ping();
                System.out.println("[redis] DlqRedisSink ping: " + pong);
            } catch (Exception e) {
                System.err.println("[redis] DlqRedisSink cannot connect: " + e.getMessage());
                throw e;
            }
        }

        @Override
        public void invoke(String dlqJsonOrLine, Context context) {
            System.out.println("[redis] DLQ sink got record");

            String dlqJson = dlqJsonOrLine.trim();

            // If it is still a raw line with "ingestTime=... | ...", wrap it into a DLQ envelope.
            if (dlqJson.startsWith("ingestTime=")) {
                String raw = extractJsonPart(dlqJsonOrLine);
                dlqJson = dlq("validation_failed", raw, List.of(err("_", "routed to dlq")));
            }

            try (Jedis jedis = pool.getResource()) {
                jedis.lpush(KEY_DLQ_LIST, dlqJson);
                jedis.ltrim(KEY_DLQ_LIST, 0, KEEP_DLQ_EVENTS - 1);
            } catch (JedisConnectionException jce) {
                System.err.println("[redis] DLQ write failed (connection): " + jce.getMessage());
            } catch (Exception ex) {
                System.err.println("[redis] DLQ write failed: " + ex.getMessage());
            }
        }
    }

    // -------------------- helpers --------------------

    /**
     * Returns true if the given map looks like a DLQ envelope's inner event structure.
     * Expected keys: reason, payload, errors
     */
    private static boolean isDlqEnvelope(Map<String, Object> eventObj) {
        return eventObj.containsKey("reason")
                && eventObj.containsKey("payload")
                && eventObj.containsKey("errors");
    }

    /**
     * If root has an "event" object, return that object as a Map; otherwise return root as-is.
     */
    private static Map<String, Object> unwrapEvent(Map<String, Object> root) {
        Object evt = root.get("event");
        if (evt instanceof Map<?, ?> m) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                out.put(String.valueOf(e.getKey()), e.getValue());
            }
            return out;
        }
        return root;
    }

    /**
     * Extracts the JSON part from a line of format: "ingestTime=... | {json}".
     * If there is no '|', returns the trimmed line.
     */
    private static String extractJsonPart(String line) {
        int idx = line.indexOf("|");
        if (idx < 0) return line.trim();
        return line.substring(idx + 1).trim();
    }

    /**
     * Builds a DLQ envelope as a JSON string.
     */
    private static String dlq(String reason, Object payload, List<Map<String, String>> errors) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("schemaVersion", 1);
        envelope.put("ingestedAt", Instant.now().toString());
        envelope.put("source", "stream-processor");

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("reason", reason);
        event.put("payload", payload);
        event.put("errors", errors);

        envelope.put("event", event);

        try {
            return MAPPER.writeValueAsString(envelope);
        } catch (Exception e) {
            return "{\"schemaVersion\":1,\"source\":\"stream-processor\",\"event\":{\"reason\":\"" + reason + "\"}}";
        }
    }

    /**
     * Creates a standardized error object for DLQ envelopes.
     */
    private static Map<String, String> err(String field, String msg) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("field", field);
        m.put("message", msg);
        return m;
    }

    private static String asString(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String redisHost() {
        return System.getenv().getOrDefault("REDIS_HOST", "localhost");
    }

    private static int redisPort() {
        return intEnv("REDIS_PORT", 16379);
    }

    private static int intEnv(String k, int def) {
        try {
            String v = System.getenv(k);
            if (v == null || v.isBlank()) return def;
            return Integer.parseInt(v.trim());
        } catch (Exception e) {
            return def;
        }
    }
}