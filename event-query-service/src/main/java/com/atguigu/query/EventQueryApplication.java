package com.atguigu.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.resps.Tuple;

import java.time.Instant;
import java.util.*;

/**
 * Event Query Service (Spring Boot + Jedis)
 *
 * Redis schema:
 *  - event:{eventId}                  -> String (raw json)
 *  - user:{userId}:events             -> ZSET  member=eventId score=eventTimeMillis
 *  - dlq:events                       -> LIST  each item = dlq json
 *
 * Env:
 *  - REDIS_HOST (default localhost)
 *  - REDIS_PORT (default 6379)
 *  - REDIS_PASSWORD (optional)
 */
@SpringBootApplication
public class EventQueryApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventQueryApplication.class, args);
    }

    // ---------- beans ----------

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public JedisPooled jedis() {
        String host = env("REDIS_HOST", "localhost");
        int port = Integer.parseInt(env("REDIS_PORT", "6379"));
        String password = env("REDIS_PASSWORD", "");

        if (password.isBlank()) {
            return new JedisPooled(host, port);
        }

        // If you later set requirepass:
        // NOTE: if password contains special chars, you should URL-encode it.
        String uri = "redis://:" + password + "@" + host + ":" + port;
        return new JedisPooled(uri);
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    // ---------- controller ----------

    @RestController
    public static class ApiController {

        private final JedisPooled jedis;
        private final ObjectMapper om;

        public ApiController(JedisPooled jedis, ObjectMapper om) {
            this.jedis = jedis;
            this.om = om;
        }

        @GetMapping("/health")
        public ResponseEntity<?> health() {
            try {
                String pong = jedis.ping();
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("status", "UP");
                out.put("redis", pong);
                out.put("now", Instant.now().toString());
                return ResponseEntity.ok(out);
            } catch (Exception e) {
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("status", "DOWN");
                out.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
                return ResponseEntity.status(503).body(out);
            }
        }

        @GetMapping(value = "/events/{eventId}", produces = MediaType.APPLICATION_JSON_VALUE)
        public ResponseEntity<?> getEvent(@PathVariable String eventId) {
            String key = "event:" + eventId;
            String v = jedis.get(key);
            if (v == null) {
                return ResponseEntity.status(404).body(err("not_found", "event not found: " + eventId));
            }
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(v);
        }

        @GetMapping(value = "/users/{userId}/events", produces = MediaType.APPLICATION_JSON_VALUE)
        public ResponseEntity<?> getUserEvents(@PathVariable String userId,
                                               @RequestParam(defaultValue = "10") int limit,
                                               @RequestParam(defaultValue = "false") boolean includeDetails) {
            if (limit <= 0) limit = 10;
            if (limit > 100) limit = 100;

            String zkey = "user:" + userId + ":events";

            // Jedis version differences:
            // some versions return List<Tuple>, not Set<Tuple>
            List<Tuple> tuples = jedis.zrevrangeWithScores(zkey, 0, limit - 1);

            List<Object> items = new ArrayList<>(tuples.size());
            for (Tuple t : tuples) {
                String eventId = t.getElement();
                long scoreMillis = (long) t.getScore();

                Map<String, Object> item = new LinkedHashMap<>();
                item.put("eventId", eventId);
                item.put("eventTimeMillis", scoreMillis);
                item.put("eventTime", Instant.ofEpochMilli(scoreMillis).toString());

                if (includeDetails) {
                    String body = jedis.get("event:" + eventId);
                    if (body != null) {
                        try {
                            JsonNode node = om.readTree(body);
                            item.put("event", node);
                        } catch (Exception parseErr) {
                            item.put("eventRaw", body);
                            item.put("eventParseError", parseErr.getMessage());
                        }
                    } else {
                        item.put("event", null);
                    }
                }

                items.add(item);
            }

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("userId", userId);
            out.put("limit", limit);
            out.put("count", items.size());
            out.put("items", items);
            return ResponseEntity.ok(out);
        }

        @GetMapping(value = "/dlq", produces = MediaType.APPLICATION_JSON_VALUE)
        public ResponseEntity<?> dlq(@RequestParam(defaultValue = "10") int limit) {
            if (limit <= 0) limit = 10;
            if (limit > 100) limit = 100;

            List<String> raw = jedis.lrange("dlq:events", 0, limit - 1);
            List<Object> items = new ArrayList<>(raw.size());

            for (String s : raw) {
                try {
                    items.add(om.readTree(s));
                } catch (Exception parseErr) {
                    Map<String, Object> bad = new LinkedHashMap<>();
                    bad.put("raw", s);
                    bad.put("parseError", parseErr.getMessage());
                    items.add(bad);
                }
            }

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("limit", limit);
            out.put("count", items.size());
            out.put("items", items);
            return ResponseEntity.ok(out);
        }

        private Map<String, Object> err(String code, String message) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("code", code);
            m.put("message", message);
            m.put("timestamp", System.currentTimeMillis());
            return m;
        }
    }
}