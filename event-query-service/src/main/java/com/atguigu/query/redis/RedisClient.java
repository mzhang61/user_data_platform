package com.atguigu.query.redis;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.resps.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A thin Redis access layer using Jedis.
 *
 * Redis keys used (must match your stream-processor writer):
 * 1) event:{eventId} -> JSON string of a single event
 * 2) user:{userId}:events -> ZSET(score=eventTimeMillis, member=eventId)
 * 3) dlq:events -> LIST (LPUSH newest first)
 */
@Component
public class RedisClient {

    private final JedisPool pool;

    public RedisClient(
            @Value("${app.redis.host}") String host,
            @Value("${app.redis.port}") int port,
            @Value("${app.redis.timeoutMs}") int timeoutMs
    ) {
        // Basic pool configuration.
        // You can tune these numbers later if needed.
        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(16);
        cfg.setMaxIdle(16);
        cfg.setMinIdle(0);

        this.pool = new JedisPool(cfg, host, port, timeoutMs);

        // Quick sanity check: fail fast if Redis is unreachable.
        try (Jedis jedis = pool.getResource()) {
            jedis.ping();
        }
    }

    /**
     * Close Jedis pool when Spring shuts down.
     */
    @PreDestroy
    public void close() {
        pool.close();
    }

    /**
     * Get a single event JSON from Redis: event:{eventId}.
     */
    public Optional<String> getEvent(String eventId) {
        if (eventId == null || eventId.isBlank()) return Optional.empty();
        try (Jedis jedis = pool.getResource()) {
            String v = jedis.get("event:" + eventId);
            return Optional.ofNullable(v);
        }
    }

    /**
     * Read the newest event IDs for a user from ZSET:
     * user:{userId}:events (score = eventTimeMillis).
     *
     * Returns newest first.
     */
    public List<Map.Entry<String, Double>> getUserEventIdsWithScores(String userId, int limit) {
        if (userId == null || userId.isBlank()) return List.of();

        int safeLimit = Math.max(1, Math.min(limit, 500));
        String zkey = "user:" + userId + ":events";

        try (Jedis jedis = pool.getResource()) {
            // Jedis 5 returns List<Tuple> here (not Set<Tuple>)
            List<Tuple> tuples = jedis.zrevrangeWithScores(zkey, 0, safeLimit - 1);

            List<Map.Entry<String, Double>> out = new ArrayList<>(tuples.size());
            for (Tuple t : tuples) {
                out.add(Map.entry(t.getElement(), t.getScore()));
            }
            return out;
        }
    }

    /**
     * Pipeline GET calls for event:{id} to reduce round-trips.
     * Keeps the same order as input IDs.
     */
    public List<String> mgetEventsByIdsInOrder(List<String> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) return List.of();

        try (Jedis jedis = pool.getResource()) {
            Pipeline p = jedis.pipelined();

            List<Response<String>> responses = new ArrayList<>(eventIds.size());
            for (String id : eventIds) {
                responses.add(p.get("event:" + id));
            }
            p.sync();

            List<String> out = new ArrayList<>(eventIds.size());
            for (Response<String> r : responses) {
                out.add(r.get()); // may be null if key does not exist
            }
            return out;
        }
    }

    /**
     * Read DLQ list: dlq:events (newest first).
     */
    public List<String> getDlq(int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        try (Jedis jedis = pool.getResource()) {
            return jedis.lrange("dlq:events", 0, safeLimit - 1);
        }
    }
}