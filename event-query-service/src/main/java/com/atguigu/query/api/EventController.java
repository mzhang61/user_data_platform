package com.atguigu.query.api;

import com.atguigu.query.api.dto.DlqItem;
import com.atguigu.query.api.dto.UserEventItem;
import com.atguigu.query.redis.RedisClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * REST endpoints for querying Redis-written events.
 */
@RestController
@RequestMapping("/api")
public class EventController {

    private final RedisClient redis;

    public EventController(RedisClient redis) {
        this.redis = redis;
    }

    /**
     * GET /api/events/{eventId}
     * Returns the JSON stored in Redis key "event:{eventId}".
     */
    @GetMapping("/events/{eventId}")
    public ResponseEntity<String> getEvent(@PathVariable String eventId) {
        return redis.getEvent(eventId)
                .map(json -> ResponseEntity.ok().body(json))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * GET /api/users/{userId}/events?limit=50
     *
     * 1) Read newest event IDs from ZSET: user:{userId}:events
     * 2) Pipeline GET event:{id} for each id
     * 3) Return list ordered by newest first
     */
    @GetMapping("/users/{userId}/events")
    public ResponseEntity<List<UserEventItem>> getUserEvents(
            @PathVariable String userId,
            @RequestParam(defaultValue = "50") int limit
    ) {
        var idsWithScores = redis.getUserEventIdsWithScores(userId, limit);

        List<String> ids = new ArrayList<>(idsWithScores.size());
        List<Long> scores = new ArrayList<>(idsWithScores.size());
        for (var e : idsWithScores) {
            ids.add(e.getKey());
            scores.add(e.getValue().longValue());
        }

        List<String> jsons = redis.mgetEventsByIdsInOrder(ids);

        List<UserEventItem> out = new ArrayList<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            out.add(new UserEventItem(ids.get(i), scores.get(i), jsons.get(i)));
        }

        return ResponseEntity.ok(out);
    }

    /**
     * GET /api/dlq?limit=20
     * Returns newest DLQ entries from Redis list "dlq:events".
     */
    @GetMapping("/dlq")
    public ResponseEntity<List<DlqItem>> getDlq(@RequestParam(defaultValue = "20") int limit) {
        List<String> items = redis.getDlq(limit);

        List<DlqItem> out = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) {
            out.add(new DlqItem(i, items.get(i)));
        }

        return ResponseEntity.ok(out);
    }
}
