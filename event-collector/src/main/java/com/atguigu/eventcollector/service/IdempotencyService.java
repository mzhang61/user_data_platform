package com.atguigu.eventcollector.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class IdempotencyService {

    private final StringRedisTemplate redis;

    @Value("${app.idempotency.ttl-seconds:86400}")
    private long ttlSeconds;

    public IdempotencyService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public boolean firstTimeSeen(String eventId) {
        // Use SETNX to ensure the eventId is processed only once within the TTL window.
        Boolean ok = redis.opsForValue().setIfAbsent(key(eventId), "1", Duration.ofSeconds(ttlSeconds));
        return Boolean.TRUE.equals(ok);
    }

    private String key(String eventId) {
        return "idemp:event:" + eventId;
    }
}