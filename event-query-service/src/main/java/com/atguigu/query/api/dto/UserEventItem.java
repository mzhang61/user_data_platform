package com.atguigu.query.api.dto;

/**
 * One user event item returned by /api/users/{userId}/events
 *
 * eventJson can be null if the "event:{eventId}" key does not exist.
 */
public record UserEventItem(
        String eventId,
        long scoreMillis,
        String eventJson
) {}
