package com.atguigu.eventcollector.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;

public record UserEvent(
        @NotBlank String eventId,
        @NotBlank String userId,
        @NotBlank String eventType,
        @NotNull Instant eventTime,
        @NotBlank String sessionId,
        String page,
        String device,
        Map<String, Object> properties
) {}
