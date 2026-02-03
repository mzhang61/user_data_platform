package com.atguigu.eventcollector.model;

import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record EventEnvelope(
        int schemaVersion,
        @NotNull Instant ingestedAt,
        @NotNull String source,
        @NotNull Object event
) {}