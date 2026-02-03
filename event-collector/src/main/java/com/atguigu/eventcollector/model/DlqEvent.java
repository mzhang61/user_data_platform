package com.atguigu.eventcollector.model;

import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

public record DlqEvent(
        @NotNull String reason,
        @NotNull Object payload,
        @NotNull List<Map<String, String>> errors
) {}