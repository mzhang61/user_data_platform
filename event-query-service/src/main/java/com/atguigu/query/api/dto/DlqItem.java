package com.atguigu.query.api.dto;

/**
 * One DLQ item from Redis list "dlq:events".
 * We keep it as raw JSON string because your DLQ format is already JSON.
 */
public record DlqItem(
        int index,
        String rawJson
) {}
