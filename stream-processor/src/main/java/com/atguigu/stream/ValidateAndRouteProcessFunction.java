package com.atguigu.stream;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Route raw JSON event into:
 * - main output: clean events (eventTime is valid ISO-8601)
 * - side output: dlq events (invalid / missing eventTime)
 */
public class ValidateAndRouteProcessFunction extends ProcessFunction<String, String> {

    private final OutputTag<String> dlqTag;

    // Extract "eventTime":"..."
    private static final Pattern EVENT_TIME_PATTERN =
            Pattern.compile("\"eventTime\"\\s*:\\s*\"([^\"]+)\"");

    public ValidateAndRouteProcessFunction(OutputTag<String> dlqTag) {
        this.dlqTag = dlqTag;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
        try {
            String eventTime = extractEventTime(value);
            // Validate ISO-8601, e.g. "2026-01-30T20:00:00Z"
            Instant.parse(eventTime);

            // valid -> clean
            out.collect(value);
        } catch (Exception e) {
            // invalid -> DLQ
            ctx.output(dlqTag, value);
        }
    }

    private String extractEventTime(String json) {
        Matcher m = EVENT_TIME_PATTERN.matcher(json);
        if (!m.find()) {
            throw new IllegalArgumentException("eventTime missing");
        }
        return m.group(1);
    }
}