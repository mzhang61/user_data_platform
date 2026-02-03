package com.atguigu.eventcollector.controller;

import com.atguigu.eventcollector.model.DlqEvent;
import com.atguigu.eventcollector.model.EventEnvelope;
import com.atguigu.eventcollector.model.UserEvent;
import com.atguigu.eventcollector.model.UserEventRequest;
import com.atguigu.eventcollector.service.EventProducer;
import com.atguigu.eventcollector.service.IdempotencyService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class EventController {

    private final EventProducer eventProducer;
    private final IdempotencyService idempotencyService;

    public EventController(EventProducer eventProducer, IdempotencyService idempotencyService) {
        this.eventProducer = eventProducer;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/events")
    public ResponseEntity<Void> ingest(@Valid @RequestBody UserEventRequest req, BindingResult br) {

        // If bean validation fails (missing fields, blank strings, etc.),
        // publish a structured DLQ event (wrapped in the same envelope format).
        if (br.hasErrors()) {
            publishValidationDlq(req, br);
            return ResponseEntity.accepted().build();
        }

        // Parse eventTime (String -> Instant). We do this manually so that
        // invalid timestamps (e.g., "not-a-time") can be routed to DLQ
        // instead of being rejected by JSON deserialization before reaching this controller.
        Instant parsedEventTime;
        try {
            parsedEventTime = Instant.parse(req.getEventTime());
        } catch (DateTimeParseException ex) {
            publishDlq(
                    "invalid_event_time",
                    req,
                    List.of(Map.of(
                            "field", "eventTime",
                            "message", "eventTime must be an ISO-8601 instant like 2026-01-30T20:00:00Z"
                    ))
            );
            return ResponseEntity.accepted().build();
        }

        // Idempotency: if we have already processed this eventId within the TTL window, do not publish again.
        if (!idempotencyService.firstTimeSeen(req.getEventId())) {
            return ResponseEntity.accepted().build();
        }

        // Build the domain event and publish to the main topic.
        UserEvent evt = new UserEvent(
                req.getEventId(),
                req.getUserId(),
                req.getEventType(),
                parsedEventTime,
                req.getSessionId(),
                req.getPage(),
                req.getDevice(),
                req.getProperties()
        );

        eventProducer.send(evt);
        return ResponseEntity.accepted().build();
    }

    private void publishValidationDlq(UserEventRequest req, BindingResult br) {
        List<Map<String, String>> errors = br.getFieldErrors().stream()
                .map(e -> Map.of(
                        "field", e.getField(),
                        "message", e.getDefaultMessage()
                ))
                .toList();

        publishDlq("validation_failed", req, errors);
    }

    private void publishDlq(String reason, UserEventRequest req, List<Map<String, String>> errors) {
        DlqEvent dlqEvent = new DlqEvent(reason, req, errors);

        EventEnvelope envelope = new EventEnvelope(
                1,
                Instant.now(),
                "event-collector",
                dlqEvent
        );

        eventProducer.sendToDlq(envelope);
    }
}