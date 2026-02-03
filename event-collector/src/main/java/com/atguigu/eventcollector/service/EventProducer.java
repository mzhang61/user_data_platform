package com.atguigu.eventcollector.service;

import com.atguigu.eventcollector.model.EventEnvelope;
import com.atguigu.eventcollector.model.UserEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class EventProducer {

    // Main topic producer (validated, normal events). We send an envelope (Object) for schema evolution.
    private final KafkaTemplate<String, Object> kafkaTemplate;

    // DLQ producer (validation failures or malformed payloads)
    private final KafkaTemplate<String, Object> dlqKafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    @Value("${app.kafka.dlq-topic}")
    private String dlqTopic;

    public EventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                         KafkaTemplate<String, Object> dlqKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.dlqKafkaTemplate = dlqKafkaTemplate;
    }

    public void send(UserEvent event) {
        // Wrap the domain event into an envelope for schema evolution and downstream compatibility.
        EventEnvelope envelope = new EventEnvelope(
                1,
                Instant.now(),
                "event-collector",
                event
        );

        // Use userId as the message key so all events for the same user go to the same partition.
        kafkaTemplate.send(topic, event.userId(), envelope);
    }

    public void sendToDlq(Object payload) {
        // Use a constant key for now; we will refine DLQ keying later if needed.
        dlqKafkaTemplate.send(dlqTopic, "dlq", payload);
    }
}