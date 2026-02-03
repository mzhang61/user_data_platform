package com.atguigu.eventcollector.model;

import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public class UserEventRequest {

    @NotBlank
    private String eventId;

    @NotBlank
    private String userId;

    @NotBlank
    private String eventType;

    // Keep as String so we can route invalid timestamps to DLQ inside the controller.
    @NotBlank
    private String eventTime;

    @NotBlank
    private String sessionId;

    @NotBlank
    private String page;

    @NotBlank
    private String device;

    private Map<String, Object> properties;

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getEventTime() { return eventTime; }
    public void setEventTime(String eventTime) { this.eventTime = eventTime; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getPage() { return page; }
    public void setPage(String page) { this.page = page; }

    public String getDevice() { return device; }
    public void setDevice(String device) { this.device = device; }

    public Map<String, Object> getProperties() { return properties; }
    public void setProperties(Map<String, Object> properties) { this.properties = properties; }
}