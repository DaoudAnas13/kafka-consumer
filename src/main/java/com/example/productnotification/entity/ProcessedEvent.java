package com.example.productnotification.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", length = 64)
    private String eventId;          // natural key = idempotency key

    @Column(nullable = false)
    private LocalDateTime processedAt;

    protected ProcessedEvent() {}

    public ProcessedEvent(String eventId, LocalDateTime processedAt) {
        this.eventId = eventId;
        this.processedAt = processedAt;
    }

    public String getEventId() { return eventId; }
    public LocalDateTime getProcessedAt() { return processedAt; }
}