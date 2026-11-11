package com.example.productnotification.consumer;

import com.example.productnotification.dto.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class ProductEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductEventConsumer.class);

    @KafkaListener(topics = "product-events", groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory")
    public void handleProductCreated(ProductCreatedEvent event, Acknowledgment ack) {

        log.info("Processing event [{}] for product {} ({})",
                event.eventId(), event.productId(), event.name());

        // ── Demo hook for Phase 7: simulate a crash AFTER processing ──
        // Any product created with name "FAIL-ME" will be processed (logged
        // above) and then the listener throws → offset is NOT committed →
        // redelivery on restart. DELETE THIS BLOCK after the demo.
        if ("FAIL-ME".equals(event.name())) {
            throw new RuntimeException("Simulated processing failure for demo");
        }
        // ────────────────────────────────────────────────────────────────

        log.info("  DONE processing event [{}]", event.eventId());

        // With RECORD mode the commit happens automatically on return.
        // The explicit acknowledge() documents intent and is required
        // for MANUAL / MANUAL_IMMEDIATE modes (Phase 7 demos below).
        ack.acknowledge();
    }
}