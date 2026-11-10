package com.example.productnotification.consumer;

import com.example.productnotification.dto.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ProductEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductEventConsumer.class);

    @KafkaListener(topics = "product-events", groupId = "notification-service")
    public void handleProductCreated(ProductCreatedEvent event) {
        log.info("New product created:");
        log.info("  Event ID : {}", event.eventId());
        log.info("  ID       : {}", event.productId());
        log.info("  Name     : {}", event.name());
        log.info("  Price    : {}", event.price());
        log.info("  Quantity : {}", event.quantity());
    }
}