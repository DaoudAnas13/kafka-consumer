package com.example.productnotification.consumer;

import com.example.productnotification.dto.ProductCreatedEvent;
import com.example.productnotification.service.NotificationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class ProductEventConsumer {

    private final NotificationService notificationService;

    public ProductEventConsumer(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @KafkaListener(topics = "product-events", groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory")
    public void handleProductCreated(ProductCreatedEvent event, Acknowledgment ack) {
        notificationService.handleProductCreated(event);
        ack.acknowledge();
    }
}