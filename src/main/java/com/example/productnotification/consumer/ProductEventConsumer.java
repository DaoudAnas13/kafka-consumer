package com.example.productnotification.consumer;

import com.example.productnotification.dto.ProductCreatedEvent;
import com.example.productnotification.service.NotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class ProductEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductEventConsumer.class);

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

    /**
     * DLT inspector — consumes parked messages just to log them.
     * Its own group id: inspecting must not compete with main processing.
     */
    @KafkaListener(topics = "product-events.DLT", groupId = "notification-service-dlt",
            containerFactory = "kafkaListenerContainerFactory")
    public void inspectDlt(ConsumerRecord<String, ProductCreatedEvent> record,
                           Acknowledgment ack) {
        log.error("===== DLT MESSAGE =====");
        log.error("  original topic : {}", record.headers().lastHeader("kafka_dlt-original-topic") != null
                ? new String(record.headers().lastHeader("kafka_dlt-original-topic").value()) : "n/a");
        log.error("  exception      : {}", header(record, "kafka_dlt-exception-message"));
        log.error("  key            : {}", record.key());
        log.error("  offset         : {}", record.offset());
        log.error("  value          : {}", record.value());
        log.error("=======================");
        ack.acknowledge();
    }

    private String header(ConsumerRecord<?, ?> record, String name) {
        var h = record.headers().lastHeader(name);
        return h != null ? new String(h.value()) : "n/a";
    }
}