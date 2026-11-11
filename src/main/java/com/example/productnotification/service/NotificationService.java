package com.example.productnotification.service;

import com.example.productnotification.dto.ProductCreatedEvent;
import com.example.productnotification.entity.ProcessedEvent;
import com.example.productnotification.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final ProcessedEventRepository processedEventRepository;

    public NotificationService(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    /**
     * Idempotent by eventId: safe to call multiple times with the same event.
     */
    @Transactional
    public void handleProductCreated(ProductCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            log.warn("DUPLICATE event [{}] skipped — already processed", event.eventId());
            return;   // ack happens after return; offset still advances
        }


        // ── Business effect (here: pretend we send a notification) ──
        log.info("New product created:");
        log.info("  Event ID : {}", event.eventId());
        log.info("  ID       : {}", event.productId());
        log.info("  Name     : {}", event.name());
        log.info("  Price    : {}", event.price());
        log.info("  Quantity : {}", event.quantity());

        // Record BEFORE the transaction commits → duplicate replay can't
        // sneak in between "effect done" and "record written".
        processedEventRepository.save(
                new ProcessedEvent(event.eventId(), LocalDateTime.now()));
    }
}