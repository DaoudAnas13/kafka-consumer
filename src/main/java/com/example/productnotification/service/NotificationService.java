package com.example.productnotification.service;

import com.example.productnotification.dto.ProductCreatedEvent;
import com.example.productnotification.entity.ProcessedEvent;
import com.example.productnotification.exception.PoisonMessageException;
import com.example.productnotification.exception.TransientProcessingException;
import com.example.productnotification.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    // Demo-only: simulate a transient fault that heals on retry.
    private static final Set<String> TRANSIENT_FAILURES = ConcurrentHashMap.newKeySet();

    private final ProcessedEventRepository processedEventRepository;

    public NotificationService(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void handleProductCreated(ProductCreatedEvent event) {

        // ── Idempotency (unchanged from Phase 8) ──
        if (processedEventRepository.existsById(event.eventId())) {
            log.warn("DUPLICATE event [{}] skipped", event.eventId());
            return;
        }

        // ── Failure classification (DEMO hooks — delete in real code) ──
        if ("TEMP-FAIL".equals(event.name()) && TRANSIENT_FAILURES.add(event.eventId())) {
            // First attempt fails; retry will find it already in the set → succeed.
            throw new TransientProcessingException(
                    "Temporary outage while notifying about " + event.eventId());
        }
        if ("POISON".equals(event.name())) {
            throw new PoisonMessageException(
                    "Product name 'POISON' can never be processed (business rule)");
        }

        // ── Business effect ──
        log.info("New product created:");
        log.info("  Event ID : {}", event.eventId());
        log.info("  ID       : {}", event.productId());
        log.info("  Name     : {}", event.name());
        log.info("  Price    : {}", event.price());
        log.info("  Quantity : {}", event.quantity());

        processedEventRepository.save(
                new ProcessedEvent(event.eventId(), LocalDateTime.now()));
    }
}